package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/cmd/track/async"
	"github.com/ethereum/go-ethereum/cmd/track/documents"
	"github.com/ethereum/go-ethereum/cmd/track/metrics"
	"github.com/ethereum/go-ethereum/cmd/track/push"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/track"

	"github.com/go-redis/redis/v8"
	"github.com/ip2location/ip2location-go/v9"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/qiniu/qmgo"
	"github.com/qiniu/qmgo/options"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	officialOpts "go.mongodb.org/mongo-driver/mongo/options"
)

type client struct {
	crawlerID string

	ip2locationDB *ip2location.DB
	mongoClient   *qmgo.Client
	redisClient   *redis.Client
	httpClient    *push.Client

	blockScript, txScript, discScript *redis.Script

	collChanMap   map[track.Type]chan interface{} // source
	bridgeChanMap map[track.Type]interface{}      // bridge chan for init and process function

	connectionMap sync.Map
}

func createClient() (*client, error) {
	enode := ""

	if rpcCli, err := rpc.DialContext(context.Background(), gethRpc); err != nil {
		return nil, err
	} else {

		var result p2p.NodeInfo
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		if err := rpcCli.CallContext(ctx, &result, "admin_nodeInfo"); err != nil {
			log.Printf("E! call admin_nodeInfo, err: %v", err)
			cancel()
			rpcCli.Close()
			return nil, err
		}
		enode = result.ID
		cancel()
		rpcCli.Close()
	}

	ipDB, err := ip2location.OpenDB("/opt/geth/IP2LOCATION-LITE-DB5.BIN")
	if err != nil {
		return nil, err
	}

	client := &client{
		ip2locationDB: ipDB,
		collChanMap:   make(map[track.Type]chan interface{}),
		bridgeChanMap: make(map[track.Type]interface{}),
		redisClient: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			PoolSize: redisPoolSize,
		}),
		crawlerID:  enode[:8],
		httpClient: push.NewClient(strings.Split(streamcubeUrls, ",")...),
	}

	go client.httpClient.Push(make(chan struct{}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := qmgo.NewClient(ctx, &qmgo.Config{Uri: mongodbURI})
	if err != nil {
		return nil, err
	}

	async.RunEvery(context.Background(), time.Hour, func() {
		if count, err := cli.Database("track").Collection("mainnetPeer").Find(context.Background(), bson.M{
			"lastSeen": bson.M{
				"$gte": time.Now().Add(-time.Hour * 24).UnixMilli(),
			},
		}).Count(); err != nil {
			log.Printf("E! query mainnetPeer count err: %v", err)
			return
		} else {
			metrics.SetLast24hrsPeerCount(count)
		}
	})

	db := cli.Database(documents.TrackDB)

	//if err := db.Collection(documents.PeerPool).EnsureIndexes(context.Background(), []string{"pubkey"}, []string{}); err != nil {
	//	return nil, err
	//}

	//> db.blockTrack.getIndexes()
	//[
	//    {"v" : 2,   "key" : {"_id" : 1},            "name" : "_id_"},
	//    {"v" : 2,   "key" : {"hash" : 1},           "name" : "hash_1",  "unique" : true},
	//    {"v" : 2,   "key" : {"fromPeerId" : 1},     "name" : "fromPeerId_1"}
	//]
	if err := db.Collection(documents.BlockTrackCollection).EnsureIndexes(context.Background(), []string{"hash"}, []string{"fromPeerId", "-number"}); err != nil {
		return nil, err
	}

	//> db.discReason.getIndexes()
	//[   { "v" : 2,  "key" : {"_id" : 1},            "name" : "_id_"} ]

	//> db.mainnetPeer.getIndexes()
	//[   {"v" : 2,   "key" : {"_id" : 1},            "name" : "_id_"},
	//    {"v" : 2,   "key" : {"id" : 1},             "name" : "pubkey_1",    "unique" : true},
	//    {"v" : 2,   "key" : {"lastSeen" : -1},      "name" : "lastSeen_-1"},
	//]
	if err := db.Collection(documents.MainnetPeer).EnsureIndexes(context.Background(), []string{"id"}, []string{"-lastSeen"}); err != nil {
		return nil, err
	}

	//> db.txTrack.getIndexes()
	//[
	//    {"v" : 2,   "key" : {"_id" : 1},            "name" : "_id_"},
	//    {"v" : 2,   "key" : {"updateTimeAt" : 1},   "name" : "updateTimeAt_1",  "expireAfterSeconds" : 604800},
	//    {"v" : 2,   "key" : {"hash" : 1},           "name" : "hash_1"},
	//    {"v" : 2,   "key" : {"fromPeerId" : 1},     "name" : "fromPeerId_1"}
	//]
	opts := officialOpts.Index()
	opts.SetExpireAfterSeconds(86400 * int32(dbTxExpireDays)) // expire in dbTxExpireDays days
	if err := db.Collection(documents.TxTrackCollection).CreateOneIndex(context.Background(), options.IndexModel{
		Key:          []string{"updateTimeAt"},
		IndexOptions: opts,
	}); err != nil {
		return nil, err
	}

	if err := db.Collection(documents.TxTrackCollection).EnsureIndexes(context.Background(), []string{}, []string{"hash", "fromPeerId"}); err != nil {
		return nil, err
	}

	opts1 := officialOpts.Index()
	opts1.SetExpireAfterSeconds(86400 * int32(dbTxExpireDays)) // expire in dbTxExpireDays days
	if err := db.Collection(documents.AggregateProof).CreateIndexes(context.Background(), []options.IndexModel{
		{
			Key:          []string{"updateTimeAt"},
			IndexOptions: opts1,
		},
		{
			Key:          []string{"slot", "blockRoot"},
			IndexOptions: officialOpts.Index().SetUnique(true),
		},
	}); err != nil {
		return nil, err
	}

	// add key expiration notification config setting
	if result, err := client.redisClient.ConfigSet(context.Background(), "notify-keyspace-events", "KEA").Result(); err != nil {
		log.Printf("E! set notify-keyspace-events err: %v", err)
	} else {
		log.Printf("I! set notify-keyspace-events KEgx result: %v", result)
	}

	//client.collChanMap[ConnectionTrackCollection] = make(chan interface{}, 1)
	client.collChanMap[track.BlockBroadcast] = make(chan interface{}, 1024)
	client.collChanMap[track.TransactionBroadcast] = make(chan interface{}, 1024)
	client.collChanMap[track.MainnetPeer] = make(chan interface{}, 1024)
	client.collChanMap[track.BlockHeadersResponse] = make(chan interface{}, 1024)
	client.collChanMap[track.Connection] = make(chan interface{}, 1024)
	client.collChanMap[track.BeaconBlockBroadcast] = make(chan interface{}, 1024)
	client.collChanMap[track.AggregateAndProof] = make(chan interface{}, 1024)
	client.collChanMap[track.BeaconPeer] = make(chan interface{}, 1024)

	client.bridgeChanMap[track.BlockBroadcast] = make(chan string, 1)
	client.bridgeChanMap[track.TransactionBroadcast] = make(chan string, 1)
	client.bridgeChanMap[track.MainnetPeer] = make(chan interface{}, 1)
	client.bridgeChanMap[track.BlockHeadersResponse] = make(chan interface{}, 1)
	client.bridgeChanMap[track.Connection] = make(chan string, 1)
	client.bridgeChanMap[track.BeaconBlockBroadcast] = make(chan interface{}, 1)
	client.bridgeChanMap[track.AggregateAndProof] = make(chan interface{}, 1)
	client.bridgeChanMap[track.BeaconPeer] = make(chan interface{}, 1)

	client.mongoClient = cli

	return client, nil
}

func (c *client) Run() {

	blockScript := `
local setKey = KEYS[1]
local ttlSetKey = KEYS[2]
local detailSetKey = KEYS[3]
redis.call("sadd", setKey, ARGV[1])
redis.call("expire", setKey, 2*ARGV[2])
redis.call("setex", ttlSetKey, ARGV[2], ARGV[1])
redis.call("hmset", detailSetKey, "number", ARGV[3], "hash", ARGV[4])
if (0 ~= tonumber(ARGV[5]))
then
	redis.call("hmset", detailSetKey, "tx_count", ARGV[5])
end
if (0 ~= tonumber(ARGV[6]))
then
	redis.call("hmset", detailSetKey, "uncle_count", ARGV[6])
end
return redis.call("expire", detailSetKey, 2*ARGV[2])
`

	// KEYS:
	// 		0: tx hash
	// 		1: propagator string
	// ARGV:
	//		0: ttl
	// 		1: tx data
	// 		2: tx hash
	txScript := `
	local setKey = table.concat({"tx:", KEYS[1]})
	local ttlSetKey = table.concat({"shadow:tx:", KEYS[1]})
	local detailSetKey = table.concat({"detail:tx:", KEYS[1]})
	redis.call("sadd", setKey, KEYS[2])
	redis.call("expire", setKey, 2*ARGV[1])
	redis.call("setex", ttlSetKey, ARGV[1], ARGV[2])
	if (0 == redis.call("exists", detailSetKey))
	then
	   redis.call("hmset", detailSetKey, "data", ARGV[2], "hash", ARGV[3], "value", ARGV[4])
	end
	return redis.call("expire", detailSetKey, 2*ARGV[1])
	`

	discScript := `
local setKey = KEYS[1]
local ttlSetKey = KEYS[2]
local detailSetKey = KEYS[3]
redis.call("hincrby", setKey, ARGV[1], 1)
redis.call("expire", setKey, 2*ARGV[2])
redis.call("setex", ttlSetKey, ARGV[2], ARGV[2])
if (0 == redis.call("exists", detailSetKey))
then
    redis.call("hmset", detailSetKey, "peerID", ARGV[3], "addr", ARGV[4])
end
return redis.call("expire", detailSetKey, 2*ARGV[2])
`

	c.blockScript = redis.NewScript(blockScript)
	c.txScript = redis.NewScript(txScript)
	c.discScript = redis.NewScript(discScript)

	go c.call(context.Background(), track.BlockBroadcast, initBlock, processBlock)
	go c.call(context.Background(), track.TransactionBroadcast, initTx, processTx)
	go c.call(context.Background(), track.MainnetPeer, initMainnetPeer, processMainnetPeer)
	go c.call(context.Background(), track.BlockHeadersResponse, initBlockHeaders, processBlockHeaders)
	go c.call(context.Background(), track.Connection, initDisconnection, processDisconnection)
	go c.call(context.Background(), track.BeaconBlockBroadcast, initBeaconBlock, processBeaconBlock)
	go c.call(context.Background(), track.AggregateAndProof, initAggregate, processAggregate)
	go c.call(context.Background(), track.BeaconPeer, initBeaconPeer, processBeaconPeer)

	go c.subscribeExpired(context.Background())
}

func (c *client) push(trackMsg interface{}) {
	topic := track.Unknown

	switch trackMsg := trackMsg.(type) {
	//case *ConnectionTrack:
	//	c.collChanMap[ConnectionTrackCollection] <- trackMsg
	case *track.BlockBroadcastMessage:
		topic = track.BlockBroadcast

		c.httpClient.Emit(push.NewBlockDataWrap(push.BlockData{
			BlockHash:    trackMsg.Hash,
			Crawler:      c.crawlerID,
			BlockTime:    trackMsg.Propagator.Timestamp,
			Counter:      "BLK",
			PropagatorID: trackMsg.Propagator.FromPeerId,
			PropagatorIP: trackMsg.Propagator.FromPeerIP,
		}))
	case *track.TransactionBroadcastMessage:
		topic = track.TransactionBroadcast

		c.httpClient.Emit(push.NewTransDataWrap(push.TransData{
			Counter:      "TX",
			TransHash:    trackMsg.Hash,
			TransTime:    trackMsg.Propagator.Timestamp,
			Crawler:      c.crawlerID,
			PropagatorID: trackMsg.Propagator.FromPeerId,
			PropagatorIP: trackMsg.Propagator.FromPeerIP,
		}))
	case *track.MainnetPeerMessage:
		topic = track.MainnetPeer
	case *track.BlockHeaderResponseMessage:
		topic = track.BlockHeadersResponse
	case *track.ConnectionMessage:
		topic = track.Connection
	case *track.BeaconBlock:
		topic = track.BeaconBlockBroadcast
		c.httpClient.Emit(push.NewBeaconBlockWrap(push.BeaconBlockData{
			Slot:      trackMsg.Slot,
			Graffiti:  hex.EncodeToString(trackMsg.Graffiti),
			BlockHash: hex.EncodeToString(trackMsg.BlockHash),
			Proposer:  trackMsg.Proposer,
			BlockTime: trackMsg.TimeStamp,
			FromPeer:  trackMsg.FromPeer,
		}))
	case *track.Aggregate:
		topic = track.AggregateAndProof
		c.httpClient.Emit(push.NewAggregateWrap(push.AggregateData{
			Slot:            trackMsg.Slot,
			AggregatorIndex: trackMsg.AggregatorIndex,
			BeaconBlockRoot: hex.EncodeToString(trackMsg.BeaconBlockRoot),
			CommitteeIndex:  trackMsg.CommitteeIndex,
			Source: push.Checkpoint{
				Root:  hex.EncodeToString(trackMsg.Source.Root),
				Epoch: uint64(trackMsg.Source.Epoch),
			},
			Target: push.Checkpoint{
				Root:  hex.EncodeToString(trackMsg.Target.Root),
				Epoch: uint64(trackMsg.Target.Epoch),
			},
			AggTime:  trackMsg.TimeStamp,
			FromPeer: trackMsg.FromPeer,
			BitList:  trackMsg.BitList,
		}))
	case *track.BeaconPeerMessage:
		topic = track.BeaconPeer
		c.httpClient.Emit(push.NewBeaconPeer(push.BeaconData{
			Type:         trackMsg.Type,
			MultiAddr:    trackMsg.MultiAddr,
			Pubkey:       trackMsg.Pubkey,
			Direction:    trackMsg.Direction,
			AgentVersion: trackMsg.AgentVersion,
		}))
	}
	if topic != track.Unknown {
		if collChanel, ok := c.collChanMap[topic]; ok {
			collChanel <- trackMsg
		}
	}
}

func (c *client) call(ctx context.Context, coll track.Type, init func(*client), process func(*client, interface{})) {
	if init != nil {
		init(c)
	}
	ticker := time.NewTicker(time.Duration(10) * time.Second)
	for {
		select {
		case trackMsg := <-c.collChanMap[coll]:
			if process != nil {
				go process(c, trackMsg)
			}
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("D! %s coll chan len: %d", coll.String(), len(c.collChanMap[coll]))
		}
	}
}

func (c *client) subscribeExpired(ctx context.Context) {
	pubsub := c.redisClient.PSubscribe(ctx, "__keyevent@0__:expired")
	_, err := pubsub.Receive(context.Background())
	if err != nil {
		log.Printf("subscribe err: %v", err)
		return
	}

	//delTick := time.NewTicker(time.Second)
	//deleteKeys := make([]string, 0, 100)

	for {
		select {
		case msg := <-pubsub.Channel():
			shadowKey := msg.Payload
			if !strings.HasPrefix(shadowKey, "shadow:") {
				continue
			}
			log.Printf("shadow key expired: %s", shadowKey)
			key := strings.TrimPrefix(shadowKey, "shadow:")

			if splits := strings.Split(key, ":"); len(splits) > 0 {
				switch splits[0] {
				case "block":
					c.bridgeChanMap[track.BlockBroadcast].(chan string) <- key
				case "tx":
					c.bridgeChanMap[track.TransactionBroadcast].(chan string) <- key
				case "disc":
					c.bridgeChanMap[track.Connection].(chan string) <- key
				}
				//deleteKeys = append(deleteKeys, fmt.Sprintf("shadow:%s", key), fmt.Sprintf("detail:%s", key))

			}
			//case <-delTick.C:
			//	if len(deleteKeys) > 0 {
			//		c.redisClient.Del(context.Background(), deleteKeys...)
			//		deleteKeys = deleteKeys[:0]
			//	}
			//default:
			//	if len(deleteKeys) >= 100 {
			//		c.redisClient.Del(ctx, deleteKeys...)
			//		deleteKeys = deleteKeys[:0]
			//	}
		}
	}
}

func (c *client) cache2mongo(ctx context.Context, subCh <-chan string, docCh chan<- interface{}) {
	for {
		select {
		case key := <-subCh:
			if strings.HasPrefix(key, "block:") {
				blockMsg := track.BlockBroadcastMessage{}
				if err := c.redisClient.HGetAll(context.Background(), fmt.Sprintf("detail:%s", key)).Scan(&blockMsg); err != nil || blockMsg.Hash == "" {
					log.Printf("E! redis get detail:%s err: %v", key, err)
					continue
				}

				propagatorStrList := []string{}
				if err := c.redisClient.SMembers(context.Background(), key).ScanSlice(&propagatorStrList); err != nil {
					log.Printf("E! redis get %s err: %v", key, err)
					continue
				}
				propagators := []track.Propagator{}
				for _, str := range propagatorStrList {
					propagator := documents.UnmarshalPropagator(str)

					if record, err := c.ip2locationDB.Get_all(propagator.FromPeerIP); err == nil {
						propagator.Location = track.GeoJsonPoint{
							Type:        "Point",
							Coordinates: []float64{float64(record.Latitude), float64(record.Longitude)},
						}
					}
					propagators = append(propagators, *propagator)
				}
				doc := documents.BlockTrack{
					Number:      blockMsg.Number,
					Hash:        blockMsg.Hash,
					TxCount:     blockMsg.TxCount,
					UncleCount:  blockMsg.UncleCount,
					Propagators: propagators,
				}
				if doc.Hash == "" || doc.Number == 0 {
					log.Printf("D! block hash or number is empty, key: %s", key)
					continue
				}
				docCh <- doc

			} else if strings.HasPrefix(key, "tx:") {
				txMsg := track.TransactionBroadcastMessage{}
				if err := c.redisClient.HGetAll(context.Background(), fmt.Sprintf("detail:%s", key)).Scan(&txMsg); err != nil || txMsg.Hash == "" {
					log.Printf("E! redis get detail:%s err: %v", key, err)
					continue
				}

				propagatorStrList := []string{}
				if err := c.redisClient.SMembers(context.Background(), key).ScanSlice(&propagatorStrList); err != nil {
					log.Printf("E! redis get %s err: %v", key, err)
					continue
				}
				propagators := []track.Propagator{}
				for _, str := range propagatorStrList {
					propagator := documents.UnmarshalPropagator(str)

					if record, err := c.ip2locationDB.Get_all(propagator.FromPeerIP); err == nil {
						propagator.Location = track.GeoJsonPoint{
							Type:        "Point",
							Coordinates: []float64{float64(record.Latitude), float64(record.Longitude)},
						}
					}
					propagators = append(propagators, *propagator)
				}
				doc := documents.TransactionTrack{
					Hash:         txMsg.Hash,
					Data:         txMsg.Data,
					Value:        txMsg.Value,
					Propagators:  propagators,
					UpdateTimeAt: primitive.NewDateTimeFromTime(time.Now()),
				}
				if doc.Hash == "" {
					log.Printf("D! tx hash is empty, key: %s", key)
					continue
				}
				docCh <- doc
			}
			//c.redisClient.Del(context.Background(), key, "detail:"+key)
		case <-ctx.Done():
			return
		}
	}
}

func (c *client) storeMany(ctx context.Context, dataCh chan interface{}, collectionName string) {
	batch := make([]interface{}, 0, documents.BatchSize)
	period := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case data := <-dataCh:
			batch = append(batch, data)
			if len(batch) >= documents.BatchSize {
				_, err := c.mongoClient.Database(documents.TrackDB).Collection(collectionName).InsertMany(context.Background(), batch)
				if err != nil {
					log.Printf("insertMany %s err: %v, batches: %+v", collectionName, err, batch)
				}
				batch = batch[:0]
			}
		case <-period.C:
			if len(batch) > 0 {
				_, err := c.mongoClient.Database(documents.TrackDB).Collection(collectionName).InsertMany(context.Background(), batch)
				if err != nil {
					log.Printf("insertMany %s err: %v, batches: %+v", collectionName, err, batch)
				}
				batch = batch[:0]
			}
		}
	}
}

func (c *client) upsertMainnetPeerConnection(ctx context.Context, dataCh chan interface{}, collectionName string) {

	coll := c.mongoClient.Database(documents.TrackDB).Collection(collectionName)

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-dataCh:
			if message, ok := data.(*track.MainnetPeerMessage); ok {

				country := ""
				city := ""

				latitude := float64(0)
				longitude := float64(0)
				if record, err := c.ip2locationDB.Get_all(message.IP); err == nil {
					country = record.Country_short
					city = record.City
					latitude = float64(record.Latitude)
					longitude = float64(record.Longitude)
				}

				fieldSetOnInsert := bson.M{
					"firstSeen": message.Timestamp,
				}

				fieldSet := bson.M{
					"static":   message.Info.Network.Static,
					"trusted":  message.Info.Network.Trusted,
					"name":     message.Info.Name,
					"caps":     message.Info.Caps,
					"ip":       message.IP,
					"pubkey":   message.Pubkey,
					"tcp":      message.TCP,
					"udp":      message.UDP,
					"country":  country,
					"city":     city,
					"lastSeen": message.Timestamp,
					"location": bson.M{
						"type":        "Point",
						"coordinates": []interface{}{latitude, longitude},
					},
				}
				if !message.Info.Network.Inbound {
					fieldSet["remote_address"] = message.Info.Network.RemoteAddress
				}

				addToSet := bson.M{}
				connectAction := message.Type == 1

				if message.Info.Network.Inbound {
					addToSet["inbound"] = map[int64]bool{
						message.Timestamp: connectAction,
					}
				} else {
					addToSet["outbound"] = map[int64]bool{
						message.Timestamp: connectAction,
					}
				}

				opts := options.UpdateOptions{}
				opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
				if err := coll.UpdateOne(ctx, bson.M{
					"id": message.Info.ID,
				}, bson.M{
					"$set":         fieldSet,
					"$setOnInsert": fieldSetOnInsert,
					"$addToSet":    addToSet,
				}, opts); err != nil {
					if err != qmgo.ErrNoSuchDocuments {
						log.Printf("E! upsert %s err: %v", collectionName, err)
					} else {
						// increase metrics when no such documents
						if client, _, _, err := metrics.ParseName(message.Info.Name); err != nil {
							metrics.IncClientCount("unknown")
						} else {
							metrics.IncClientCount(client)
						}
					}
				}

			}
		}
	}
}

const NumberOfMappingPeers = 5

func (c *client) upsertBlockPropagators(ctx context.Context, dataCh chan interface{}, blockPoolMappingCh chan<- blockPeerMapping, collectionName string) {
	coll := c.mongoClient.Database(documents.TrackDB).Collection(collectionName)

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-dataCh:
			message, ok := data.(documents.BlockTrack)
			if !ok {
				log.Panicf("data from dataCh type assertion failure: %t", data)
			}
			pushField := bson.M{
				"propagators": bson.M{
					"$each": message.Propagators,
				},
			}

			opts := options.UpdateOptions{}
			opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
			if err := coll.UpdateOne(ctx,
				bson.M{
					"hash": message.Hash,
				},
				bson.M{
					"$push": pushField,
					"$set": bson.M{
						"number":     message.Number,
						"txCount":    message.TxCount,
						"uncleCount": message.UncleCount,
					},
				},
				opts); err != nil && err != qmgo.ErrNoSuchDocuments { // upsert err
				log.Printf("E! upsert %s err: %v", collectionName, err)
			} else {
				blockPoolMappingCh <- blockPeerMapping{
					blockHash: message.Hash,
				}
			}

		}
	}
}

func (c *client) updatePeerPoolMapping(ctx context.Context, mappingCh <-chan blockPeerMapping) {

	blockColl := c.mongoClient.Database(documents.TrackDB).Collection(documents.BlockTrackCollection)
	//peerPoolColl := c.mongoClient.Database(documents.TrackDB).Collection(documents.PeerPool)
	mainnetColl := c.mongoClient.Database(documents.TrackDB).Collection(documents.MainnetPeer)

	log.Printf("D! start update peer pool mapping")

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-mappingCh:
			{
				blockTrack := documents.BlockTrack{}
				if err := blockColl.Find(context.Background(), bson.M{"hash": m.blockHash}).One(&blockTrack); err != nil {
					log.Printf("E! find block %s err: %v", m.blockHash, err)
					continue
				}

				pList := track.PropagatorList(blockTrack.Propagators)
				sort.Sort(pList)
				if pList.Len() > NumberOfMappingPeers {
					pList = pList[:NumberOfMappingPeers]
				}
				if blockTrack.Header == nil {
					continue
				}

				pool := strings.ToLower(blockTrack.Header.Coinbase.Hex())

				incMap := bson.M{
					"poolMapping." + pool: 1,
				}

				opts := options.UpdateOptions{}
				opts.UpdateOptions = officialOpts.Update().SetUpsert(true)

				for _, hash := range pList {
					if err := mainnetColl.UpdateOne(context.Background(), bson.M{
						"pubkey": hash.FromPeerId,
					}, bson.M{
						"$inc": incMap,
					}, opts); err != nil && err != qmgo.ErrNoSuchDocuments {
						log.Printf("E! update pool mapping err: %v", err)
						continue
					}
				}
				if err := blockColl.UpdateOne(context.Background(),
					bson.M{"hash": m.blockHash},
					bson.M{
						"$set": bson.M{
							"peerMapped": true,
						}}); err != nil {
					log.Printf("E! update blockTrack err: %v", err)
				}
			}
		}
	}

}

func (c *client) upsertBlockHeaders(ctx context.Context, dataCh chan interface{}, collectionName string) {

	coll := c.mongoClient.Database(documents.TrackDB).Collection(collectionName)

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-dataCh:
			message, ok := data.(*track.BlockHeaderResponseMessage)
			if !ok || message.Header == nil {
				continue
			}
			fieldSet := bson.M{
				"header": message.Header,
			}

			opts := options.UpdateOptions{}
			opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
			if err := coll.UpdateOne(ctx,
				bson.M{
					"hash": message.Hash,
				},
				bson.M{
					"$set": fieldSet,
				},
				opts); err != nil && err != qmgo.ErrNoSuchDocuments {
				log.Printf("E! upsert %s err: %v", collectionName, err)
			}

		}
	}
}

func (c *client) upsertDisconnection(ctx context.Context, dataCh chan string, collectionName string) {
	coll := c.mongoClient.Database(documents.TrackDB).Collection(collectionName)

	for {
		select {
		case <-ctx.Done():
			return
		case discKey := <-dataCh:

			detailMsg := track.ConnectionMessage{}
			if err := c.redisClient.HGetAll(context.Background(), `detail:`+discKey).Scan(&detailMsg); err != nil || detailMsg == (track.ConnectionMessage{}) {
				log.Printf("E! redis get detail:%s err: %v", discKey, err)
				continue
			}

			strMap, err := c.redisClient.HGetAll(context.Background(), discKey).Result()
			if err != nil {
				log.Printf("E! redis get disconnection count map err: %v", err)
				continue
			}
			countMap := make(map[string]int64, len(strMap))
			for key, countString := range strMap {
				count, err := strconv.ParseInt(countString, 10, 64)
				if err != nil {
					log.Printf("E! parse count %s err: %v", countString, err)
					continue
				}
				countMap[key] = count
			}

			opts := options.UpdateOptions{}
			opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
			if err := coll.UpdateOne(ctx,
				bson.M{
					"peerID": detailMsg.PeerID,
					"addr":   detailMsg.Addr,
				},
				bson.M{
					"$inc": countMap,
				},
				opts); err != nil && err != qmgo.ErrNoSuchDocuments {
				log.Printf("E! upsert %s err: %v", collectionName, err)
			}
		}
	}
}

func (c *client) upsertBeaconPeer(ctx context.Context, dataCh chan interface{}, collectionName string) {

	coll := c.mongoClient.Database(documents.TrackDB).Collection(collectionName)

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-dataCh:
			message, ok := data.(*track.BeaconPeerMessage)
			if !ok {
				continue
			}

			fieldSetOnInsert := bson.M{
				"firstSeen": message.TimeStamp,
			}

			fieldSet := bson.M{
				"multiAddr":    message.MultiAddr,
				"agentVersion": message.AgentVersion,
				"pubkey":       message.Pubkey,
			}

			addToSet := bson.M{}

			connectAction := message.Type == 1

			switch network.Direction(message.Direction) {
			case network.DirUnknown:
			case network.DirInbound:
				addToSet["inbound"] = map[int64]bool{
					message.TimeStamp: connectAction,
				}
			case network.DirOutbound:
				addToSet["outbound"] = map[int64]bool{
					message.TimeStamp: connectAction,
				}
			}

			opts := options.UpdateOptions{}
			opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
			if err := coll.UpdateOne(ctx, bson.M{
				"pubkey": message.Pubkey,
			}, bson.M{
				"$set":         fieldSet,
				"$setOnInsert": fieldSetOnInsert,
				"$addToSet":    addToSet,
			}, opts); err != nil {
				if err != qmgo.ErrNoSuchDocuments {
					log.Printf("E! upsert %s err: %v", collectionName, err)
				}
			}

		}
	}

}

func (c *client) upsertAggregateProof(ctx context.Context, dataCh chan interface{}, collectionName string) {
	coll := c.mongoClient.Database(documents.TrackDB).Collection(collectionName)

	for {
		select {
		case <-ctx.Done():
			return
		case data := <-dataCh:
			message := data.(*track.Aggregate)
			fieldSetOnInsert := bson.M{
				"firstSeen": message.TimeStamp,
			}

			uniqueKey := bson.M{
				"slot":      message.Slot,
				"blockRoot": hex.EncodeToString(message.BeaconBlockRoot),
			}
			fieldSet := bson.M{
				"slot":         message.Slot,
				"blockRoot":    hex.EncodeToString(message.BeaconBlockRoot),
				"updateTimeAt": time.Now().UnixMilli(),
			}

			addToSet := bson.M{
				fmt.Sprintf("committee.%d", message.CommitteeIndex): bson.M{
					"index":    message.AggregatorIndex,
					"aggTime":  message.TimeStamp,
					"fromPeer": message.FromPeer,
					"source": bson.M{
						"epoch": message.Source.Epoch,
						"root":  hex.EncodeToString(message.Source.Root),
					},
					"target": bson.M{
						"epoch": message.Target.Epoch,
						"root":  hex.EncodeToString(message.Target.Root),
					},
				},
			}

			opts := options.UpdateOptions{}
			opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
			if err := coll.UpdateOne(ctx, uniqueKey, bson.M{
				"$set":         fieldSet,
				"$setOnInsert": fieldSetOnInsert,
				"$addToSet":    addToSet,
			}, opts); err != nil {
				if err != qmgo.ErrNoSuchDocuments {
					log.Printf("E! upsert %s err: %v", collectionName, err)
				}
			}

		}
	}
}

//
//func (c *client) upsertRoutine(ctx context.Context, collectionName string) {
//	dataChan, ok := c.collChanMap[collectionName]
//	if !ok {
//		return
//	}
//
//	for {
//		select {
//		case <-ctx.Done():
//			log.Println("store routine exit")
//			return
//		case data := <-dataChan:
//
//			track := data.(*MainnetPeerTrack)
//
//			if _, err := c.mongoClient.Database(TrackDB).Collection(collectionName).Upsert(context.Background(), bson.M{
//				"peerId": track.Info.ID,
//			}, track); err != nil {
//				log.Printf("E! Upsert err: %v", err)
//			}
//		}
//	}
//}
