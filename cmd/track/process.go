package main

import (
	"context"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"log"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/cmd/track/documents"
	"github.com/ethereum/go-ethereum/cmd/track/push"
	"github.com/ethereum/go-ethereum/track"

	"github.com/robfig/cron/v3"
)

const (
	TTL = time.Minute * time.Duration(10)

	// BlockKeyFormat is set for propagators
	BlockKeyFormat = `block:%d:%s`
	// DetailBlockKeyFormat is hashmap for details
	DetailBlockKeyFormat = `detail:` + BlockKeyFormat
	// TTLBlockKeyFormat is string for TTL
	TTLBlockKeyFormat = `shadow:` + BlockKeyFormat

	// TxKeyFormat is set for propagators
	TxKeyFormat = `tx:%s`
	// DetailTxKeyFormat is hashmap for details
	DetailTxKeyFormat = `detail:` + TxKeyFormat
	// TTLTxKeyFormat is string for TTL
	TTLTxKeyFormat = `shadow:` + TxKeyFormat

	DiscKeyFormat       = `disc:%s`
	DetailDiscKeyFormat = `detail:` + DiscKeyFormat
	TTLDiscKeyFormat    = `shadow:` + DiscKeyFormat
)

type blockPeerMapping struct {
	blockHash string
}

func initBlock(c *client) {

	docCh := make(chan interface{}, 1)
	mappingCh := make(chan blockPeerMapping, 1)

	go c.upsertBlockPropagators(context.Background(), docCh, mappingCh, documents.BlockTrackCollection)
	go c.updatePeerPoolMapping(context.Background(), mappingCh)
	go c.cache2mongo(context.Background(), c.bridgeChanMap[track.BlockBroadcast].(chan string), docCh)
}

func processBlock(c *client, msg interface{}) {
	m := msg.(*track.BlockBroadcastMessage)

	if m.Number == 0 {
		return
	}

	if _, err := c.blockScript.Run(context.Background(), c.redisClient, []string{
		fmt.Sprintf(BlockKeyFormat, m.Number, m.Hash),
		fmt.Sprintf(TTLBlockKeyFormat, m.Number, m.Hash),
		fmt.Sprintf(DetailBlockKeyFormat, m.Number, m.Hash),
	}, documents.MarshalPropagator(&m.Propagator), TTL.Seconds(), m.Number, m.Hash, m.TxCount, m.UncleCount).Result(); err != nil {
		log.Printf("E! run script err: %v", err)
		return
	}

	// bogus
	c.bridgeChanMap[track.BlockHeadersResponse].(chan interface{}) <- &track.BlockHeaderResponseMessage{
		Timestamp: m.Propagator.Timestamp,
		Hash:      m.Hash,
		Header:    m.Header,
	}
}

func initTx(c *client) {
	docCh := make(chan interface{}, 1)
	go c.storeMany(context.Background(), docCh, documents.TxTrackCollection)
	go c.cache2mongo(context.Background(), c.bridgeChanMap[track.TransactionBroadcast].(chan string), docCh)
}

func processTx(c *client, msg interface{}) {
	m := msg.(*track.TransactionBroadcastMessage)
	if m.Hash == "" {
		return
	}

	if _, err := c.txScript.Run(context.Background(), c.redisClient, []string{
		m.Hash,
		documents.MarshalPropagator(&m.Propagator),
	}, TTL.Seconds(), m.Data, m.Hash, m.Value).Result(); err != nil {
		log.Printf("E! run script err: %v", err)
		return
	}
}

func initMainnetPeer(c *client) {
	go c.upsertMainnetPeerConnection(context.Background(), c.bridgeChanMap[track.MainnetPeer].(chan interface{}), documents.MainnetPeer)
}

func processMainnetPeer(c *client, msg interface{}) {
	c.bridgeChanMap[track.MainnetPeer].(chan interface{}) <- msg.(*track.MainnetPeerMessage)

	m := msg.(*track.MainnetPeerMessage)

	if m.Pubkey == "" {
		return
	}

	// add to set
	peerIDSet.Add(m.Pubkey)

	peerIDShort := m.Pubkey[:6]

	hash, err := strconv.ParseInt(peerIDShort, 16, 64)
	if err != nil {
		log.Printf("E! parse hex err: %v", err)
		return
	}

	countryCode := ""
	if countryRecord, err := c.ip2locationDB.Get_country_short(m.IP); err != nil {
		log.Printf("E! get ip country err: %v ip: %s", err, m.IP)
	} else {
		countryCode = countryRecord.Country_short
	}

	// if it's connection type
	if m.Type == 1 {
		c.connectionMap.Store(m.Pubkey, m)

		c.httpClient.Emit(push.NewOnlineDataWrap(push.OnlineData{
			PeerID:     m.Pubkey,
			PeerIDHash: hash,
			Country:    countryCode,
			Crawler:    c.crawlerID,
			Counter:    "CON",
			OffTime:    m.Timestamp,
			ConIP:      m.IP,
		}))
		return
	} else if m.Type == 0 {
		if value, loaded := c.connectionMap.LoadAndDelete(m.Pubkey); loaded {
			c.httpClient.Emit(push.NewOnlineDataWrap(push.OnlineData{
				PeerID:     m.Pubkey,
				PeerIDHash: hash,
				Country:    countryCode,
				Crawler:    c.crawlerID,
				OnLen:      m.Timestamp - value.(*track.MainnetPeerMessage).Timestamp,
				Counter:    "CON",
				OffTime:    m.Timestamp,
				ConIP:      m.IP,
			}))
		}
	}

}

func initBlockHeaders(c *client) {
	go c.upsertBlockHeaders(context.Background(), c.bridgeChanMap[track.BlockHeadersResponse].(chan interface{}), documents.BlockTrackCollection)
}
func processBlockHeaders(c *client, msg interface{}) {
	c.bridgeChanMap[track.BlockHeadersResponse].(chan interface{}) <- msg.(*track.BlockHeaderResponseMessage)
}

var peerIDSet = mapset.NewSet()

func initDisconnection(c *client) {
	go c.upsertDisconnection(context.Background(), c.bridgeChanMap[track.Connection].(chan string), documents.Disconnection)
	// up a routine to report connection daily
	Cron := cron.New(cron.WithLocation(time.Local))
	Cron.AddFunc("59 23 * * *", func() {
		now := time.Now()
		log.Printf("CRON time: %v", now)

		c.connectionMap.Range(func(key, value interface{}) bool {
			peerIDShort := key.(string)[:6]
			hash, err := strconv.ParseInt(peerIDShort, 16, 64)
			if err != nil {
				log.Printf("E! parse int err: %v", err)
				return true
			}

			m := value.(*track.MainnetPeerMessage)
			countryCode := ""
			if countryRecord, err := c.ip2locationDB.Get_country_short(m.IP); err != nil {
				log.Printf("E! get ip country err: %v ip: %s", err, m.IP)
			} else {
				countryCode = countryRecord.Country_short
			}

			c.httpClient.Emit(push.NewOnlineDataWrap(push.OnlineData{
				PeerID:     m.Pubkey,
				PeerIDHash: hash,
				Country:    countryCode,
				Crawler:    c.crawlerID,
				OnLen:      now.UnixMilli() - m.Timestamp,
				Counter:    "CON",
				OffTime:    now.UnixMilli(),
				ConIP:      m.IP,
			}))
			m.Timestamp = now.UnixMilli()
			return true
		})
		peerIDSet.Clear()
	})
	Cron.AddFunc("* * * * *", func() {
		log.Printf("peerID set size: %v", peerIDSet.Cardinality())
	})
	go Cron.Run()
}

func processDisconnection(c *client, msg interface{}) {
	m := msg.(*track.ConnectionMessage)

	//check
	if m.PeerID == "" || m.IsDisconnection == false {
		return
	}

	if _, err := c.discScript.Run(context.Background(), c.redisClient, []string{
		fmt.Sprintf(DiscKeyFormat, m.PeerID),
		fmt.Sprintf(TTLDiscKeyFormat, m.PeerID),
		fmt.Sprintf(DetailDiscKeyFormat, m.PeerID),
	}, m.Reason, TTL.Seconds(), m.PeerID, m.Addr).Result(); err != nil {
		log.Printf("E! run script err: %v", err)
		return
	}
}

func initBeaconBlock(c *client) {
	go c.storeMany(
		context.Background(),
		c.bridgeChanMap[track.BeaconBlockBroadcast].(chan interface{}),
		documents.BeaconBlockCollection)
}

func processBeaconBlock(c *client, msg interface{}) {
	c.bridgeChanMap[track.BeaconBlockBroadcast].(chan interface{}) <- msg
}

func initAggregate(c *client) {
	go c.upsertAggregateProof(context.Background(),
		c.bridgeChanMap[track.AggregateAndProof].(chan interface{}),
		documents.AggregateProof)
}

func processAggregate(c *client, msg interface{}) {
	m := msg.(*track.Aggregate)
	c.bridgeChanMap[track.AggregateAndProof].(chan interface{}) <- m
}

func initBeaconPeer(c *client) {
	//go c.upsertMainnetPeerConnection(context.Background(), c.bridgeChanMap[track.MainnetPeer].(chan interface{}), documents.MainnetPeer)
	go c.upsertBeaconPeer(context.TODO(), c.bridgeChanMap[track.BeaconPeer].(chan interface{}), documents.BeaconPeer)
}

func processBeaconPeer(c *client, msg interface{}) {
	c.bridgeChanMap[track.BeaconPeer].(chan interface{}) <- msg.(*track.BeaconPeerMessage)

	// kafka push
}
