package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/ethereum/go-ethereum/cmd/track/api"
	"github.com/ethereum/go-ethereum/cmd/track/db"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"

	_ "github.com/ethereum/go-ethereum/cmd/track/docs"
	"github.com/ethereum/go-ethereum/cmd/track/metrics"
	"github.com/ethereum/go-ethereum/track"
	communicate "github.com/ethereum/go-ethereum/track/proto"

	"google.golang.org/grpc"
)

var (
	mongodbURI    string
	redisAddr     string
	redisPoolSize int
	//streamCube      string
	streamcubeUrls   string
	gethTrackServer  string
	prysmTrackServer string
	gethRpc          string
	enableMetrics    bool
	httpAPIServe     bool
	dbTxExpireDays   int
)

func init() {
	flag.StringVar(&mongodbURI, "mongodb_uri", "mongodb://admin:123456@localhost:27017", "MongoDB URI")
	flag.StringVar(&redisAddr, "redis_addr", "localhost:6379", "Redis addr")
	flag.StringVar(&streamcubeUrls, "streamcube_urls", "", "stream cube push urls")
	flag.StringVar(&gethTrackServer, "geth", "passthrough:///unix:///tmp/track.sock", "geth grpc addr")
	flag.StringVar(&prysmTrackServer, "prysm", "passthrough:///unix:///tmp/beacon_track.sock", "prysm grpc addr")
	flag.IntVar(&redisPoolSize, "redis_pool_size", 1000, "redis client pool size")
	flag.StringVar(&gethRpc, "geth_rpc", "http://localhost:8545", "geth rpc addr")
	flag.BoolVar(&enableMetrics, "metrics", false, "enable prometheus metrics http server")
	flag.BoolVar(&httpAPIServe, "http_serve", false, "http serve")
	flag.IntVar(&dbTxExpireDays, "db_tx_expire_days", 1, "days for ttl in txTrack")

	flag.Parse()
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	storeClient, err := createClient()
	if err != nil {
		log.Println("Error creating client", "error", err)
		return
	}
	storeClient.Run()

	if httpAPIServe {
		db.MustInitDB(mongodbURI)
		api.Serve(storeClient.crawlerID)
	}

	if enableMetrics {
		metrics.ServeMetrics(mongodbURI)
	}

	go ELMessageFetch(storeClient)

	go CLMessageFetch(storeClient)

	go http.ListenAndServe(":16060", nil)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
}

func ELMessageFetch(storeClient *client) {

	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(gethTrackServer, opts...)

	if err != nil {
		log.Fatalf("fail to dial: %v\n", err)
	}
	defer conn.Close()
	client := communicate.NewTrackPuberClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.GetTrackList(ctx, &communicate.GetTrackRequest{})
	if err != nil {
		log.Fatalln("Error creating stream", "error", err)
	}

	for {
		messageList, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln("Error receiving message", "error", err)
		}
		if messageList != nil {
			for _, message := range messageList.List {

				switch track.Type(message.Topic) {
				//case track.PeerConnection:
				//	msg := &ConnectionTrack{}
				//	if err := json.Unmarshal([]byte(message.Data), msg); err != nil {
				//		log.Fatalln("Error unmarshalling message", "error", err)
				//	}
				//	msg.Datetime = dateTime
				//	storeClient.push(msg)
				case track.BlockBroadcast:
					msg := &track.BlockBroadcastMessage{}
					if err := json.Unmarshal(message.Data, msg); err != nil {
						log.Fatalln("Error unmarshalling message", "error", err)
					}
					msg.Propagator.Timestamp = message.Timestamp
					storeClient.push(msg)
				case track.TransactionBroadcast:
					msg := &track.TransactionBroadcastMessage{}
					if err := json.Unmarshal(message.Data, msg); err != nil {
						log.Fatalln("Error unmarshalling message", "error", err)
					}
					msg.Propagator.Timestamp = message.Timestamp
					storeClient.push(msg)
				case track.MainnetPeer:
					msg := &track.MainnetPeerMessage{}
					if err := json.Unmarshal(message.Data, msg); err != nil {
						log.Fatalln("Error unmarshalling message", "error", err)
					}
					msg.Timestamp = message.Timestamp
					storeClient.push(msg)
				case track.BlockHeadersResponse:
					msg := &track.BlockHeaderResponseMessage{}
					if err := json.Unmarshal(message.Data, msg); err != nil {
						log.Fatalln("Error unmarshalling message", "error", err)
					}
					msg.Timestamp = message.Timestamp
					storeClient.push(msg)
				case track.Connection:
					msg := &track.ConnectionMessage{}
					if err := json.Unmarshal(message.Data, msg); err != nil {
						log.Fatalln("Error unmarshalling message", "error", err)
					}
					msg.TimeStamp = message.Timestamp
					storeClient.push(msg)
				}
			}

		}
	}
}

func CLMessageFetch(storeClient *client) {

	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(prysmTrackServer, opts...)

	if err != nil {
		log.Fatalf("fail to dial: %v\n", err)
	}
	defer conn.Close()
	client := communicate.NewTrackPuberClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.GetTrackList(ctx, &communicate.GetTrackRequest{})
	if err != nil {
		log.Fatalln("Error creating stream", "error", err)
	}

	for {
		messageList, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln("Error receiving message", "error", err)
		}
		if messageList == nil {
			break
		}
		for _, message := range messageList.List {

			switch track.Type(message.Topic) {
			case track.BeaconPeer:
				msg := &track.BeaconPeerMessage{}
				if err := json.Unmarshal(message.Data, msg); err != nil {
					log.Fatalln("Error unmarshalling message", "error", err)
				}
				msg.TimeStamp = message.Timestamp
				storeClient.push(msg)
			case track.BeaconBlockBroadcast:
				msg := &track.BeaconBlock{}
				if err := json.Unmarshal(message.Data, msg); err != nil {
					log.Fatalln("Error unmarshalling message", "error", err)
				}
				msg.TimeStamp = message.Timestamp
				storeClient.push(msg)
			case track.AggregateAndProof:
				msg := &track.Aggregate{}
				if err := json.Unmarshal(message.Data, msg); err != nil {
					log.Fatalln("Error unmarshalling message", "error", err)
				}
				msg.TimeStamp = message.Timestamp
				storeClient.push(msg)
			}
		}
	}
}
