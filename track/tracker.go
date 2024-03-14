package track

import (
	"net"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	communicate "github.com/ethereum/go-ethereum/track/proto"
	eth "github.com/prysmaticlabs/prysm/v3/proto/prysm/v1alpha1"

	"google.golang.org/grpc"
)

type Type uint8

const (
	Unknown Type = iota
	TransactionBroadcast
	BlockBroadcast
	MainnetPeer
	BlockHeadersResponse
	Connection
	BeaconBlockBroadcast
	AggregateAndProof
	BeaconPeer
)

func (t Type) String() string {
	switch t {
	case TransactionBroadcast:
		return "TransactionBroadcast"
	case BlockBroadcast:
		return "BlockBroadcast"
	case MainnetPeer:
		return "MainnetPeer"
	case BlockHeadersResponse:
		return "BlockHeadersResponse"
	case Connection:
		return "Connection"
	case BeaconBlockBroadcast:
		return "BeaconBlockBroadcast"
	case AggregateAndProof:
		return "AggregateAndProof"
	case BeaconPeer:
		return "BeaconPeer"
	default:
		return "Unknown"
	}
}

type (
	GeoJsonPoint struct {
		Type        string    `json:"type" bson:"type"`
		Coordinates []float64 `json:"coordinates" bson:"coordinates"`
	}
	Propagator struct {
		FromPeerId string       `json:"fromPeerId" bson:"fromPeerId"`
		FromPeerIP string       `json:"fromPeerIP" bson:"fromPeerIP"`
		IsHead     bool         `json:"isHead" bson:"isHead"`
		Timestamp  int64        `json:"timestamp" bson:"timestamp"`
		Location   GeoJsonPoint `json:"location" bson:"location"`
	}

	// TransactionBroadcastMessage for topic TransactionBroadcast
	TransactionBroadcastMessage struct {
		Hash       string     `json:"hash" redis:"hash"`
		Data       string     `json:"data" redis:"data"`
		Propagator Propagator `json:"propagators"`
		Value      string     `json:"value" redis:"value"`
		To         string     `json:"to" redis:"to"`
		From       string     `json:"from" redis:"from"`
		V          string     `json:"v" redis:"v"`
		R          string     `json:"r" redis:"r"`
		S          string     `json:"s" redis:"s"`
	}

	// BlockBroadcastMessage for topic BlockBroadcast
	BlockBroadcastMessage struct {
		Number     uint64        `json:"number" redis:"number"`
		Hash       string        `json:"hash" redis:"hash"`
		UncleCount uint64        `json:"uncle_count" redis:"uncle_count"`
		TxCount    uint64        `json:"tx_count" redis:"tx_count"`
		Propagator Propagator    `json:"propagators"`
		Header     *types.Header `json:"headers" bson:"headers"`
	}

	PeerInfo struct {
		ENR     string   `json:"enr,omitempty" bson:"enr,omitempty"` // Ethereum Node Record
		Enode   string   `json:"enode" bson:"enode"`                 // Node URL
		ID      string   `json:"id" bson:"id"`                       // Unique node identifier
		Name    string   `json:"name" bson:"name"`                   // Name of the node, including client type, version, OS, custom data
		Caps    []string `json:"caps" bson:"caps"`                   // Protocols advertised by this peer
		Network struct {
			LocalAddress  string `json:"localAddress" bson:"-"`               // Local endpoint of the TCP data connection
			RemoteAddress string `json:"remoteAddress" bson:"remote_address"` // Remote endpoint of the TCP data connection
			Inbound       bool   `json:"inbound" bson:"-"`
			Trusted       bool   `json:"trusted" bson:"trusted"`
			Static        bool   `json:"static" bson:"static"`
		} `json:"network" bson:",inline"`
		Protocols map[string]interface{} `json:"protocols" bson:"-"` // Sub-protocol specific metadata fields
	}

	// MainnetPeerMessage for topic MainnetPeer
	MainnetPeerMessage struct {
		Timestamp int64    `json:"timestamp" bson:"-"`
		Type      uint8    `json:"type" bson:"-"`
		Pubkey    string   `json:"pubkey" bson:"pubkey"`
		Info      PeerInfo `json:"info" bson:",inline"`
		IP        string   `json:"IP" bson:"ip"`
		TCP       int      `json:"TCP" bson:"tcp"`
		UDP       int      `json:"UDP" bson:"udp"`
	}

	// BlockHeaderResponseMessage for topic BlockHeadersResponse
	BlockHeaderResponseMessage struct {
		Timestamp int64 `json:"timestamp" bson:"-"`
		//Number    uint64          `json:"number" redis:"number"`
		Hash   string        `json:"hash" bson:"hash"`
		Header *types.Header `json:"headers" bson:"headers"`
	}

	// ConnectionMessage for topic Connection
	ConnectionMessage struct {
		PeerID          string `json:"peerID" bson:"peerID" redis:"peerID"`
		Addr            string `json:"addr" bson:"addr" redis:"addr"`
		IsDisconnection bool
		CountryCode     string `json:"-" bson:"-"`
		Reason          string `json:"reason" bson:"reason"`
		TimeStamp       int64  `json:"timestamp" bson:"timestamp"`
	}

	BeaconBlock struct {
		Slot      uint64
		Graffiti  []byte
		BlockHash []byte
		Proposer  uint64
		FromPeer  string
		TimeStamp int64
	}
	Aggregate struct {
		AggregatorIndex uint64
		BeaconBlockRoot []byte
		Slot            uint64
		CommitteeIndex  uint64
		Source          *eth.Checkpoint
		Target          *eth.Checkpoint
		FromPeer        string
		BitList         string
		BitCount        uint64
		TimeStamp       int64
	}
	BeaconPeerMessage struct {
		Type         uint8
		MultiAddr    string
		Pubkey       string
		AgentVersion string
		Direction    uint8
		TimeStamp    int64
	}
)

type PropagatorList []Propagator

func (l PropagatorList) Less(i, j int) bool {
	return l[i].Timestamp < l[j].Timestamp
}

func (l PropagatorList) Len() int {
	return len(l)
}

func (l PropagatorList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

var (
	t *tracker
)

type tracker struct {
	ch chan *message // emit channel
}

type message struct {
	topic     Type
	timestamp int64 // unix timestamp, milliseconds
	data      interface{}
}

func EmitTrack(topic Type, ts int64, data interface{}) {
	if t == nil {
		return
	}
	msg := message{topic, ts, data}
	t.ch <- &msg
}

func ServerInit() {

	server := Server{
		subs: make(map[string]*clientSubContext),
	}

	t = &tracker{
		ch: make(chan *message, 1),
	}
	go func() {
		ticker := time.NewTicker(time.Minute)

		for {
			select {
			case message := <-t.ch:
				server.m.Lock()
				for _, sub := range server.subs {
					select {
					case sub.msgCh <- *message:
					case <-sub.ctx.Done():
						continue
					}
				}
				server.m.Unlock()
			case <-ticker.C:
				server.m.Lock()
				for sub, ctx := range server.subs {
					log.Info("[TrackServer] subscriber", "sub", sub, "chSize", len(ctx.msgCh))
				}
				server.m.Unlock()

			}
		}
	}()

	if err := os.Remove("/opt/geth/track.sock"); err != nil {
		log.Error("clean socket", "err", err)
	}

	lis, err := net.Listen("unix", "/opt/geth/track.sock")
	if err != nil {
		log.Error("failed to listen", "err", err)
		panic(err)
	}
	var opts []grpc.ServerOption
	//if *tls {
	//	if *certFile == "" {
	//		*certFile = data.Path("x509/server_cert.pem")
	//	}
	//	if *keyFile == "" {
	//		*keyFile = data.Path("x509/server_key.pem")
	//	}
	//	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	//	if err != nil {
	//		log.Fatalf("Failed to generate credentials %v", err)
	//	}
	//	opts = []grpc.ServerOption{grpc.Creds(creds)}
	//}
	grpcServer := grpc.NewServer(opts...)
	communicate.RegisterTrackPuberServer(grpcServer, &server)
	go grpcServer.Serve(lis)
}
