package documents

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/track"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	TrackDB = "track"

	BlockTrackCollection  = "blockTrack"
	TxTrackCollection     = "txTrack"
	MainnetPeer           = "mainnetPeer"
	Disconnection         = "discReason"
	PeerPool              = "peerPool"
	BeaconBlockCollection = "beaconBlockTrack"
	BeaconPeer            = "beaconPeer"
	AggregateProof        = "aggregateProof"

	BatchSize = 256
)

type (
	//ConnectionTrack struct {
	//	Datetime                    primitive.DateTime `bson:"datetime"`
	//	track.PeerConnectionMessage `bson:",inline"`
	//}

	TransactionTrack struct {
		Hash         string             `json:"hash" bson:"hash"`
		Data         string             `json:"data" bson:"data"`
		Value        string             `json:"value" bson:"value"`
		Propagators  []track.Propagator `json:"propagators	" bson:"propagators"`
		UpdateTimeAt primitive.DateTime `bson:"updateTimeAt"`
	}

	BlockTrack struct {
		Number      uint64             `json:"number" bson:"number"`
		Hash        string             `json:"hash" bson:"hash"`
		Propagators []track.Propagator `json:"propagators	" bson:"propagators"`
		Header      *types.Header      `json:"header" bson:"header"`
		TxCount     uint64             `json:"tx_count" bson:"txCount"`
		UncleCount  uint64             `json:"uncle_count" bson:"uncleCount"`
	}

	MainnetPeerTrack struct {
		Inbound                  []map[string]bool `bson:"inbound"`
		Outbound                 []map[string]bool `bson:"outbound"`
		track.MainnetPeerMessage `bson:",inline"`
	}

	PeerPoolTrack struct {
		Pubkey      string         `json:"pubkey" bson:"pubkey"`
		PoolMapping map[string]int `json:"pool_mapping" bson:"poolMapping"`
	}
)

func MarshalPropagator(p *track.Propagator) string {
	return fmt.Sprintf("%s|%s|%t|%d", p.FromPeerId, p.FromPeerIP, p.IsHead, p.Timestamp)
}

func UnmarshalPropagator(key string) *track.Propagator {
	strList := strings.Split(key, "|")
	if len(strList) != 4 {
		return nil
	}
	ts, _ := strconv.ParseInt(strList[3], 10, 64)

	return &track.Propagator{
		FromPeerId: strList[0],
		FromPeerIP: strList[1],
		IsHead:     strList[2] == "true",
		Timestamp:  ts,
	}
}
