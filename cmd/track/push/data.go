package push

const (
	BlockDataPackage = 1 << iota
	OnlineDataPackage
	TransactionDataPackage
	BeaconBlockDataPackage
	AggregateDataPackage
	BeaconPeerPackage
)

type Typed interface {
	GetType() PackageEnum
}

type (
	PackageEnum uint64

	PackageType struct {
		T string `json:"@type,omitempty"`
		t PackageEnum
	}

	BlockDataWrap struct {
		PackageType
		BlockData
	}
	BlockData struct {
		BlockHash    string `json:"blockHash"`
		Crawler      string `json:"crawler"`
		BlockTime    int64  `json:"blockTime"`
		Counter      string `json:"counter"`
		Network      string `json:"network"`
		PropagatorID string `json:"propagatorID"`
		PropagatorIP string `json:"propagatorIP"`
	}

	OnlineDataWrap struct {
		PackageType
		OnlineData
	}
	OnlineData struct {
		PeerID     string `json:"peerID"`
		PeerIDHash int64  `json:"peerIDHash"`
		Country    string `json:"country"`
		Crawler    string `json:"crawler"`
		OnLen      int64  `json:"onLen"`
		Counter    string `json:"counter"`
		Network    string `json:"network"`
		OffTime    int64  `json:"offTime"`
		ConIP      string `json:"conIP"`
	}

	TransDataWrap struct {
		PackageType
		TransData
	}
	TransData struct {
		Counter      string `json:"counter"`
		TransHash    string `json:"transHash"`
		TransTime    int64  `json:"transTime"`
		Crawler      string `json:"crawler"`
		Network      string `json:"network"`
		PropagatorID string `json:"propagatorID"`
		PropagatorIP string `json:"propagatorIP"`
	}

	BeaconBlockDataWrap struct {
		PackageType
		BeaconBlockData
	}

	BeaconBlockData struct {
		Slot      uint64 `json:"slot"`
		Graffiti  string `json:"graffiti"`
		BlockHash string `json:"blockHash"`
		Proposer  uint64 `json:"proposer"`
		BlockTime int64  `json:"blockTime"`
		FromPeer  string `json:"fromPeer"`
		Network   string `json:"network"`
	}

	AggregateDataWrap struct {
		PackageType
		AggregateData
	}

	Checkpoint struct {
		Root  string
		Epoch uint64
	}

	AggregateData struct {
		Slot            uint64     `json:"slot"`
		AggregatorIndex uint64     `json:"index"`
		BeaconBlockRoot string     `json:"beaconBlockRoot"`
		CommitteeIndex  uint64     `json:"committeeIndex"`
		Source          Checkpoint `json:"source"`
		Target          Checkpoint `json:"target"`
		AggTime         int64      `json:"aggTime"`
		FromPeer        string     `json:"fromPeer"`
		BitList         string     `json:"bitList"`
		BitCount        uint64     `json:"bitCount"`
		Network         string     `json:"network"`
	}

	BeaconDataWrap struct {
		PackageType
		BeaconData
	}

	BeaconData struct {
		Type         uint8  `json:"type"`
		MultiAddr    string `json:"multiAddr"`
		Pubkey       string `json:"pubkey"`
		AgentVersion string `json:"agentVersion"`
		Direction    uint8  `json:"direction"`
		Network      string `json:"network"`
	}
)

func (e PackageEnum) String() string {
	switch e {
	case BlockDataPackage:
		return "BlockData"
	case OnlineDataPackage:
		return "OnlineData"
	case TransactionDataPackage:
		return "TransactionData"
	case BeaconBlockDataPackage:
		return "BeaconBlockData"
	case AggregateDataPackage:
		return "AggregateData"
	case BeaconPeerPackage:
		return "BeaconPeer"
	default:
		return ""
	}
}

func (t *PackageType) GetType() PackageEnum {
	return t.t
}

func (t *PackageType) GetTypeString() string {
	return t.t.String()
}

func NewBlockDataWrap(blockData BlockData) *BlockDataWrap {
	blockData.Network = "eth"
	return &BlockDataWrap{
		PackageType: PackageType{
			T: "cn.com.bsfit.slt.common.pojo.BlockData",
			t: BlockDataPackage,
		},
		BlockData: blockData,
	}
}

func NewOnlineDataWrap(onlineData OnlineData) *OnlineDataWrap {
	onlineData.Network = "eth"
	return &OnlineDataWrap{
		PackageType: PackageType{
			T: "cn.com.bsfit.slt.common.pojo.PeerOfflineData",
			t: OnlineDataPackage,
		},
		OnlineData: onlineData,
	}
}

func NewTransDataWrap(transData TransData) *TransDataWrap {
	transData.Network = "eth"
	return &TransDataWrap{
		PackageType: PackageType{
			T: "cn.com.bsfit.slt.common.pojo.TransData",
			t: TransactionDataPackage,
		},
		TransData: transData,
	}
}

func NewBeaconBlockWrap(data BeaconBlockData) *BeaconBlockDataWrap {
	data.Network = "eth"
	return &BeaconBlockDataWrap{
		PackageType:     PackageType{t: BeaconBlockDataPackage},
		BeaconBlockData: data,
	}
}

func NewAggregateWrap(data AggregateData) *AggregateDataWrap {
	data.Network = "eth"
	return &AggregateDataWrap{
		PackageType:   PackageType{t: AggregateDataPackage},
		AggregateData: data,
	}
}

func NewBeaconPeer(data BeaconData) *BeaconDataWrap {
	data.Network = "eth"
	return &BeaconDataWrap{
		PackageType: PackageType{t: BeaconPeerPackage},
		BeaconData:  data,
	}
}
