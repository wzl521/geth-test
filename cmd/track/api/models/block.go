package models

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"reflect"
)

const (
	HashLength    = 32
	AddressLength = 20
)
const (
	BloomByteLength = 256
	BloomBitLength  = 8 * BloomByteLength
)

type Bloom [BloomByteLength]byte

func (b *Bloom) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, hexutil.Encode(b[:]))), nil
}

type Address [AddressLength]byte

func (a *Address) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, hexutil.Encode(a[:]))), nil
}

type Hash [HashLength]byte

var (
	hashT = reflect.TypeOf(Hash{})
)

func (h Hash) Hex() string { return hexutil.Encode(h[:]) }
func (h *Hash) UnmarshalJSON(input []byte) error {
	return hexutil.UnmarshalFixedJSON(hashT, input, h[:])
}
func (h *Hash) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", h.String())), nil
}

func (h Hash) String() string {
	return h.Hex()
}

type BlockNonce [8]byte

func (b *BlockNonce) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, hexutil.Encode(b[:]))), nil
}

type (
	Header struct {
		ParentHash  Hash    `json:"parentHash" bson:"parenthash"`
		UncleHash   Hash    `json:"sha3Uncles" bson:"unclehash"`
		Coinbase    Address `json:"miner" bson:"coinbase"`
		Root        Hash    `json:"stateRoot" bson:"root"`
		TxHash      Hash    `json:"transactionsRoot" bson:"txhash"`
		ReceiptHash Hash    `json:"receiptsRoot" bson:"receipthash"`
		Bloom       Bloom   `json:"logsBloom" bson:"bloom"`
		//Difficulty  *big.Int   `json:"difficulty" bson:"difficulty"`
		//Number      *big.Int   `json:"number" bson:"number"`
		GasLimit  uint64     `json:"gasLimit" bson:"gaslimit"`
		GasUsed   uint64     `json:"gasUsed" bson:"gasused"`
		Time      uint64     `json:"timestamp" bson:"time"`
		Extra     []byte     `json:"extraData" bson:"extra"`
		MixDigest Hash       `json:"mixHash" bson:"mixdigest"`
		Nonce     BlockNonce `json:"nonce" bson:"nonce"`
		//BaseFee *big.Int `json:"baseFeePerGas" bson:"basefee"`
	}
	Propagator struct {
		FromPeerId string `json:"fromPeerId" bson:"fromPeerId"`
		IsHead     bool   `json:"isHead" bson:"isHead"`
		Timestamp  int64  `json:"timestamp" bson:"timestamp"`
	}
	Block struct {
		Hash        string       `json:"hash" bson:"hash"`
		Number      int          `json:"number" bson:"number"`
		Propagators []Propagator `json:"propagators" bson:"propagators"`
		Headers     []Header     `json:"headers" bson:"headers"`
	}
	BlocksResponse struct {
		Total  int64   `json:"total"`
		Blocks []Block `json:"data"`
	}
)
