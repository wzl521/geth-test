package models

type Top10Countries []CountryCount

type CountryCount struct {
	CountryCode    string `bson:"country"`
	Last24hrsCount int    `bson:"last24hrsCount"`
}

type Top10Response []CountryNode

type Node struct {
	Inbound       []map[string]bool `bson:"inbound"`
	Outbound      []map[string]bool `bson:"outbound"`
	TCP           int               `json:"TCP" bson:"tcp"`
	UDP           int               `json:"UDP" bson:"udp"`
	IP            string            `json:"IP" bson:"ip"`
	Pubkey        string            `json:"pubkey" bson:"pubkey"`
	Country       string            `json:"country" bson:"country"`
	Caps          []string          `json:"caps" bson:"caps"`
	ID            string            `json:"id" bson:"pubkey"` // Unique node identifier
	Name          string            `json:"name" bson:"name"`
	RemoteAddress string            `json:"remoteAddress" bson:"remote_address"` // Remote endpoint of the TCP data connection
	ENR           string            `json:"-" bson:"enr,omitempty"`              // Ethereum Node Record
	Enode         string            `json:"-" bson:"enode"`                      // Node URL
}

type NodesResponse struct {
	Total int64  `json:"total"`
	Nodes []Node `json:"data"`
}
type Nodes []Node

type CountryNode struct {
	Code       string          `json:"code"`
	Name       string          `json:"name"`
	Percentage string          `json:"percentage"`
	Value      int             `json:"value"`
	Stat       CountryNodeStat `json:"stat"`
}

type CountryNodeStat struct {
	Last24h string `json:"last24h"`
	Last7d  string `json:"last7d"`
}

type NodeStats struct {
	Total    int    `json:"total"`
	Last24h  string `json:"last24h"`
	Last7d   string `json:"last7d"`
	LastWeek []int  `json:"lastWeek"`
}
