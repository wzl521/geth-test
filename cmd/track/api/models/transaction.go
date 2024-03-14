package models

type Transaction struct {
	Hash        string       `json:"hash" bson:"hash"`
	Data        string       `json:"data" bson:"data"`
	Propagators []Propagator `json:"propagators" bson:"propagators"`
}

type TransactionsResponse struct {
	Total        int64         `json:"total"`
	Transactions []Transaction `json:"data"`
}
