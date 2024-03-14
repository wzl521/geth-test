package client

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
)

var streamCubeUrl string

func SetStreamCubeUrl(url string) {
	streamCubeUrl = url
}

type Var struct {
	Name string `json:"name"`
	ID   int64  `json:"id"`
}
type Query struct {
	Key     Key            `json:"key"`
	VarsMap map[string]Var `json:"varsMap"`
}

type Key struct {
	NS        string `json:"ns"`
	Dim       string `json:"dim"`
	Duration  string `json:"dur"`
	Reference int64  `json:"ref"`
	KeyName   string `json:"name"`
}

// Response
type Response struct {
}

func NewQuery() *Query {
	return &Query{
		VarsMap: make(map[string]Var),
	}
}

func (q *Query) Namespace(ns string) *Query {
	q.Key.NS = ns
	return q
}

func (q *Query) Dim(dim string) *Query {
	q.Key.Dim = dim
	return q
}

func (q *Query) Duration(dur string) *Query {
	q.Key.Duration = dur
	return q
}

func (q *Query) Reference(ref int64) *Query {
	q.Key.Reference = ref
	return q
}

func (q *Query) KeyName(name string) *Query {
	q.Key.KeyName = name
	return q
}

func (q *Query) AddVar(var_ *Var) *Query {
	q.VarsMap[strconv.FormatInt(var_.ID, 10)] = *var_
	return q
}

func DoRequest(queries []*Query) ([]byte, error) {
	queriesJSON, _ := json.Marshal(queries)

	var err error
	response, err := http.Post(streamCubeUrl, "application/json", bytes.NewReader(queriesJSON))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	return ioutil.ReadAll(response.Body)
}
