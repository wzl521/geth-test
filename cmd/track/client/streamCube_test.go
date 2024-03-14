package client

import (
	"testing"
	"time"
)

func TestDoRequest(t *testing.T) {
	SetStreamCubeUrl("http://10.100.1.232:8555/front/query")

	timeNow := time.Now()

	queries := make([]*Query, 0)

	tomorrow := timeNow.Add(time.Second * 86400)

	year, month, day := tomorrow.Date()
	tomorrowBegin := time.Date(year, month, day, 0, 0, 0, 0, tomorrow.Location())

	for i := 0; i < 7; i++ {
		query_it := NewQuery()
		queries = append(queries,
			query_it.KeyName("enode://f248070a1a89d41149813f1214eff5588344bdfbf2add8e2d1a828b69186804ef42474b554f7c6b50cf0b51b298079435fcfd768a1d62af131b4712e778d7828@123.157.235.205:30303").
				Namespace("Block").
				Dim("探测点").
				Reference(tomorrowBegin.Add(time.Duration(-i*86400)*time.Second).UnixMilli()).
				Duration("1d").
				AddVar(&Var{
					Name: "某探测点连接的去重节点数TOP10",
					ID:   int64(i),
				}),
		)
	}

	by, err := DoRequest(queries)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("response: %v", string(by))
}
