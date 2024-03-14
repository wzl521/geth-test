package metrics

import (
	"context"
	"log"
	"net/http"

	"github.com/ethereum/go-ethereum/cmd/track/documents"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"
)

func importFromMongo(uri string) error {
	cli, err := qmgo.NewClient(context.Background(), &qmgo.Config{Uri: uri})
	if err != nil {
		return err
	}
	db := cli.Database(documents.TrackDB)
	query := db.Collection("mainnetPeer").Find(context.Background(), bson.M{})

	totalCount, err := query.Count()
	if err != nil {
		return err
	}
	log.Printf("mainnet count: %d", totalCount)

	cursor := query.Cursor()
	mnTrack := documents.MainnetPeerTrack{}

	clientCountMap := make(map[string]int)

	for cursor.Next(&mnTrack) {
		if client, version, arch, err := ParseName(mnTrack.MainnetPeerMessage.Info.Name); err != nil {
			clientCountMap["unknown"]++
			IncClientCount("unknown")
		} else {
			_, _, _ = client, version, arch
			clientCountMap[client]++

			IncClientCount(client)
			//log.Printf("parse peer name: %s %s %s", client, version, arch)
		}
	}
	for client, count := range clientCountMap {
		log.Printf("client: %s\t\t\t count: %d %.2f%%", client, count, float64(count)/float64(totalCount)*100)
	}

	return nil
}

func serve() {
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}

func ServeMetrics(mongoUri string) {
	if err := importFromMongo(mongoUri); err != nil {
		log.Printf("E! import from mongo error: %v", err)
	}
	go serve()
}
