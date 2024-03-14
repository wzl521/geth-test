package main

import (
	"context"
	"github.com/qiniu/qmgo"
	"github.com/qiniu/qmgo/options"
	"go.mongodb.org/mongo-driver/bson"
	officialOpts "go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"strings"
	"time"
)

type DisconnectionMessage struct {
	PeerID    string `json:"peerID" bson:"peerID"`
	Addr      string `json:"addr" bson:"addr"`
	Reason    string `json:"reason" bson:"reason"`
	TimeStamp int64  `json:"timestamp" bson:"timestamp"`
}

func main() {
	mongodbURI := `mongodb://admin:123456@localhost:27017`

	cli, err := qmgo.NewClient(context.Background(), &qmgo.Config{Uri: mongodbURI})
	if err != nil {
		log.Fatalln(err)
	}
	oldCollName := `disconnection`
	newCollName := `discReasonTest`

	oldColl := cli.Database("track").Collection(oldCollName)

	query := oldColl.Find(context.Background(), bson.M{})
	cursor := query.Cursor()

	var disconnectionMessage DisconnectionMessage

	total, err := query.Count()
	if err != nil {
		log.Fatalln(err)
	}
	count := 0

	newColl := cli.Database("track").Collection(newCollName)

	ticker := time.NewTicker(time.Minute)
loop:
	for {
		select {
		case <-ticker.C:
			log.Printf("process: %f", float64(count)/float64(total))
		default:
			if next := cursor.Next(&disconnectionMessage); next {
				count += 1
				reason := disconnectionMessage.Reason
				if strings.HasSuffix(reason, "connection reset by peer") {
					reason = "tcp: connection reset by peer"
				}

				incMap := bson.M{
					reason: 1,
				}

				opts := options.UpdateOptions{}
				opts.UpdateOptions = officialOpts.Update().SetUpsert(true)
				if err := newColl.UpdateOne(context.Background(),
					bson.M{
						"peerID": disconnectionMessage.PeerID,
						"addr":   disconnectionMessage.Addr,
					},
					bson.M{
						"$inc": incMap,
					},
					opts); err != nil && err != qmgo.ErrNoSuchDocuments {
					log.Printf("E! upsert %s err: %v", newCollName, err)
				}
			} else {
				break loop
			}
		}
	}

	if err := cursor.Close(); err != nil {
		log.Fatalln(err)
	}
}
