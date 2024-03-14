package db

import (
	"context"
	"github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

var DB *qmgo.Client
var ODB *mongo.Client

func MustInitDB(uri string) {
	if cli, err := qmgo.NewClient(context.Background(), &qmgo.Config{
		Uri: uri,
	}); err != nil {
		log.Fatalln(err)
	} else {
		DB = cli
	}

	if cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI(uri)); err != nil {
		log.Fatalln(err)
	} else {
		ODB = cli
	}
}
