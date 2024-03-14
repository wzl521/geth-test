package api

import (
	"github.com/ethereum/go-ethereum/cmd/track/api/service"
	_ "github.com/ethereum/go-ethereum/cmd/track/docs"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	"github.com/swaggo/gin-swagger"
)

var Crawler string

func Serve(crawler_ string) {
	Crawler = crawler_

	router := gin.Default()

	// node
	router.GET("/node/top10", service.GetTop10Nodes)
	router.GET("/node/:id", service.GetNode)
	router.GET("/node/all", service.GetAllNodes)
	//router.GET("/node/stats", service.GetNodeStats) // deprecated

	// block
	router.GET("/block/all", service.GetAllBlocks) // java impl
	router.GET("/block/:number", service.GetBlock) // java impl

	// transaction
	router.GET("/transaction/all", service.GetAllTransactions)
	router.GET("/transaction/:hash", service.GetTransaction)

	router.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	router.Run()
}
