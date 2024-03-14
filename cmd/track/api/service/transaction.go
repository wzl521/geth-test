package service

import (
	"context"
	"strconv"

	"github.com/ethereum/go-ethereum/cmd/track/api/models"
	"github.com/ethereum/go-ethereum/cmd/track/db"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

// GetAllTransactions function
// @Summary Get all transactions by paging
// @version 1.0
// @Tags transaction
// @Accept  json
// @Produce  json
// @Param page path int false "page index, start from 1, default 1"
// @Param pagesize path int false "page size, default 10"
// @Success 200 {object} models.TransactionsResponse
// @Router /transaction/all [get]
func GetAllTransactions(ctx *gin.Context) {
	page := int64(1)
	pagesize := int64(10)

	if p, err := strconv.ParseInt(ctx.Query("page"), 10, 64); err == nil {
		page = p
	}
	if p, err := strconv.ParseInt(ctx.Query("pagesize"), 10, 64); err == nil {
		pagesize = p
	}

	coll := db.DB.Database("track").Collection("txTrack")

	txs := make([]models.Transaction, 0, pagesize)
	if err := coll.Find(context.Background(), bson.M{}).Skip((page - 1) * pagesize).Limit(pagesize).Sort("-_id").All(&txs); err != nil {
		ctx.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}

	officalColl := db.ODB.Database("track").Collection("txTrack")
	total, err := officalColl.EstimatedDocumentCount(context.Background())
	if err != nil {
		ctx.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(200, models.TransactionsResponse{
		Total:        total,
		Transactions: txs,
	})

}

// GetTransaction function
// @Summary Get transaction by hash
// @version 1.0
// @Tags transaction
// @Accept  json
// @Produce  json
// @Param hash path string true "transaction hash"
// Success 200 {object} models.TransactionsResponse
// @Router /transaction/{hash} [get]
func GetTransaction(ctx *gin.Context) {
	hash := ctx.Param("hash")
	if hash == "" {
		ctx.JSON(500, gin.H{
			"error": "hash is empty",
		})
		return
	}

	coll := db.DB.Database("track").Collection("txTrack")

	txs := make([]models.Transaction, 0)
	if err := coll.Find(context.Background(), bson.M{"hash": hash}).All(&txs); err != nil {
		ctx.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(200, models.TransactionsResponse{
		Total:        int64(len(txs)),
		Transactions: txs,
	})
}
