package service

import (
	"context"
	"strconv"

	"github.com/ethereum/go-ethereum/cmd/track/api/models"
	"github.com/ethereum/go-ethereum/cmd/track/db"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

// GetAllBlocks function
// @Summary Get all blocks by paging
// @version 1.0
// @Tags block
// @Accept json
// @Produce json
// @Param page path int true "page index, start from 1, default 1"
// @Param pagesize path int true "page size, default 10"
// @Success 200 {object} models.BlocksResponse
// @Router /block/all [get]
func GetAllBlocks(ctx *gin.Context) {
	page := int64(1)
	pagesize := int64(10)

	if page_, err := strconv.ParseInt(ctx.Query("page"), 10, 64); err == nil {
		page = page_
	}
	if pagesize_, err := strconv.ParseInt(ctx.Query("pagesize"), 10, 64); err == nil {
		pagesize = pagesize_
	}

	coll := db.DB.Database("track").Collection("blockTrack")

	var blocks []models.Block
	if err := coll.Find(context.Background(), bson.M{}).Sort("-number").Skip((page - 1) * pagesize).Limit(pagesize).All(&blocks); err != nil {
		ctx.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	officialColl := db.ODB.Database("track").Collection("blockTrack")
	total, err := officialColl.EstimatedDocumentCount(context.Background())
	if err != nil {
		ctx.JSON(500, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(200, models.BlocksResponse{
		Total:  total,
		Blocks: blocks,
	})
}

// GetBlock function
// @Summary Get block by block number
// @version 1.0
// @Tags block
// @Accept json
// @Produce json
// @Param blockNumber path int true "block number"
// @Success 200 {object} models.BlocksResponse
// @Router /block/{number} [get]
func GetBlock(ctx *gin.Context) {
	blockNumber := 0
	if p, err := strconv.Atoi(ctx.Param("number")); err == nil {
		blockNumber = p
	}

	coll := db.DB.Database("track").Collection("blockTrack")

	blocks := make([]models.Block, 0)
	if err := coll.Find(context.Background(), bson.M{"number": blockNumber}).All(&blocks); err != nil {
		ctx.JSON(500, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(200, models.BlocksResponse{
		Total:  int64(len(blocks)),
		Blocks: blocks,
	})
}
