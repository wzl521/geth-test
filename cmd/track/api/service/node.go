package service

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/cmd/track/api/models"
	"github.com/ethereum/go-ethereum/cmd/track/db"

	"github.com/gin-gonic/gin"
	"github.com/pariz/gountries"
	"go.mongodb.org/mongo-driver/bson"
)

// GetTop10Nodes function
// @Summary Get top 10 nodes
// @version 1.0
// @Tags node
// @Accept  json
// @Produce  json
// @Success 200 {object} models.Top10Response
// @Router /node/top10 [get]
func GetTop10Nodes(ctx *gin.Context) {

	timeNow := time.Now()
	top10 := models.Top10Countries{}

	coll := db.DB.Database("track").Collection("mainnetPeer")

	total, err := coll.Find(context.Background(), bson.M{
		"lastSeen": bson.M{
			"$gte": timeNow.Add(-time.Hour * 24).UnixMilli(),
		},
	}).Count()
	if err != nil {
		ctx.JSON(500, gin.H{
			"error": err,
		})
		return
	}

	if err := coll.Aggregate(context.Background(), []bson.M{
		{
			"$match": bson.M{
				"lastSeen": bson.M{
					"$gte": timeNow.Add(-time.Hour * 24 * 7).UnixMilli(),
				},
			},
		},
		{
			"$project": bson.M{
				"country": 1,
				"before24hrs": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$and": []interface{}{
								bson.M{"$lt": []interface{}{"$lastSeen", timeNow.Add(-time.Hour * 24).UnixMilli()}},
								bson.M{"$gte": []interface{}{"$lastSeen", timeNow.Add(-time.Hour * 48).UnixMilli()}},
							},
						},
						1, 0,
					},
				},
				"last24hrs": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$and": []interface{}{
								bson.M{"$lt": []interface{}{"$lastSeen", timeNow.UnixMilli()}},
								bson.M{"$gte": []interface{}{"$lastSeen", timeNow.Add(-time.Hour * 24).UnixMilli()}},
							},
						},
						1, 0,
					},
				},
				"last7days": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$and": []interface{}{
								bson.M{"$lt": []interface{}{"$lastSeen", timeNow.UnixMilli()}},
								bson.M{"$gte": []interface{}{"$lastSeen", timeNow.Add(-time.Hour * 24 * 7).UnixMilli()}},
							},
						},
						1, 0,
					},
				},
				"before7days": bson.M{
					"$cond": []interface{}{
						bson.M{
							"$and": []interface{}{
								bson.M{"$lt": []interface{}{"$lastSeen", timeNow.Add(-time.Hour * 24 * 7).UnixMilli()}},
								bson.M{"$gte": []interface{}{"$lastSeen", timeNow.Add(-time.Hour * 24 * 14).UnixMilli()}},
							},
						},
						1, 0,
					},
				},
			},
		},
		{
			"$group": bson.M{
				"_id":            "$country",
				"last24hrsCount": bson.M{"$sum": "$last24hrs"},
			},
		},
		{

			"$sort": bson.M{
				"last24hrsCount": -1,
			},
		},
		{
			"$limit": 10,
		},
		{
			"$project": bson.M{
				"country":        "$_id",
				"last24hrsCount": 1,
			},
		},
	}).All(&top10); err != nil {
		log.Printf("E! Error getting top 10 nodes: %s", err)
		ctx.JSON(500, gin.H{
			"error": err,
		})
		return
	}

	response := make(models.Top10Response, 0, 10)
	query := gountries.New()

	for _, top := range top10 {

		last24h := ""
		last7d := ""
		countryName := ""
		percentage := ""

		if total != 0 {
			percentage = fmt.Sprintf("%.2f%%", float64(top.Last24hrsCount)/float64(total)*100)
		}

		if result, err := query.FindCountryByAlpha(top.CountryCode); err == nil {
			countryName = result.Name.Common
		}
		//if top.Before24hrsCount != 0 {
		//	last24h = fmt.Sprintf("%.2f%%", float64(top.Last24hrsCount-top.Before24hrsCount)/float64(top.Before24hrsCount)*100)
		//}
		//if top.Before7daysCount != 0 {
		//	last7d = fmt.Sprintf("%.2f%%", float64(top.Last7daysCount-top.Before7daysCount)/float64(top.Before7daysCount)*100)
		//}

		response = append(response, models.CountryNode{
			Code:       top.CountryCode,
			Name:       countryName,
			Percentage: percentage,
			Value:      top.Last24hrsCount,
			Stat: models.CountryNodeStat{
				Last24h: last24h,
				Last7d:  last7d,
			},
		})
	}

	ctx.JSON(200, response)
}

// GetNode function
// @Summary Get node
// @version 1.0
// @Tags node
// @Accept  json
// @Produce  json
// @Param  id   path      string  true  "node ID"
// @Success 200 {object} models.NodesResponse
// @Router /node/{id} [get]
func GetNode(ctx *gin.Context) {
	nodeID := ctx.Param("id")
	if nodeID == "" {
		ctx.JSON(500, gin.H{
			"error": "no id provided",
		})
		return
	}
	coll := db.DB.Database("track").Collection("mainnetPeer")

	var node models.Node
	if err := coll.Find(context.Background(), bson.M{
		"id": nodeID,
	}).One(&node); err != nil {
		ctx.JSON(404, gin.H{
			"error": err.Error(),
		})
		return
	}

	ctx.JSON(200, models.NodesResponse{
		Total: 1,
		Nodes: models.Nodes{
			node,
		},
	})
}

// GetAllNodes function
// @Summary Get all nodes by paging
// @version 1.0
// @Tags node
// @Accept json
// @Produce json
// @Param page path int false "page index, start from 1, default 1"
// @Param pagesize path int false "page size, default 10"
// @Success 200 {object} models.NodesResponse
// @Router /node/all [get]
func GetAllNodes(ctx *gin.Context) {
	page := int64(1)
	pagesize := int64(10)

	if p, err := strconv.ParseInt(ctx.Query("page"), 10, 64); err == nil {
		page = p
	}
	if p, err := strconv.ParseInt(ctx.Query("pagesize"), 10, 64); err == nil {
		pagesize = p
	}

	coll := db.DB.Database("track").Collection("mainnetPeer")

	nodes := make(models.Nodes, 0, pagesize)

	if err := coll.Find(context.Background(), bson.M{
		"lastSeen": bson.M{
			"$gte": time.Now().Add(-time.Hour * 24).UnixMilli(),
		},
	}).Sort("-lastSeen").Skip((page - 1) * pagesize).Limit(pagesize).All(&nodes); err != nil {
		log.Printf("E! Error getting all nodes: %s", err)
		ctx.JSON(500, gin.H{
			"error": err,
		})
		return
	}

	total, err := coll.Find(context.Background(), bson.M{
		"lastSeen": bson.M{
			"$gte": time.Now().Add(-time.Hour * 24).UnixMilli(),
		},
	}).Count()
	if err != nil {
		ctx.JSON(500, gin.H{
			"error": err,
		})
		return
	}

	ctx.JSON(200, models.NodesResponse{
		Total: total,
		Nodes: nodes,
	})
}

type GetNodesCount struct {
	Total          int `bson:"total"`
	Last24hrsCount int `bson:"last24hrsCount"`
}

// GetNodeStats function
// @Summary Get node stats
// @version 1.0
// @Tags node
// @Produce json
// @Success 200 {object} models.NodeStats
// @Router /node/stats [get]
//func GetNodeStats(ctx *gin.Context) {
//	timeNow := time.Now()
//
//	last24h := "0.00%"
//	last7d := "0.00%"
//
//	queries := make([]*client.Query, 0)
//
//	tomorrow := timeNow.Add(time.Second * 86400)
//
//	year, month, day := tomorrow.Date()
//	tomorrowBegin := time.Date(year, month, day, 0, 0, 0, 0, tomorrow.Location())
//
//	for i := 0; i < 7; i++ {
//		query_it := client.NewQuery()
//		queries = append(queries,
//			query_it.KeyName(api.Crawler).
//				Namespace("Block").
//				Dim("探测点").
//				Reference(tomorrowBegin.Add(time.Duration(-i*86400)*time.Second).UnixMilli()).
//				Duration("1d").
//				AddVar(&client.Var{
//					Name: "某探测点连接的去重节点数TOP10",
//					ID:   int64(i),
//				}),
//		)
//	}
//
//	by, err := client.DoRequest(queries)
//	if err != nil {
//		ctx.JSON(500, gin.H{
//			"error": err.Error(),
//		})
//		return
//	}
//
//	ctx.JSON(200, models.NodeStats{
//		Total:   nodesCount.Total,
//		Last24h: last24h,
//		Last7d:  last7d,
//		LastWeek: []int{
//			2000, 2100, 2200, 2300, 2400, 2500, 2333,
//		},
//	})
//
//}
