package push

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	BatchSize        int
	StreamcubeEnable bool
	KafkaEnable      bool
	KafkaAddr        string
	//KafkaTopicFilter uint64
)

func init() {
	flag.IntVar(&BatchSize, "batch", 50, "http post batch")
	flag.BoolVar(&StreamcubeEnable, "streamcube", false, "enable streamCube push client")
	flag.BoolVar(&KafkaEnable, "kafka", false, "enable kafka push")
	flag.StringVar(&KafkaAddr, "kafka.addr", "localhost:9092", "kafka broker address")
	//flag.Uint64Var(&KafkaTopicFilter, "kafka.topic.filter", uint64(BlockDataPackage),
	//	"use kafka.topic.filter to filter the messages pushing to kafka, "+
	//		"1 for block; 2 for online data; 4 for transaction")
}

func NewClient(urls ...string) *Client {

	urlReachableMap := make(map[string]int64, len(urls))
	for _, url := range urls {
		urlReachableMap[url] = 0
	}

	return &Client{
		urlList:         urls,
		urlReachableMap: urlReachableMap,
		source:          make(chan Typed, 1024),
	}
}

type Client struct {
	urlList         []string
	urlReachableMap map[string]int64
	source          chan Typed
}

func (c *Client) GetReachableUrlIndex() int {
	for idx, url := range c.urlList {
		if errorUnixTime, ok := c.urlReachableMap[url]; ok {
			if time.Now().Unix()-errorUnixTime > 60 {
				return idx
			}
		}
	}
	return -1
}

func (c *Client) SetUrlUnreachable(index int) {
	c.urlReachableMap[c.urlList[index]] = time.Now().Unix()
}

func (c *Client) Emit(m Typed) {
	c.source <- m
}

func (c *Client) Push(done chan struct{}) {

	postList := make([]Typed, 0, BatchSize)
	listChan := make(chan []Typed, 64)
	go c.push(listChan)

	pushInterval := time.NewTicker(time.Second)

	for {
		select {
		case <-done:
			return
		case m := <-c.source:
			// if enable pushing to streamcube
			postList = append(postList, m)
			if len(postList) >= BatchSize {
				newPost := make([]Typed, len(postList))
				copy(newPost, postList)
				listChan <- newPost
				postList = postList[:0]
			}

		case <-pushInterval.C:
			if len(postList) > 0 {
				newPost := make([]Typed, len(postList))
				copy(newPost, postList)
				listChan <- newPost
				postList = postList[:0]
			}
		}
	}
}

func (c *Client) push(listChan chan []Typed) {
	if len(c.urlList) == 0 {
		log.Fatalln("Streamcube push enabled while no urls set")
		return
	}

	count := 0
	pushUrlIndex := rand.Intn(len(c.urlList))
	log.Printf("I! using streamcube push url: %s", c.urlList[pushUrlIndex])

	logInterval := time.Minute
	logTicker := time.NewTicker(time.Minute)

	examle := false

	timeoutClient := http.Client{
		Timeout: time.Duration(5) * time.Second,
	}

	kafkaWriter := kafka.Writer{
		Addr:                   kafka.TCP(KafkaAddr),
		Async:                  true,
		AllowAutoTopicCreation: true,
	}
	defer kafkaWriter.Close()

	for {
		select {
		case list := <-listChan:
			if StreamcubeEnable {
				if pushUrlIndex == -1 {
					pushUrlIndex = c.GetReachableUrlIndex()
					continue
				}

				by, _ := json.Marshal(list)

				if resp, err := timeoutClient.Post(c.urlList[pushUrlIndex], "application/json", bytes.NewReader(by)); err != nil {
					c.SetUrlUnreachable(pushUrlIndex)
					if pushUrlIndex = c.GetReachableUrlIndex(); pushUrlIndex != -1 {
						log.Printf("[PUSHCLIENT] push error: %v, use new url %s instead", err, c.urlList[pushUrlIndex])
					} else {
						log.Printf("[PUSHCLIENT] push error: %v, no url reachable", err)
					}
				} else {
					count += len(list)

					if examle {
						log.Printf("[PUSHCLIENT] Push example %s", string(by))
						examle = false
					}

					_, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						log.Printf("[PUSHCLIENT] Read err: %v", err)
					}

					resp.Body.Close()
				}
			}

			// if enalbe pushing to kafka
			if KafkaEnable {
				kafkaMsgs := make([]kafka.Message, 0, len(list))
				for _, rawMsg := range list {
					by, err := json.Marshal(rawMsg)
					if err != nil {
						log.Printf("E! error marshal: %v", err)
						continue
					}

					kafkaMsgs = append(kafkaMsgs, kafka.Message{
						Topic: rawMsg.GetType().String(),
						Value: by,
					})
				}

				if err := kafkaWriter.WriteMessages(context.Background(), kafkaMsgs...); err != nil {
					log.Printf("Kafka write message err: %v", err)
				}
			}

		case <-logTicker.C:
			log.Printf("[PUSHCLIENT] Pushed avg %.2f", float64(count)/logInterval.Seconds())
			count = 0
			examle = true
		}
	}
}
