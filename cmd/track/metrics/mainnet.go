package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	gethClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_geth_count",
		Help: "The total number of geth clients recorded",
	})
	erigonClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_erigon_count",
		Help: "The total number of erigon clients recorded",
	})
	openEthClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_open_eth_count",
		Help: "The total number of openEthereum clients recorded",
	})
	nethermindClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_nethermind_count",
		Help: "The total number of nethermind clients recorded",
	})
	besuClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_besu_count",
		Help: "The total number of besu clients recorded",
	})
	coreGethClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_coreGeth_count",
		Help: "The total number of coreGeth clients recorded",
	})
	otherClientCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_other_count",
		Help: "The total number of other clients recorded",
	})

	last24hrsSeenPeerCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "last24hrs_peer_count",
		Help: "peer count from 24hrs ago to now",
	})
)

func IncClientCount(client string) {
	switch client {
	case "Geth":
		gethClientCount.Inc()
	case "erigon":
		erigonClientCount.Inc()
	case "OpenEthereum":
		openEthClientCount.Inc()
	case "Nethermind":
		nethermindClientCount.Inc()
	case "besu":
		besuClientCount.Inc()
	case "CoreGeth":
		coreGethClientCount.Inc()
	default:
		otherClientCount.Inc()
	}
}

func SetLast24hrsPeerCount(count int64) {
	last24hrsSeenPeerCount.Set(float64(count))
}

// 2022/4/22 total: 10772
// besu                    count: 100	0.93%
// Geth                    count: 8397	77.95%
// Nethermind              count: 181	1.68%
// OpenEthereum            count: 662	6.15%
// teth                    count: 27	0.25%
// ethereumjs-devp2p       count: 40	0.37%
// erigon                  count: 1096	10.17%
// merp-client             count: 4		0.04%
// CoreGeth                count: 10	0.09%
// bor                     count: 3		0.03%
// ethereum-scraper        count: 75	0.70%
// unknown                 count: 177	1.64%
