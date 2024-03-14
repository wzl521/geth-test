package push

import (
	"encoding/json"
	"testing"
)

func TestMarshal(t *testing.T) {
	OnlinePost := make([]interface{}, 0)

	OnlinePost = append(OnlinePost, NewOnlineDataWrap(OnlineData{
		PeerID:  "1000",
		Country: "CN",
		Crawler: "Crawler_1",
		OnLen:   0,
		Counter: "CON",
		OffTime: 1652317200000,
		ConIP:   "1.1.1.1",
	}))
	OnlinePost = append(OnlinePost, NewOnlineDataWrap(OnlineData{
		PeerID:  "1000",
		Country: "CN",
		Crawler: "Crawler_1",
		OnLen:   0,
		Counter: "CON",
		OffTime: 1652319600000,
		ConIP:   "1.1.1.1",
	}))
	OnlinePost = append(OnlinePost, NewOnlineDataWrap(OnlineData{
		PeerID:  "2000",
		Country: "CN",
		Crawler: "Crawler_1",
		OnLen:   0,
		Counter: "CON",
		OffTime: 1652319600000,
		ConIP:   "2.2.2.2",
	}))

	by, _ := json.Marshal(OnlinePost)
	t.Logf("txPost: %s", string(by))
}
