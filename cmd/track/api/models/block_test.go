package models

import (
	"encoding/json"
	"testing"
)

func TestMarshalHash(t *testing.T) {
	block := Block{
		Hash:        "0x1234567890123456789012345678901234567890123456789012345678901234",
		Number:      123456789,
		Propagators: nil,
		Headers: []Header{
			{
				ParentHash: Hash{
					250,
					244,
					38,
					254,
					31,
					60,
					179,
					216,
					237,
					58,
					199,
					168,
					233,
					47,
					0,
					166,
					74,
					137,
					54,
					225,
					144,
					165,
					171,
					166,
					216,
					45,
					134,
					38,
					253,
					221,
					168,
					93,
				},
			},
		},
	}

	if by, err := json.Marshal(block); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("by: %s", string(by))
	}

}
