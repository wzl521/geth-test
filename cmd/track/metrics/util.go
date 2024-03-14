package metrics

import (
	"fmt"
	"strings"
)

// ParseName parses a name into a metric and a field.
// Geth/v1.10.13-stable-7232f905/linux-amd64/go1.17.3
func ParseName(name string) (client, version, arch string, err error) {
	parts := strings.Split(name, "/")

	switch len(parts) {
	case 4:
		client = parts[0]
		version = parts[1]
		arch = parts[2]
	case 5:
		client = parts[0]
		version = parts[2]
		arch = parts[3]
	default:
		err = fmt.Errorf("invalid name: %s", name)

	}

	return
}
