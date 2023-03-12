package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	// 2. find the first server with larger hash value than blockHash
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	//fmt.Println("Check server maps in hashing", c.ServerMap)
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}
	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	serverMap := make(map[string]string)

	for _, serverName := range serverAddrs {
		serverHash := Hash_to_string("blockstore" + serverName)
		serverMap[serverHash] = "blockstore" + serverName
	}
	//fmt.Println("Check server maps in new hasing",serverMap)
	return &ConsistentHashRing{ServerMap: serverMap}
}
