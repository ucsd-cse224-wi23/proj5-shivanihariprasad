package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	_, ok := (m.FileMetaMap)[fileMetaData.Filename]
	if ok {
		if m.FileMetaMap[fileMetaData.Filename].Version >= fileMetaData.Version {
			return &Version{Version: m.FileMetaMap[fileMetaData.Filename].Version}, fmt.Errorf("Old Version Update Action")
		}
	}

	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	return &Version{
		Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)
	//blockStoreAddrs := m.BlockStoreAddrs

	for _, hash := range blockHashesIn.Hashes {
		addr := m.ConsistentHashRing.GetResponsibleServer(hash)
		_, ok := blockStoreMap[addr]
		if !ok {
			blockStoreMap[addr] = &BlockHashes{}
		}
		if(blockStoreMap[addr].Hashes == nil){
			blockStoreMap[addr].Hashes = make([]string,0)
		}
		blockStoreMap[addr].Hashes = append(blockStoreMap[addr].Hashes, hash)

	}

	//fmt.Println("hashes", blockHashesIn.Hashes)
	//fmt.Println("blockHashMap", blockStoreMap)
	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil

}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
