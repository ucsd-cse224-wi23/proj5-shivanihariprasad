package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//fmt.Println("check map", bs.BlockMap)
	if _, ok := (bs.BlockMap)[blockHash.Hash]; ok {
		//fmt.Println("Entering the get section",bs.BlockMap[blockHash.Hash].BlockSize)
		return bs.BlockMap[blockHash.Hash], nil
	}
	return nil, fmt.Errorf("block not found")
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	//fmt.Println("Check data", block.BlockData)
	hashVal := GetBlockHashString(block.BlockData)
	bs.BlockMap[hashVal] = block
	//fmt.Println("check hash val", hashVal)
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashList []string
	for _, h := range blockHashesIn.Hashes {
		_, ok := (bs.BlockMap)[h]
		if ok {
			hashList = append(hashList, h)
		}
	}
	return &BlockHashes{Hashes: hashList}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	var hashes = make([]string, 0)
	//fmt.Println("Entered this function", bs.BlockMap)
	for h, _ := range bs.BlockMap {
		hashes = append(hashes, h)
	}
	return &BlockHashes{Hashes: hashes}, nil

}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
