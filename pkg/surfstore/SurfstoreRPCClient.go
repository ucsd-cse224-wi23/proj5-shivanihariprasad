package surfstore

import (
	context "context"
	"fmt"
	"log"
	"strings"
	"time"

	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	//fmt.Println("check print in rpc", b.BlockSize)
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	log.Printf("Success : %v \n", success.GetFlag())
	*succ = success.Flag
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		val, err := c.GetFileInfoMap(ctx, &empty.Empty{})

		if err != nil {
			fmt.Println("Error getting fileinfomap", err)
			if strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			if strings.Contains(err.Error(), "Server is not the leader") {
				continue
			}
			if strings.Contains(err.Error(), "No majority from followers") {
				continue
			}
			log.Println("Error setting fileinfomap", err)
			continue

		}

		//*serverFileInfoMap = val.FileInfoMap
		for key, value := range (*val).FileInfoMap {
			(*serverFileInfoMap)[key] = value
		}

	}
	return nil

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			fmt.Println("Error setting UpdateFile", err)
			if strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			if strings.Contains(err.Error(), "Server is not the leader") {
				continue
			}
			if strings.Contains(err.Error(), "No majority from followers") {
				continue
			}
			fmt.Println("Error setting UpdateFile", err)
			continue
		}
		*latestVersion = b.Version
	}
	return nil
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &empty.Empty{})
	if err != nil {
		log.Println("Error getting block hashes", err)
		conn.Close()
		return err

	}
	*blockHashes = b.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			fmt.Println("Error setting GetBlockStoreMap", err)
			if strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			if strings.Contains(err.Error(), "Server is not the leader") {
				continue
			}
			if strings.Contains(err.Error(), "No majority from followers") {
				continue
			}
			log.Println("Error setting GetBlockStoreMap", err)
			continue
		}
		for key, value := range (*b).BlockStoreMap {
			(*blockStoreMap)[key] = (*value).Hashes
		}
	}
	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer conn.Close()
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		//var b *BlockStoreAddr
		b, err := c.GetBlockStoreAddrs(ctx, &empty.Empty{})
		//fmt.Println("check adddr", b.BlockStoreAddrs)

		if err != nil {
			fmt.Println("Error setting GetBlockStoreAddrs", err)
			if strings.Contains(err.Error(), "Server is crashed") {
				continue
			}
			if strings.Contains(err.Error(), "Server is not the leader") {
				continue
			}
			if strings.Contains(err.Error(), "No majority from followers") {
				continue
			}
			log.Println("Error setting GetBlockStoreAddrs", err)
			continue
		}

		*blockStoreAddrs = b.BlockStoreAddrs
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
