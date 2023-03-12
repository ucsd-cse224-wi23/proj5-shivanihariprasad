package surfstore

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type FileInfoOperation struct {
	fileName      string
	version       int32 // 1 for new file
	blockHashList []string
	action        int // 0: no change, 1: create, 2: update, 3: delete
	dataList      []Block
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	fmt.Println("Starting sync...")
	blockStoreAddrs := []string{}
	client.GetBlockStoreAddrs(&blockStoreAddrs)
	//Now get consistent hash ring
	serverFileInfoMap := make(map[string]*FileMetaData)
	//dbFileInfoMap := make(map[string]*FileMetaData)
	//clientFileInfoMap := make(map[string]*FileInfoOperation)
	err := client.GetFileInfoMap(&serverFileInfoMap)
	fmt.Println("check server file info map",serverFileInfoMap)
	if err != nil {
		log.Println("Error in client sync", err)
	}
	log.Println("Server info map", serverFileInfoMap)
	baseDir, err := filepath.Abs(client.BaseDir)
	if err != nil {
		log.Println("Error finding absolute path of base dir", baseDir)
	}
	if _, err := os.Stat(baseDir); os.IsNotExist(err) {
		err := os.Mkdir(baseDir, 0755)
		if err != nil {
			log.Println("Error creating directory:", err)
			return
		}
		log.Println("Directory created:", baseDir)
	} else {
		log.Println("Directory already exists:", baseDir)
	}

	clientFileInfoMap := IterateLocalDir(baseDir, client)
	//Get data from index.db
	dbFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Println("Error in loading meta data from db ", err)
	}
	//Sync client version from that of DB file
	UpdateClientFromDBFile(&clientFileInfoMap, &dbFileInfoMap)

	//Compare for every server object, what is available client data and update accordingly
	CompareServerWithClient(client, &serverFileInfoMap, &clientFileInfoMap, blockStoreAddrs)

	//Compare for every client object, what is available server data and update accordingly
	CompareClientWithServer(&clientFileInfoMap, &serverFileInfoMap, client, blockStoreAddrs)

	clientFileInfo := make(map[string]*FileMetaData)
	for k, v := range clientFileInfoMap {
		clientFileInfo[k] = &FileMetaData{Filename: k, Version: v.version, BlockHashList: v.blockHashList}
	}
	//Sync client info with database
	WriteMetaFile(clientFileInfo, baseDir)

}

func UpdateClientFromDBFile(clientFileInfoMap *map[string]*FileInfoOperation, dbFileInfoMap *map[string]*FileMetaData) {
	for fileName, metaData := range *dbFileInfoMap {
		_, ok := (*clientFileInfoMap)[fileName]
		if ok {
			if !CheckIfArraysAreEqual((*clientFileInfoMap)[fileName].blockHashList, metaData.BlockHashList) {
				(*clientFileInfoMap)[fileName].action = 2 // update
				(*clientFileInfoMap)[fileName].version = metaData.Version + 1
			} else {
				(*clientFileInfoMap)[fileName].action = 0 // no change

			}
		} else {
			//File was deleted in local index file also
			if CheckIfFileIsDeleted(metaData.BlockHashList) {
				(*clientFileInfoMap)[fileName] = &FileInfoOperation{
					fileName:      fileName,
					version:       metaData.Version,
					action:        0, // no change
					blockHashList: []string{"0"},
					dataList:      []Block{},
				}
			} else { // file deleted now by client, so we need to update DS
				(*clientFileInfoMap)[fileName] = &FileInfoOperation{
					fileName:      fileName,
					version:       metaData.Version + 1,
					action:        3, // delete
					blockHashList: []string{"0"},
					dataList:      []Block{},
				}
			}
		}
	}
}

func CompareServerWithClient(client RPCClient, serverFileInfoMap *map[string]*FileMetaData, clientFileInfoMap *map[string]*FileInfoOperation, blockStoreAddrs []string) {
	for k, v := range *serverFileInfoMap {
		_, ok := (*clientFileInfoMap)[k]
		if !ok {
			FetchFileFromServer(k, v, blockStoreAddrs, clientFileInfoMap, client)
		} else {
			//Now check client version and server version
			CheckAndHandleUpdateOfFile(k, client, v, clientFileInfoMap, blockStoreAddrs)
		}
	}
}

func FetchFileFromServer(k string, v *FileMetaData, blockStoreAddrs []string, clientFileInfoMap *map[string]*FileInfoOperation, client RPCClient) {
	//Check the hash list first and see if it is deleted file
	if CheckIfFileIsDeleted(v.BlockHashList) {
		(*clientFileInfoMap)[k] = &FileInfoOperation{
			fileName:      k,
			blockHashList: v.BlockHashList,
			action:        0, //No change
			version:       v.Version,
			dataList:      []Block{},
		}
		//Now delete file from local
		os.Remove(filepath.Clean(client.BaseDir + "/" + k))
		return
	}
	//var dataBuffer []Block
	//It means server has a file that client does not
	file, err := os.Create(filepath.Clean(client.BaseDir + "/" + k))
	if err != nil {
		log.Println("error while creating file", err)
	}
	defer file.Close()
	var blockStoreMap = make(map[string][]string)
	client.GetBlockStoreMap(v.BlockHashList, &blockStoreMap)
	var blockMap = make(map[string]Block)
	var blockList []Block
	//fmt.Print("Entered FetchFileFromServer")
	for blockStoreAddr, hashList := range blockStoreMap {
		for _, hash := range hashList {
			var block Block
			//need to get responsible server
			err := client.GetBlock(hash, blockStoreAddr[len("blockstore"):], &block)
			//dataBuffer = append(dataBuffer, block)
			if err != nil {
				log.Println("Error while getting blocks from server", err)
			}
			blockMap[hash] = block
		}
	}
	for _, hash := range v.BlockHashList {
		blockList = append(blockList, blockMap[hash])
	}

	for _, block := range blockList {
		_, err = file.Write(block.BlockData)
		if err != nil {
			log.Println("Error while writing blocks to client", err)
		}
	}

	//Now update clientInfoMap with this data, this could be useful to write to index.db
	(*clientFileInfoMap)[k] = &FileInfoOperation{fileName: k, version: v.Version, action: 0, blockHashList: v.BlockHashList, dataList: blockList}
}

func CheckAndHandleUpdateOfFile(fileName string, client RPCClient, serverFileInfo *FileMetaData, clientFileInfoMap *map[string]*FileInfoOperation, blockStoreAddrs []string) {
	//CASE 1: When server has a higher version compared to client and it not changed
	if (*clientFileInfoMap)[fileName].action == 0 {
		if (*serverFileInfo).Version > (*clientFileInfoMap)[fileName].version {
			//Delete local file and update it
			FetchFileFromServer(fileName, serverFileInfo, blockStoreAddrs, clientFileInfoMap, client)
			return
		}
	}
	//Case 2: When file was created by client, but server has a file with same name, sync it
	if (*clientFileInfoMap)[fileName].action == 1 {
		//get new serverinfo file
		newServerFileInfoMap := make(map[string]*FileMetaData)
		err := client.GetFileInfoMap(&newServerFileInfoMap)
		if err != nil {
			log.Println("Error in retrieving server obj ", err)
		}
		FetchFileFromServer(fileName, newServerFileInfoMap[fileName], blockStoreAddrs, clientFileInfoMap, client)
		return
	}

	//case 3: If client has updated file and server has old version, try to update
	if (*clientFileInfoMap)[fileName].action == 3 || (*clientFileInfoMap)[fileName].action == 2 {
		if (*clientFileInfoMap)[fileName].version > (*serverFileInfo).Version {
			err := UpdateFileAndWriteAllBlocks((*clientFileInfoMap)[fileName], client, blockStoreAddrs)
			if err == nil {
				(*clientFileInfoMap)[fileName].action = 0
				return
			}
			//Err occurred while updating server, download data then
		}
		//get new serverinfo file
		newServerFileInfoMap := make(map[string]*FileMetaData)
		err := client.GetFileInfoMap(&newServerFileInfoMap)
		if err != nil {
			log.Println("Error in retrieving server obj ", err)
		}
		FetchFileFromServer(fileName, newServerFileInfoMap[fileName], blockStoreAddrs, clientFileInfoMap, client)
		return
	}

}
func UpdateFileAndWriteAllBlocks(v *FileInfoOperation, client RPCClient, blockStoreAddrs []string) error {
	var version int32
	err := client.UpdateFile(&FileMetaData{Filename: v.fileName, Version: v.version, BlockHashList: v.blockHashList}, &version)
	if err != nil {
		log.Println("Error while putting metadata to server", err, version)
		return err

	}
	var blockMap = make(map[string]Block)
	for _, val := range v.dataList {
		blockHash := Hash_to_string(string(val.BlockData))
		blockMap[blockHash] = val
	}
	var blockStoreMap = make(map[string][]string)
	//fmt.Print("Entered UpdateFileAndWriteAllBlocks")
	client.GetBlockStoreMap(v.blockHashList, &blockStoreMap)
	//fmt.Println("check here", blockStoreMap)
	for blockStoreAddr, hashList := range blockStoreMap {
		for _, hash := range hashList {
			var succ bool
			var block Block = blockMap[hash]
			//fmt.Println("check data", hash, len(block.BlockData))
			err := client.PutBlock(&block, blockStoreAddr[len("blockstore"):], &succ)

			if err != nil {
				fmt.Println("Error while putting blocks to server", err)
				return err
			}
		}
	}
	return nil
}

func CompareClientWithServer(clientFileInfoMap *map[string]*FileInfoOperation, serverFileInfoMap *map[string]*FileMetaData, client RPCClient, blockStoreAddrs []string) {
	//Push local changes to server if it is not there
	for k, v := range *clientFileInfoMap {
		//fmt.Println(k, v.fileName, v.version, len(v.blockHashList), len(v.dataList))
		_, ok := (*serverFileInfoMap)[k]
		if !ok { // NOW JUST CHECKING IF FILE IS NOT THERE IN SERVER, PUT IN SERVER
			err := UpdateFileAndWriteAllBlocks(v, client, blockStoreAddrs)
			if err == nil {
				(*clientFileInfoMap)[k].action = 3
			} else {
				// handle update
				newServerFileInfoMap := make(map[string]*FileMetaData)
				err := client.GetFileInfoMap(&newServerFileInfoMap)
				if err != nil {
					log.Println("Error in retrieving server obj ", err)
				}
				CheckAndHandleUpdateOfFile(k, client, newServerFileInfoMap[k], clientFileInfoMap, blockStoreAddrs)
			}
		}
	}
}
