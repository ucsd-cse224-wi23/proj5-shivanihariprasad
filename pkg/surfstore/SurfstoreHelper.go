package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Println("Error During Meta Write Back 1", err)
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Println("Error During Meta Write Back 2", err)
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Println("Error During Meta Write Back 3", err)
	}
	statement.Exec()
	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Println("Error During Meta Write Bac 4", err)
	}
	//fmt.Println("file info in their code", fileMetas)
	for _, fileInfoOperation := range fileMetas {
		//log.Println("Struct", fileInfoOperation)
		for hashIndex, hashValue := range fileInfoOperation.BlockHashList {
			statement.Exec(fileInfoOperation.Filename, fileInfoOperation.Version, hashIndex, hashValue)
		}

	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `Select DISTINCT fileName from indexes`

const getTuplesByFileName string = `Select fileName, version, hashIndex, hashValue from indexes where fileName = ?`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Println("Error When Opening Meta")
	}
	ReadFromDatabase(&fileMetaMap, db)
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
func IterateLocalDir(baseDir string, client RPCClient) map[string]*FileInfoOperation {

	var FileInfoClientMap = make(map[string]*FileInfoOperation)
	readFiles(baseDir, &FileInfoClientMap, client)
	return FileInfoClientMap
}

func readFiles(baseDir string, FileInfoClientMap *map[string]*FileInfoOperation, client RPCClient) {
	err := filepath.Walk(baseDir, func(pathString string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			// Skip directories
			return nil
		}

		//log.Println(pathString)
		if !strings.Contains(pathString, "index.db") {
			fileName := path.Base(pathString)
			hashString, dataString := readBinaryDataFromFile(pathString, client)
			//log.Println(fileName, dataString)
			(*FileInfoClientMap)[fileName] = &FileInfoOperation{
				fileName:      fileName,
				version:       1,
				blockHashList: hashString,
				action:        1, //create
				dataList:      dataString,
			}

		}

		return nil
	})

	if err != nil {
		log.Println("Error walking directory:", err)
		return
	}
}

func readBinaryDataFromFile(path string, client RPCClient) ([]string, []Block) {
	file, err := os.Open(path)
	if err != nil {
		log.Println("Error opening file:", err)
		return nil, nil
	}
	defer file.Close()
	var dataString []string
	var dataBuffer []Block

	for {
		buffer := make([]byte, client.BlockSize)
		n, err := file.Read(buffer)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Println("Error reading file:", err)
			return nil, nil
		}
		var block Block
		block.BlockData = buffer[:n]
		block.BlockSize = int32(n)
		dataBuffer = append(dataBuffer, block)
		dataString = append(dataString, ComputeHash(buffer[:n]))
		// Process the buffer
		// Here we read a 4-byte integer from the buffer and print it

	}
	return dataString, dataBuffer

}

func ComputeHash(buf []byte) string {
	hashBytes := sha256.Sum256(buf[:])
	return hex.EncodeToString(hashBytes[:])
}

func ReadFromDatabase(fileMetaMap *map[string]*FileMetaData, db *sql.DB) {
	// create table in .db file
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Println(err.Error())
	}
	statement.Exec()
	// retrieve all cse courses tuples in ascending order by code.
	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Println("Error while retreiving filenames from db", err.Error())
	}
	var fileName string
	for rows.Next() {
		rows.Scan(&fileName)
		//log.Println("check filename", fileName)
		//Now get all the data for this specific filename
		rowsForFile, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Println("Error while getting records of filename", err.Error())
		}
		var hashValue string
		var hashIndex int
		var version int
		var hashList []string
		for rowsForFile.Next() {
			rowsForFile.Scan(&fileName, &version, &hashIndex, &hashValue)
			hashList = append(hashList, hashValue)
		}
		(*fileMetaMap)[fileName] = &FileMetaData{Filename: fileName, Version: int32(version), BlockHashList: hashList}
	}
}

func CheckIfArraysAreEqual(x []string, y []string) bool {
	return reflect.DeepEqual(x, y)
}

func CheckIfFileIsDeleted(hashblockList []string) bool {
	if hashblockList[0] == "0" {
		//tombstone case
		return true
	}

	if len(hashblockList) >= 1 {
		//This means file is not deleted
		return false
	}
	return false
}

func Hash_to_string(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}
