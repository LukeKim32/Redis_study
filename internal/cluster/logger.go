package cluster

import (
	"bufio"
	"fmt"
	"interface_hash_server/internal/hash"
	"interface_hash_server/internal/models"
	"interface_hash_server/tools"
	"log"
	"os"
	"strconv"
	"strings"
)

// dataLoggers gets a logger by passed-key of Each Node address
var dataLoggers map[string] /* key = each Node's address*/ *log.Logger

type logFormat struct {
	models.KeyValuePair
	Command string
}

const (
	//LogDirectory is a directory path where log files are saved
	logDirectory = "./internal/cluster/dump"

	/* constants for "index" of Data Log each lines */
	hashIndexWord = 0
	commandWord   = 1
	keyWord       = 2
	valueWord     = 3
)

// SetUpModificationLogger 는 Data Modification이 일어날 때 파일에 기록을 하기 위한 로거 세터
/*	For Data persistency support
 */
func SetUpModificationLogger(nodeAddressList []string) {

	if _, err := os.Stat(logDirectory); os.IsNotExist(err) {
		// rwxrwxrwx (777)
		os.Mkdir(logDirectory, os.ModePerm)
	}

	dataLoggers = make(map[string]*log.Logger)

	for _, eachNodeAddress := range nodeAddressList {
		// 각 노드의 주소 = 각 파일명
		createDataLogFile(eachNodeAddress)
	}

}

func createDataLogFile(address string) error {
	filePath := fmt.Sprintf("%s/%s", logDirectory, address)

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fpLog, err := os.OpenFile(filePath,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		dataLoggers[address] = log.New(fpLog, "", 0)
	}

	return nil
}

func (redisClient RedisClient) RecordModificationLog(command string, key string, value string) error {

	tools.InfoLogger.Printf("%s 노드에 데이터 수정사항 로그 저장", redisClient.Address)

	targetDataLogger, isSet := dataLoggers[redisClient.Address]
	if isSet == false {
		return fmt.Errorf("RecordModificationLog() : data Logger is not set up")
	}

	hashSlotIndex := hash.GetHashSlotIndex(key)
	targetDataLogger.Printf("%d %s %s %s", hashSlotIndex, command, key, value)

	return nil
}

// readDataLogs reads Node's data log file and records the information in @hashIndexToKeyValuePairMap
func (redisClient RedisClient) getLatestDataFromLog(hashIndexToKeyValueMap map[uint16](map[string]string)) error {
	filePath := fmt.Sprintf("%s/%s", logDirectory, redisClient.Address)
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("readDataLogs() : opening file %s error - %s", filePath, err.Error())
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		hashIndexIn64, err := strconv.ParseUint(words[hashIndexWord], 10, 16)
		if err != nil {
			return fmt.Errorf("readDataLogs() : Parsing hash index error")
		}

		hashIndex := uint16(hashIndexIn64)

		tools.InfoLogger.Printf("readDataLogs() : data log file read result : %d %s %s %s\n",
			hashIndex, words[commandWord], words[keyWord], words[valueWord])

		if hashIndexToKeyValueMap[hashIndex] == nil {
			hashIndexToKeyValueMap[hashIndex] = make(map[string]string)
		}

		keyValueMap := hashIndexToKeyValueMap[hashIndex]

		// Record Logs to Map (Latest Modification will overwrite previous data)
		switch words[commandWord] {
		case "SET":
			keyValueMap[words[keyWord]] = words[valueWord]
			break
		case "DEL":
			delete(keyValueMap, words[keyWord])
			break
		default:
			return fmt.Errorf("readDataLogs() : Unavailable Command(%s) read from log file", words[commandWord])
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("readDataLogs() : scanner error - %s", err.Error())
	}

	return nil
}

// readDataLogs reads Node's data log file and records the information in @hashIndexToKeyValuePairMap
func (redisClient RedisClient) readDataLogs(hashIndexToLogFormatMap map[uint16][]logFormat) error {
	filePath := fmt.Sprintf("%s/%s", logDirectory, redisClient.Address)
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("readDataLogs() : opening file %s error - %s", filePath, err.Error())
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		hashIndexIn64, err := strconv.ParseUint(words[hashIndexWord], 10, 16)
		if err != nil {
			return fmt.Errorf("readDataLogs() : Parsing hash index error")
		}

		hashIndex := uint16(hashIndexIn64)

		var logFormat logFormat
		logFormat.Key = words[keyWord]
		logFormat.Value = words[valueWord]
		logFormat.Command = words[commandWord]

		tools.InfoLogger.Printf("readDataLogs() : data log file read result : %d %s %s %s\n",
			hashIndex, words[commandWord], words[keyWord], words[valueWord])

		hashIndexToLogFormatMap[hashIndex] = append(hashIndexToLogFormatMap[hashIndex], logFormat)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("readDataLogs() : scanner error - %s", err.Error())
	}

	return nil
}

func (redisClient RedisClient) removeDataLogs() error {
	filePath := fmt.Sprintf("%s/%s", logDirectory, redisClient.Address)

	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("removeDataLogs() : removing file %s error - %s", filePath, err.Error())
	}

	delete(dataLoggers, redisClient.Address)

	return nil
}
