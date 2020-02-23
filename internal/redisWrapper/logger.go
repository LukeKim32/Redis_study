package redisWrapper

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
	logDirectory = "./internal/redisWrapper/dump"

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

	dataLoggers = make(map[string]*log.Logger, len(nodeAddressList))

	for _, eachNodeAddress := range nodeAddressList {
		// 각 노드의 주소 = 각 파일명
		filePath := fmt.Sprintf("%s/%s", logDirectory, eachNodeAddress)
		fpLog, err := os.OpenFile(filePath,
			os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}

		dataLoggers[eachNodeAddress] = log.New(fpLog, "", 0)
	}

}

func RecordModificationLog(address string, command string, key string, value string) error {

	tools.InfoLogger.Printf("%s 노드에 데이터 수정사항 로그 저장", address)

	targetDataLogger, isSet := dataLoggers[address]
	if isSet == false {
		return fmt.Errorf("RecordModificationLog() : data Logger is not set up")
	}

	hashSlotIndex := hash.GetHashSlotIndex(key)
	targetDataLogger.Printf("%d %s %s %s", hashSlotIndex, command, key, value)

	return nil
}

// readDataLogs reads Node's data log file and records the information in @hashIndexToKeyValuePairMap
func readDataLogs(address string, hashIndexToLogFormatMap map[uint16][]logFormat) error {
	filePath := fmt.Sprintf("%s/%s", logDirectory, address)
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
