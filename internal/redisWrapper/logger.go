package redisWrapper

import (
	"fmt"
	"interface_hash_server/internal/hash"
	"log"
	"os"
)

// dataLoggers gets a logger by passed-key of Each Node address
var dataLoggers map[string] /* key = each Node's address*/ *log.Logger

const (
	//LogDirectory is a directory path where log files are saved
	logDirectory = "./internal/redisWrapper/dump"
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

func RecordModification(address string, command string, key string, value string) error {

	targetDataLogger, isSet := dataLoggers[address]
	if isSet != false {
		return fmt.Errorf("RecordModification() : data Logger is not set up")
	}

	hashSlotIndex := hash.GetHashSlotIndex(key)
	targetDataLogger.Printf("%d %s %s %s", hashSlotIndex, command, key, value)

	return nil
}
