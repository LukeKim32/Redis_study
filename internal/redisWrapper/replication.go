package redisWrapper

import (
	"interface_hash_server/tools"

	"github.com/gomodule/redigo/redis"
)

func ReplicateToSlave(masterNode RedisClient, command string, key string, value string) {

	tools.InfoLogger.Println("Replicate to Slave 시작")

	tools.InfoLogger.Println("Slave가 살아있는지 확인하기")

	slaveNode, err := getSlave(masterNode)
	if err != nil {
		return
	}

	// Error will be ignored as Slave can be dead
	redis.String(slaveNode.Connection.Do(command, key, value))

	RecordModificationLog(slaveNode.Address, command, key, value)

	tools.InfoLogger.Println("Replicate to Slave 종료")
}
