package cluster

import (
	"interface_hash_server/tools"

	"github.com/gomodule/redigo/redis"
)

func (masterClient RedisClient) ReplicateToSlave(command string, key string, value string) {

	tools.InfoLogger.Println("Replicate to Slave 시작")

	tools.InfoLogger.Println("Slave가 살아있는지 확인하기")

	slaveClient, err := masterClient.getSlave()
	if err != nil {
		return
	}

	// Error will be ignored as Slave can be dead
	redis.String(slaveClient.Connection.Do(command, key, value))

	slaveClient.RecordModificationLog(command, key, value)

	tools.InfoLogger.Println("Replicate to Slave 종료")
}
