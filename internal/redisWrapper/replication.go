package redisWrapper

import (
	"github.com/gomodule/redigo/redis"
)

func ReplicateToSlave(masterNode RedisClient, command string, key string, value string) {
	slaveNode, err := getSlave(masterNode)
	if err != nil {
		return
	}

	// Error will be ignored as Slave can be dead
	redis.String(slaveNode.Connection.Do(command, key, value))

	RecordModification(slaveNode.Address, command, key, value)
}
