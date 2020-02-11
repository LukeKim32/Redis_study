package redisWrapper

import (
	"github.com/gomodule/redigo/redis"
)

func ReplicateToSlave(masterNode RedisClient, command string, key string, value string) {
	slaveNode, err := getSlave(masterNode)
	if err != nil {
		return
	}

	redis.String(slaveNode.Connection.Do(command, key, value))
}
