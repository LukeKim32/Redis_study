package redisWrapper

import (
	"github.com/gomodule/redigo/redis"
	"interface_hash_server/internal/hash"
	"interface_hash_server/tools"
)

// RedisNodePorts are ports list of Redis Nodes
var RedisNodeAddress = []string{
	"172.29.0.4:8000", /* Redis Node (Container name : redis_one) */
	"172.29.0.5:8001", /* Redis Node (Container name : redis_two) */
	"172.29.0.6:8002", /* Redis Node (Container name : redis_three) */
}

var NodeConnections []redis.Conn

// HashSlotMap is a map of (Hash Slot -> Redis Node Client)
var HashSlotMap map[uint16]RedisClient

const (
	// ConnectTimeoutDuration unit is nanoseconds
	ConnectTimeoutDuration = 5000000000
)

func NodeConnectionSetup() error {
	var err error

	NodeCount := len(RedisNodeAddress)
	NodeConnections := make([]redis.Conn, NodeCount)
	HashSlotMap = make(map[uint16]RedisClient)

	for i, eachNodeAddress := range RedisNodeAddress {
		NodeConnections[i], err = redis.Dial("tcp", eachNodeAddress, redis.DialConnectTimeout(ConnectTimeoutDuration))
		if err != nil {
			tools.ErrorLogger.Printf("Redis Node(%s) connection failure - %s\n", eachNodeAddress, err.Error())
			return err
		}

		tools.InfoLogger.Printf("Redis Node(%s) connection Success\n", eachNodeAddress)

		// arithmatic order fixed to prevent Mantissa Loss
		hashSlotStart := uint16(float64(i) / float64(NodeCount) * float64(hash.HashSlotsNumber))
		hashSlotEnd := uint16(float64(i+1) / float64(NodeCount) * float64(hash.HashSlotsNumber))
		for j := hashSlotStart; j < hashSlotEnd; j++ {
			HashSlotMap[j] = RedisClient{
				Connection: NodeConnections[i],
				Address:    eachNodeAddress,
			}
		}

		tools.InfoLogger.Printf("Node %s hash slot range %d ~ %d\n", eachNodeAddress, hashSlotStart, hashSlotEnd)
	}

	return nil
}

func GetRedisClient(hashSlotIndex uint16) (RedisClient, error) {
	redisClient := HashSlotMap[hashSlotIndex]

	tools.InfoLogger.Printf("GetRedisClient() : Redis Node(%s) selected\n", redisClient.Address)

	_, err := redis.String(redisClient.Connection.Do("PING"))
	if err != nil {
		tools.ErrorLogger.Printf("Redis Node(%s) Ping test failure\n", redisClient.Address)
		return RedisClient{}, err
	}

	tools.InfoLogger.Printf("Redis Node(%s) Ping test success\n", redisClient.Address)

	return redisClient, nil
}
