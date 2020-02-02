package redisWrapper

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"interface_hash_server/internal/hash"
	"interface_hash_server/tools"
)


// HashSlotMap is a map of (Hash Slot -> Redis Node Client)
var HashSlotMap map[uint16]RedisClient

var clientHashRangeMap map[string]HashRange

type HashRange struct {
	startIndex uint16
	endIndex uint16
}

const (
	// ConnectTimeoutDuration unit is nanoseconds
	ConnectTimeoutDuration = 5000000000
)

type ConnectOption string

const (
	Default ConnectOption = "Default"
	SlaveSetup ConnectOption = "SlaveSetup"
)

// masterSlaveMap is a map of (Master Address -> Slave Address)
var masterSlaveMap map[string]string

func NodeConnectionSetup(addressList []string, connectOption ConnectOption) error {

	var err error

	for i, eachNodeAddress := range addressList {
		var newRedisClient RedisClient

		newRedisClient.Connection, err = redis.Dial("tcp", eachNodeAddress, redis.DialConnectTimeout(ConnectTimeoutDuration))
		if err != nil {
			tools.ErrorLogger.Printf("Redis Node(%s) connection failure - %s\n", eachNodeAddress, err.Error())
			return err
		}
		newRedisClient.Address = eachNodeAddress

		switch connectOption {
		case Default :
			redisMasterClients = append(redisMasterClients, newRedisClient)

		case SlaveSetup :
			if len(redisMasterClients) == 0 {
				fmt.Errorf("Redis Master Node should be set up first")
			}

			if masterSlaveMap == nil {
				masterSlaveMap = make( map[string]string)
			}
			// 동일한 Index의 Master Node와 Map
			masterSlaveMap[RedisMasterAddressList[i]] = eachNodeAddress
			redisSlaveClients = append(redisSlaveClients, newRedisClient)

			tools.InfoLogger.Printf("Redis Slave Node(%s) Mapped from Master Node(%s) Success\n", eachNodeAddress, RedisMasterAddressList[i])
		}

		tools.InfoLogger.Printf("Redis Node(%s) connection Success\n", eachNodeAddress)
	}

	return nil
}

func MakeRedisAddressHashMap() error {

	connectionCount := len(redisMasterClients)
	if connectionCount == 0 {
		return fmt.Errorf("Redis Connections should be setup first")
	}

	HashSlotMap = make(map[uint16]RedisClient)
	clientHashRangeMap = make( map[string]HashRange)
	
	for i, eachRedisClient := range redisMasterClients {
		// arithmatic order fixed to prevent Mantissa Loss
		hashSlotStart := uint16(float64(i) / float64(connectionCount) * float64(hash.HashSlotsNumber))
		hashSlotEnd := uint16(float64(i+1) / float64(connectionCount) * float64(hash.HashSlotsNumber))
		var nextSlotIndex uint16

		for j := hashSlotStart; j < hashSlotEnd; j+=16 {
			HashSlotMap[j] = eachRedisClient
			HashSlotMap[j+1] = eachRedisClient
			HashSlotMap[j+2] = eachRedisClient
			HashSlotMap[j+3] = eachRedisClient
			HashSlotMap[j+4] = eachRedisClient
			HashSlotMap[j+5] = eachRedisClient
			HashSlotMap[j+6] = eachRedisClient
			HashSlotMap[j+7] = eachRedisClient
			HashSlotMap[j+8] = eachRedisClient
			HashSlotMap[j+9] = eachRedisClient
			HashSlotMap[j+10] = eachRedisClient
			HashSlotMap[j+11] = eachRedisClient
			HashSlotMap[j+12] = eachRedisClient
			HashSlotMap[j+13] = eachRedisClient
			HashSlotMap[j+14] = eachRedisClient
			HashSlotMap[j+15] = eachRedisClient
			nextSlotIndex = j+16
		}

		for ; nextSlotIndex < hashSlotEnd ; nextSlotIndex++ {
			HashSlotMap[nextSlotIndex] = eachRedisClient
		}

		clientHashRangeMap[eachRedisClient.Address] = HashRange { 
			startIndex : hashSlotStart, 
			endIndex : hashSlotEnd,
		} 

		tools.InfoLogger.Printf("Node %s hash slot range %d ~ %d\n", eachRedisClient.Address, hashSlotStart, hashSlotEnd)
	}

	return nil
}

