package redisWrapper

import (
	"fmt"
	"interface_hash_server/internal/hash"
	"interface_hash_server/tools"
	"sync"

	"github.com/gomodule/redigo/redis"
)

// HashSlotMap is a map of (Hash Slot -> Redis Node Client)
var HashSlotMap map[uint16]RedisClient

const (
	// ConnectTimeoutDuration unit is nanoseconds
	ConnectTimeoutDuration = 5000000000
)

type ConnectOption string

const (
	Default    ConnectOption = "Default"
	SlaveSetup ConnectOption = "SlaveSetup"
)

func NodeConnectionSetup(addressList []string, connectOption ConnectOption) error {

	var err error

	for i, eachNodeAddress := range addressList {
		var newRedisClient RedisClient

		newRedisClient.Connection, err = redis.Dial("tcp", eachNodeAddress, redis.DialConnectTimeout(ConnectTimeoutDuration))
		if err != nil {
			tools.ErrorLogger.Printf(ConnectionFailure, eachNodeAddress, err.Error())
			return err
		}
		newRedisClient.Address = eachNodeAddress

		switch connectOption {
		case Default:
			newRedisClient.Role = MasterRole
			redisMasterClients = append(redisMasterClients, newRedisClient)

		case SlaveSetup:
			if len(redisMasterClients) == 0 {
				fmt.Errorf(RedisMasterNotSetUpYet)
			}
			if len(redisMasterClients) > len(addressList) {
				fmt.Errorf(SlaveNumberMustBeLarger)
			}

			// Modula index for circular assignment
			index := i % len(redisMasterClients)
			targetMasterClient := redisMasterClients[index]

			newRedisClient.Role = SlaveRole
			redisSlaveClients = append(redisSlaveClients, newRedisClient)

			initMasterSlaveMaps(targetMasterClient, newRedisClient)

			tools.InfoLogger.Printf(SlaveMappedToMaster, newRedisClient.Address, targetMasterClient.Address)
		}

		tools.InfoLogger.Printf(NodeConnectSuccess, eachNodeAddress)
	}

	return nil
}

func MakeHashMapToRedis() error {

	connectionCount := len(redisMasterClients)
	if connectionCount == 0 {
		return fmt.Errorf(RedisMasterNotSetUpYet)
	}

	HashSlotMap = make(map[uint16]RedisClient)
	clientHashRangeMap = make(map[string][]HashRange)

	for i, eachRedisNode := range redisMasterClients {
		// arithmatic order fixed to prevent Mantissa Loss
		hashSlotStart := uint16(float64(i) / float64(connectionCount) * float64(hash.HashSlotsNumber))
		hashSlotEnd := uint16(float64(i+1) / float64(connectionCount) * float64(hash.HashSlotsNumber))

		assignHashSlotMap(hashSlotStart, hashSlotEnd, eachRedisNode)

		newHashRange := HashRange{
			startIndex: hashSlotStart,
			endIndex:   hashSlotEnd,
		}
		clientHashRangeMap[eachRedisNode.Address] = append(clientHashRangeMap[eachRedisNode.Address], newHashRange)

		tools.InfoLogger.Printf(HashSlotAssignResult, eachRedisNode.Address, hashSlotStart, hashSlotEnd)
	}

	return nil
}

func initMasterSlaveMaps(masterNode RedisClient, slaveNode RedisClient) {
	if masterSlaveMap == nil {
		masterSlaveMap = make(map[string]RedisClient)
	}
	if slaveMasterMap == nil {
		slaveMasterMap = make(map[string]RedisClient)
	}
	if MasterSlaveChannelMap == nil {
		MasterSlaveChannelMap = make(map[string](chan MasterSlaveMessage))
	}
	if redisMutexMap == nil {
		redisMutexMap = make(map[string]*sync.Mutex)
	}

	masterSlaveMap[masterNode.Address] = slaveNode
	slaveMasterMap[slaveNode.Address] = masterNode
	MasterSlaveChannelMap[masterNode.Address] = make(chan MasterSlaveMessage)
	redisMutexMap[masterNode.Address] = &sync.Mutex{} // Mutex for each Master-Slave set
	redisMutexMap[slaveNode.Address] = redisMutexMap[masterNode.Address]
}
