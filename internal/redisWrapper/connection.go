package redisWrapper

import (
	"fmt"
	"interface_hash_server/internal/hash"
	"interface_hash_server/tools"

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
			tools.ErrorLogger.Printf("Redis Node(%s) connection failure - %s\n", eachNodeAddress, err.Error())
			return err
		}
		newRedisClient.Address = eachNodeAddress

		switch connectOption {
		case Default:
			redisMasterClients = append(redisMasterClients, newRedisClient)

		case SlaveSetup:
			if len(redisMasterClients) == 0 {
				fmt.Errorf("Redis Master Node should be set up first")
			}

			// 동일한 Index의 Master Node와 Map
			masterAddressList := GetInitialMasterAddressList()
			masterNode, err := GetRedisClientWithAddress(masterAddressList[i])
			if err != nil {
				return err
			}

			initMasterSlaveMaps(masterNode, newRedisClient)

			redisSlaveClients = append(redisSlaveClients, newRedisClient)

			tools.InfoLogger.Printf("Redis Slave Node(%s) Mapped from Master Node(%s) Success\n", eachNodeAddress, masterAddressList[i])
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
	clientHashRangeMap = make(map[string][]HashRange)

	for i, eachRedisClient := range redisMasterClients {
		// arithmatic order fixed to prevent Mantissa Loss
		hashSlotStart := uint16(float64(i) / float64(connectionCount) * float64(hash.HashSlotsNumber))
		hashSlotEnd := uint16(float64(i+1) / float64(connectionCount) * float64(hash.HashSlotsNumber))

		assignHashSlotMap(hashSlotStart, hashSlotEnd, eachRedisClient)

		newHashRange := HashRange{
			startIndex: hashSlotStart,
			endIndex:   hashSlotEnd,
		}
		clientHashRangeMap[eachRedisClient.Address] = append(clientHashRangeMap[eachRedisClient.Address], newHashRange)

		tools.InfoLogger.Printf("Node %s hash slot range %d ~ %d\n", eachRedisClient.Address, hashSlotStart, hashSlotEnd)
	}

	return nil
}

func initMasterSlaveMaps(masterNode RedisClient, slaveNode RedisClient) {
	if masterSlaveMap == nil {
		masterSlaveMap = make(map[RedisClient]RedisClient)
	}
	if slaveMasterMap == nil {
		masterSlaveMap = make(map[RedisClient]RedisClient)
	}
	if MasterSlaveChannelMap == nil {
		MasterSlaveChannelMap = make(map[string](chan MonitorResult))
	}

	masterSlaveMap[masterNode] = slaveNode
	slaveMasterMap[slaveNode] = masterNode
	MasterSlaveChannelMap[masterNode.Address] = make(chan MonitorResult)
}
