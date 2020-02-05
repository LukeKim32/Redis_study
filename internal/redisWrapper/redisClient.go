package redisWrapper

import (
	"fmt"
	"interface_hash_server/tools"

	"github.com/gomodule/redigo/redis"
)

type RedisClient struct {
	Connection redis.Conn
	Address    string
}

var redisMasterClients []RedisClient
var redisSlaveClients []RedisClient

// masterSlaveMap is a map of (Master Address -> Slave Address)
var masterSlaveMap map[RedisClient]RedisClient

// slaveMasterMap is a map of (Slave Address -> Master Address)
var slaveMasterMap map[RedisClient]RedisClient

// GetRedisClient returns Redis Client corresponding to @hashSlot
/* Check Process :
 * 1) Check Hash Mapped Redis Client First (Master Node)
 * 2) Get Votes From Monitor Servers
 * 3-1) If Votes are bigger than half, Re-setup connection and return
 * 3-2) If Votes are smaller than half, Promote It's Slave Client to New Master
 */
func GetRedisClient(hashSlotIndex uint16) (RedisClient, error) {
	redisClient := HashSlotMap[hashSlotIndex]

	tools.InfoLogger.Printf("GetRedisClient() : Redis Node(%s) selected\n", redisClient.Address)

	_, err := redis.String(redisClient.Connection.Do("PING"))
	if err != nil {
		tools.ErrorLogger.Printf("Redis Node(%s) Ping test failure\n", redisClient.Address)

		// Now No error is allowed! Every Request will work
		// ask monitor nodes if master node is dead
		votes, err := askRedisIsAliveToMonitors(redisClient.Address)
		if err != nil {
			return RedisClient{}, err
		}

		numberOfTotalVotes := len(monitorNodeAddressList) + 1
		if votes > (numberOfTotalVotes / 2) { // If more than half says it's not dead
			// setupConnectionAgain()
		} else {
			// if 과반수 says dead,
			// Switch Slave to Master. (Slave까지 죽은것에 대해선 Redis Cluster도 처리 X => Docker restart로 처리해보자)
			slaveNode := masterSlaveMap[redisClient]
			if err := promoteSlaveToMaster(slaveNode); err != nil {
				return RedisClient{}, err
			}

			return slaveNode, nil
		}
	}

	tools.InfoLogger.Printf("Redis Node(%s) Ping test success\n", redisClient.Address)

	return redisClient, nil
}

// promoteSlaveToMaster promotes Slave Client to Master Client and Check Slave Client is connected
/* 1) Replace Hash Map Value(Redis Client info)
 * 2) Swap Master Client and Slave client from each others' list
 * Returns Newly Master-Promoted Client (Previous Slave)
 */
func promoteSlaveToMaster(slaveNode RedisClient) error {

	masterNode, isSet := slaveMasterMap[slaveNode]
	if isSet == false {
		return fmt.Errorf("promoteSlaveToMaster() - Master Slave Map is not set up")
	}

	// Ping test to check If slave client is alive
	_, err := redis.String(slaveNode.Connection.Do("PING"))
	if err != nil {
		return fmt.Errorf("New Master-Promoted Client is also dead")
	}

	for _, eachHashRangeOfClient := range clientHashRangeMap[masterNode.Address] {
		hashSlotStart := eachHashRangeOfClient.startIndex
		hashSlotEnd := eachHashRangeOfClient.endIndex

		// Assign Master's Hash Slots to Slave Node
		assignHashSlotMap(hashSlotStart, hashSlotEnd, slaveNode)

		newHashRange := HashRange{
			startIndex: hashSlotStart,
			endIndex:   hashSlotEnd,
		}

		// Record slave address -> Hash slots Range
		clientHashRangeMap[slaveNode.Address] = append(clientHashRangeMap[slaveNode.Address], newHashRange)
	}

	// Remove Master's Hash Range => Let Garbace Collect
	clientHashRangeMap[masterNode.Address] = nil
	delete(clientHashRangeMap, masterNode.Address)

	// Swap Slave Client and Master Client
	if _, err := RemoveRedisClient(slaveNode, redisSlaveClients); err != nil {
		return err
	}

	if _, err := RemoveRedisClient(masterNode, redisMasterClients); err != nil {
		return err
	}
	redisSlaveClients = append(redisSlaveClients, masterNode)
	redisMasterClients = append(redisMasterClients, slaveNode)

	delete(slaveMasterMap, slaveNode)
	slaveMasterMap[masterNode] = slaveNode
	delete(masterSlaveMap, masterNode)
	masterSlaveMap[slaveNode] = masterNode

	return nil
}

func GetRedisClientWithAddress(address string) (RedisClient, error) {

	clientsList := make([]RedisClient, len(redisMasterClients)+len(redisSlaveClients))
	clientsList = append(redisMasterClients, redisSlaveClients...)

	if len(clientsList) == 0 {
		return RedisClient{}, fmt.Errorf("GetRedisClientWithAddress() error : No Redis Clients set up")
	}

	for _, eachClient := range clientsList {
		if eachClient.Address == address {
			return eachClient, nil
		}
	}

	return RedisClient{}, fmt.Errorf("GetRedisClientWithAddress() error : No Matching Redis Client With passed address")
}

func RemoveRedisClient(targetNode RedisClient, redisClientsList []RedisClient) (RedisClient, error) {
	var removedRedisClient RedisClient

	for i, eachClient := range redisClientsList {
		if eachClient == targetNode {
			removedRedisClient = eachClient

			redisClientsList[i] = redisClientsList[len(redisClientsList)-1]
			redisClientsList[len(redisClientsList)-1] = RedisClient{}
			redisClientsList = redisClientsList[:len(redisClientsList)-1]

			return removedRedisClient, nil
		}
	}

	return RedisClient{}, fmt.Errorf("removeRedisClient() : No Matching Redis Client to Remove")
}

// assignHashSlotMap assigns Hash slots (@start ~ @end) to passed @redisNode
/* It basically unrolls the loop with 16 states for cahching
 * And If the range Is not divided by 16, Remains will be handled with single statement loop
 */
func assignHashSlotMap(start uint16, end uint16, redisNode RedisClient) {

	var i uint16
	nextSlotIndex := start + 16
	// Replace Hash Map With Slave Client
	for i := start; nextSlotIndex < end; i += 16 {
		HashSlotMap[i] = redisNode
		HashSlotMap[i+1] = redisNode
		HashSlotMap[i+2] = redisNode
		HashSlotMap[i+3] = redisNode
		HashSlotMap[i+4] = redisNode
		HashSlotMap[i+5] = redisNode
		HashSlotMap[i+6] = redisNode
		HashSlotMap[i+7] = redisNode
		HashSlotMap[i+8] = redisNode
		HashSlotMap[i+9] = redisNode
		HashSlotMap[i+10] = redisNode
		HashSlotMap[i+11] = redisNode
		HashSlotMap[i+12] = redisNode
		HashSlotMap[i+13] = redisNode
		HashSlotMap[i+14] = redisNode
		HashSlotMap[i+15] = redisNode
		nextSlotIndex += 16
	}

	for ; i < end; i++ {
		HashSlotMap[i] = redisNode
	}

}
