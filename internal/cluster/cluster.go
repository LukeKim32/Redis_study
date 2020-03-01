package cluster

import (
	"fmt"
	"interface_hash_server/internal/cluster/templates"
	"interface_hash_server/tools"
	"time"
)

// GetRedisClient : 해쉬 슬롯의 @hashSlotIndex 번째 인덱스를 담당하는 Redis Client 반환
/* Check Process :
 * 1) Check Hash Mapped Redis Client First (Master Node)
 * 2) Get Votes From Monitor Servers
 * 3-1) If Votes are bigger than half, Re-setup connection and return
 * 3-2) If Votes are smaller than half, Promote It's Slave Client to New Master
 */
func GetRedisClient(hashSlotIndex uint16) (RedisClient, error) {

	redisMutexMap[hashSlot[hashSlotIndex].Address].Lock()

	// HashSlot이 handleIfDead() 재분배되어도, 이전 값 유지
	defer redisMutexMap[hashSlot[hashSlotIndex].Address].Unlock()

	start := time.Now()
	defer fmt.Printf(templates.FunctionExecutionTime, time.Since(start))

	redisClient := hashSlot[hashSlotIndex]

	// 반환 전, redisClient의 생존여부 확인/처리
	if err := redisClient.handleIfDead(); err != nil {
		return RedisClient{}, err
	}

	tools.InfoLogger.Printf(templates.RedisNodeSelected, hashSlot[hashSlotIndex].Address)

	return hashSlot[hashSlotIndex], nil
}

func GetRedisClientWithAddress(address string) (RedisClient, error) {

	clientsList := make([]RedisClient, len(redisMasterClients)+len(redisSlaveClients))
	clientsList = append(redisMasterClients, redisSlaveClients...)

	if len(clientsList) == 0 {
		return RedisClient{}, fmt.Errorf(templates.NotAnyRedisSetUpYet)
	}

	for _, eachClient := range clientsList {
		if eachClient.Address == address {
			return eachClient, nil
		}
	}

	return RedisClient{}, fmt.Errorf(templates.NoMatchingResponseNode)
}

func swapMasterSlaveConfigs(masterNode *RedisClient, slaveNode *RedisClient) error {

	if err := slaveNode.removeFromList(); err != nil {
		return err
	}

	if err := masterNode.removeFromList(); err != nil {
		return err
	}

	slaveNode.Role = MasterRole
	masterNode.Role = SlaveRole

	redisSlaveClients = append(redisSlaveClients, *masterNode)
	redisMasterClients = append(redisMasterClients, *slaveNode)

	delete(masterSlaveMap, masterNode.Address)
	masterSlaveMap[slaveNode.Address] = *masterNode

	delete(slaveMasterMap, slaveNode.Address)
	slaveMasterMap[masterNode.Address] = *slaveNode

	return nil
}

func checkRedisClientSetup() error {

	if len(redisMasterClients) == 0 {
		return fmt.Errorf(templates.RedisMasterNotSetUpYet)
	}

	if len(redisSlaveClients) == 0 {
		return fmt.Errorf(templates.RedisSlaveNotSetUpYet)
	}

	return nil
}
