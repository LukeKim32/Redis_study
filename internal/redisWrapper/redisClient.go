package redisWrapper

import (
	"fmt"
	"interface_hash_server/tools"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	MasterRole = "master"
	SlaveRole  = "slave"
)

type RedisClient struct {
	Connection redis.Conn
	Address    string
	Role       string
}

var redisMasterClients []RedisClient
var redisSlaveClients []RedisClient

// masterSlaveMap is a map of (Master Address -> Slave Node)
var masterSlaveMap map[string]RedisClient

// slaveMasterMap is a map of (Slave Address -> Master Node)
var slaveMasterMap map[string]RedisClient

// redistributeSlotMutex is used for sync in accessing Hash Maps After Redistribution
var redistributeSlotMutex = &sync.Mutex{}

// redisMutexMap is used for sync of request to each Redis <Master-Slave> Set With key of "Address"
var redisMutexMap map[string]*sync.Mutex

// GetRedisClient returns Redis Client corresponding to @hashSlot
/* Check Process :
 * 1) Check Hash Mapped Redis Client First (Master Node)
 * 2) Get Votes From Monitor Servers
 * 3-1) If Votes are bigger than half, Re-setup connection and return
 * 3-2) If Votes are smaller than half, Promote It's Slave Client to New Master
 */
func GetRedisClient(hashSlotIndex uint16) (RedisClient, error) {

	redisMutexMap[HashSlotMap[hashSlotIndex].Address].Lock()
	/* HashSlotMap이 checkRedisFailover() 재분배되어도, 이전 값 유지 */
	defer redisMutexMap[HashSlotMap[hashSlotIndex].Address].Unlock()

	start := time.Now()
	defer fmt.Printf(FunctionExecutionTime, time.Since(start))

	redisClient := HashSlotMap[hashSlotIndex]

	if err := checkRedisFailover(redisClient); err != nil {
		return RedisClient{}, err
	}

	tools.InfoLogger.Printf(RedisNodeSelected, HashSlotMap[hashSlotIndex].Address)

	return HashSlotMap[hashSlotIndex], nil
}

// checkRedisFailover checks @masterClient and handles failover
/*
	1. Checks If @masterClient is alive
	2. If dead,
		1) Promote mapped Slave to new master
		2) Restart previous master(@masterClient)
	3. If Master-Promoting process (2.) fails, Redistribute @masterClient Hash Slots
*/
func checkRedisFailover(masterClient RedisClient) error {

	// ask monitor nodes if master node is dead
	numberOfTotalVotes := len(monitorNodeAddressList) + 1
	votes, err := askRedisIsAliveToMonitors(masterClient)
	if err != nil {
		return err
	}

	tools.InfoLogger.Printf(FailOverVoteResult, masterClient.Address, votes, numberOfTotalVotes)

	if votes > (numberOfTotalVotes / 2) {
		return nil

	} else {
		// if more than Half says Master is dead
		tools.InfoLogger.Printf(PromotinSlaveStart, masterClient.Address)

		if err := promoteSlaveToMaster(masterClient); err != nil {

			if err.Error() == BothMasterSlaveDead {
				if err := redistruibuteHashSlot(masterClient); err != nil {
					return err
				}
				return nil // HashSlot Redistributed, This <Master-Slave> Set has no use
			}
			return err
		}

		// If promotion successes
		// Restart dead-Master using docker API

		// tools.InfoLogger.Printf("checkRedisFailover() : Promotion Success, New Slave(Previous Master %s) container restart \n", masterClient.Address)
		// if err := RestartRedisContainer(masterClient.Address); err != nil {
		// 	return err
		// }

		tools.InfoLogger.Printf(SlaveAsNewMaster, slaveMasterMap[masterClient.Address].Address)

		return nil
	}
}

// promoteSlaveToMaster checks Slave Client is alive, and promotes Slave to New Master
/*
   @maseterNode is passed with parameter
   1) Replace Hash Map Value(Redis Client info)
   2) Swap Master Client and Slave client from each others list
   Returns Slave Node and error if Slave node has an error
*/
func promoteSlaveToMaster(masterNode RedisClient) error {

	// Check if mapped Slave is alive before Promotion
	slaveNode, isSet := masterSlaveMap[masterNode.Address]
	if isSet == false {
		return fmt.Errorf(MasterSlaveMapNotInit)
	}

	numberOfTotalVotes := len(monitorNodeAddressList) + 1
	votes, err := askRedisIsAliveToMonitors(slaveNode)
	if err != nil {
		return err
	}

	tools.InfoLogger.Printf(FailOverVoteResult, slaveNode.Address, votes, numberOfTotalVotes)

	// If more than half says Slave is dead
	if votes <= (numberOfTotalVotes / 2) {
		return fmt.Errorf(BothMasterSlaveDead)
	}

	tools.InfoLogger.Printf(PromotingSlaveNode, slaveNode.Address)

	// Start Promotion
	for _, eachHashRangeOfClient := range clientHashRangeMap[masterNode.Address] {
		hashSlotStart := eachHashRangeOfClient.startIndex
		hashSlotEnd := eachHashRangeOfClient.endIndex

		// Assign Master's Hash Slots to Slave Node
		slaveNode.Role = MasterRole
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

	// Refresh configs/global variables of Slave Client and Master Client
	if err := swapMasterSlaveConfigs(masterNode, slaveNode); err != nil {
		return err
	}

	return nil
}

// getSlave checks Slave Node of @masterNode if alive and returns it
/* Error Template is not defined yet
 */
func getSlave(masterNode RedisClient) (RedisClient, error) {

	slaveNode, isSet := masterSlaveMap[masterNode.Address]
	if isSet == false {
		return RedisClient{}, fmt.Errorf("")
	}

	// ask monitor nodes if slave node is dead
	numberOfTotalVotes := len(monitorNodeAddressList) + 1
	votes, err := askRedisIsAliveToMonitors(slaveNode)
	if err != nil {
		return RedisClient{}, fmt.Errorf("")
	}

	tools.InfoLogger.Printf(FailOverVoteResult, slaveNode.Address, votes, numberOfTotalVotes)

	if votes < (numberOfTotalVotes / 2) {
		return RedisClient{}, fmt.Errorf("")
	}

	// After cheked it's alive
	return slaveNode, nil
}

func GetRedisClientWithAddress(address string) (RedisClient, error) {

	clientsList := make([]RedisClient, len(redisMasterClients)+len(redisSlaveClients))
	clientsList = append(redisMasterClients, redisSlaveClients...)

	if len(clientsList) == 0 {
		return RedisClient{}, fmt.Errorf(NotAnyRedisSetUpYet)
	}

	for _, eachClient := range clientsList {
		if eachClient.Address == address {
			return eachClient, nil
		}
	}

	return RedisClient{}, fmt.Errorf(NoMatchingResponseNode)
}

func swapMasterSlaveConfigs(masterNode RedisClient, slaveNode RedisClient) error {

	removedSlaveNode, err := RemoveSlaveFromList(slaveNode)
	if err != nil {
		return err
	}

	removedMasterNode, err := RemoveMasterFromList(masterNode)
	if err != nil {
		return err
	}

	removedSlaveNode.Role = MasterRole
	removedMasterNode.Role = SlaveRole

	redisSlaveClients = append(redisSlaveClients, removedMasterNode)
	redisMasterClients = append(redisMasterClients, removedSlaveNode)

	delete(masterSlaveMap, masterNode.Address)
	masterSlaveMap[slaveNode.Address] = masterNode

	delete(slaveMasterMap, slaveNode.Address)
	slaveMasterMap[masterNode.Address] = slaveNode

	for _, eachMaster := range redisMasterClients {
		tools.InfoLogger.Printf(RefreshedMasters, eachMaster.Address)
	}
	for _, eachSlave := range redisSlaveClients {
		tools.InfoLogger.Printf(RefreshedSlaves, eachSlave.Address)
	}

	return nil
}

func RemoveSlaveFromList(slaveNode RedisClient) (RedisClient, error) {
	var removedRedisClient RedisClient

	tools.InfoLogger.Printf(SlaveNodeInfo, slaveNode.Role, slaveNode.Address)

	for i, eachClient := range redisSlaveClients {
		if eachClient.Address == slaveNode.Address {
			removedRedisClient = eachClient

			redisSlaveClients[i] = redisSlaveClients[len(redisSlaveClients)-1]
			redisSlaveClients[len(redisSlaveClients)-1] = RedisClient{}

			redisSlaveClients = redisSlaveClients[:len(redisSlaveClients)-1]

			return removedRedisClient, nil
		}
	}

	return RedisClient{}, fmt.Errorf(NoMatchingSlaveToRemove, slaveNode.Address)
}

func RemoveMasterFromList(masterNode RedisClient) (RedisClient, error) {
	var removedRedisClient RedisClient

	tools.InfoLogger.Printf(MasterNodeInfo, masterNode.Role, masterNode.Address)

	for i, eachClient := range redisMasterClients {
		if eachClient.Address == masterNode.Address {
			removedRedisClient = eachClient

			redisMasterClients[i] = redisMasterClients[len(redisMasterClients)-1]
			redisMasterClients[len(redisMasterClients)-1] = RedisClient{}

			redisMasterClients = redisMasterClients[:len(redisMasterClients)-1]

			return removedRedisClient, nil
		}
	}

	return RedisClient{}, fmt.Errorf(NoMatchingMasterToRemove, masterNode.Address)
}

func isRedisMaster(redisNode RedisClient) bool {
	for _, eachMasterNode := range redisMasterClients {
		if eachMasterNode == redisNode {
			return true
		}
	}

	return false
}
