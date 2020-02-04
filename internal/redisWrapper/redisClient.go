package redisWrapper

import (
	"encoding/json"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"interface_hash_server/configs"
	"interface_hash_server/tools"
	"net/http"
)

type RedisClient struct {
	Connection redis.Conn
	Address    string
}

var redisMasterClients []RedisClient
var redisSlaveClients []RedisClient

// RedisNodeAddressList are Address list of Redis Nodes
var RedisMasterAddressList = []string{
	configs.RedisMasterOneAddress,
	configs.RedisMasterTwoAddress,
	configs.RedisMasterThreeAddress,
}

var RedisSlaveAddressList = []string{
	configs.RedisSlaveOneAddress,
	configs.RedisSlaveTwoAddress,
	configs.RedisSlaveThreeAddress,
}

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
			slaveNode := MasterSlaveMap[redisClient]
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

	masterNode, isSet := SlaveMasterMap[slaveNode]
	if isSet == false {
		return fmt.Errorf("promoteSlaveToMaster() - Master Slave Map is not set up")
	}

	// Ping test to check If slave client is alive
	_, err := redis.String(slaveNode.Connection.Do("PING"))
	if err != nil {
		return fmt.Errorf("New Master-Promoted Client is also dead")
	}

	startHashSlotIndex := clientHashRangeMap[masterNode.Address].startIndex
	endHashSlotIndex := clientHashRangeMap[masterNode.Address].endIndex

	// Replace Hash Map With Slave Client
	for i := startHashSlotIndex; i < endHashSlotIndex; i++ {
		HashSlotMap[i] = slaveNode
	}

	clientHashRangeMap[slaveNode.Address] = HashRange{
		startIndex: startHashSlotIndex,
		endIndex:   endHashSlotIndex,
	}
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

	delete(SlaveMasterMap, slaveNode)
	SlaveMasterMap[masterNode] = slaveNode
	delete(MasterSlaveMap, masterNode)
	SlaveMasterMap[slaveNode] = masterNode

	return nil
}

// askMasterIsAlive returns the "votes" of Monitor Servers' checking if Redis node is alive
/* with given @redisNodeAddress
 * Uses Goroutines to request to every Monitor Nodes
 */
func askRedisIsAliveToMonitors(redisNodeAddress string) (int, error) {
	numberOfmonitorNode := len(monitorNodeAddressList)
	outputChannel := make(chan MonitorResponse, numberOfmonitorNode) // Buffered Channel - Async

	for _, eachMonitorNodeAddress := range monitorNodeAddressList {
		// request
		// GET request With URI http://~/monitor/{redisNodeAddress}
		go func(outputChannel chan<- MonitorResponse, monitorAddress string, redisNodeAddress string) {
			requestURI := fmt.Sprintf("http://%s/monitor/%s", monitorAddress, redisNodeAddress)

			tools.InfoLogger.Println("askMasterIsAlive() : Request To : ", requestURI)

			response, err := http.Get(requestURI)
			if err != nil {
				tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 1: ", err)
				outputChannel <- MonitorResponse{ErrorMessage: fmt.Sprintf("Monitor server(IP : %s) response error", monitorAddress)}
			}
			defer response.Body.Close()

			var monitorResponse MonitorResponse
			decoder := json.NewDecoder(response.Body)
			if err := decoder.Decode(&monitorResponse); err != nil {
				tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 2: ", monitorResponse.ErrorMessage)
				outputChannel <- MonitorResponse{ErrorMessage: err.Error()}
			}

			if monitorResponse.RedisNodeAddress == redisNodeAddress {
				tools.InfoLogger.Println("askMasterIsAlive() : Response - is alive : ", monitorResponse.IsAlive)
				outputChannel <- MonitorResponse{IsAlive: monitorResponse.IsAlive, ErrorMessage: ""}
			} else {
				tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 3: ", monitorResponse.ErrorMessage)
				outputChannel <- MonitorResponse{ErrorMessage: fmt.Sprintf("Reuqested Redis Node Address Not Match with Response")}
			}
		}(outputChannel, eachMonitorNodeAddress, redisNodeAddress)
	}

	votes := 0
	for i := 0; i < numberOfmonitorNode; i++ {
		// 임의의 Goroutine에서 보낸 Response 처리 (Buffered channel 이라 Goroutine이 async)
		// Wait til Channel gets Response
		monitorResponse := <-outputChannel
		if monitorResponse.ErrorMessage != "" {
			return 0, fmt.Errorf(monitorResponse.ErrorMessage)
		}

		if monitorResponse.IsAlive {
			votes++
		}
	}

	return votes, nil
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
