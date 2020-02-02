package redisWrapper

import (
	"fmt"
	"net/http"
	"encoding/json"
	"github.com/gomodule/redigo/redis"
	"interface_hash_server/configs"
	"interface_hash_server/tools"
)

type RedisClient struct{
	Connection redis.Conn
	Address	string
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
		votes, err := askMasterIsAlive(redisClient.Address)
		if err != nil {
			return RedisClient{}, err
		}

		numberOfTotalVotes := len(monitorNodeAddressList) + 1
		if votes > (numberOfTotalVotes/2) {  // If more than half says it's not dead
			// setupConnectionAgain()
		} else {
			// if 과반수 says dead,
			// Switch Slave to Master. (Slave까지 죽은것에 대해선 Redis Cluster도 처리 X => Docker restart로 처리해보자)
			newMasterClient, err := promoteSlaveToMaster(redisClient.Address)
			if  err != nil {
				return RedisClient{}, err
			}

			return newMasterClient, nil
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
func promoteSlaveToMaster(masterAddress string) (RedisClient, error){

	slaveAddress := masterSlaveMap[masterAddress]
	slaveClient, err := GetRedisClientWithAddress(slaveAddress)
	if err != nil {
		return RedisClient{}, err
	}

	// Ping test to check If slave client is alive
	_, err = redis.String(slaveClient.Connection.Do("PING"))
	if err != nil {
		return RedisClient{}, fmt.Errorf("New Master-Promoted Client is also dead")
	}

	startHashSlotIndex := clientHashRangeMap[masterAddress].startIndex
	endHashSlotIndex := clientHashRangeMap[masterAddress].endIndex

	// Replace Hash Map With Slave Client
	for i :=startHashSlotIndex; i < endHashSlotIndex; i++ {
		HashSlotMap[i] = slaveClient
	}

	clientHashRangeMap[slaveAddress] = HashRange {
		startIndex : startHashSlotIndex,
		endIndex : endHashSlotIndex,
	}
	delete(clientHashRangeMap,masterAddress)	

	// Swap Slave Client and Master Client
	if _, err := RemoveRedisClient(slaveAddress,redisSlaveClients); err != nil {
		return RedisClient{}, err
	}

	masterClient, err := RemoveRedisClient(masterAddress,redisMasterClients)
	if err != nil {
		return RedisClient{}, err
	}
	redisSlaveClients = append(redisSlaveClients,masterClient)
	redisMasterClients = append(redisMasterClients, slaveClient)

	
	return slaveClient, nil
}


// askMasterIsAlive returns the "votes" of Monitor Servers' checking if Redis node is alive
/* with given @redisNodeAddress
 * Uses Goroutines to request to every Monitor Nodes
*/
func askMasterIsAlive(redisNodeAddress string) (int, error) {
	numberOfmonitorNode := len(monitorNodeAddressList)
	outputChannel := make(chan MonitorResponse, numberOfmonitorNode) // Buffered Channel - Async
	
	for _, eachMonitorNodeAddress := range monitorNodeAddressList {
		// request
		// GET request With URI http://~/monitor/{redisNodeAddress}
		go func(outputChannel chan <- MonitorResponse, monitorAddress string, redisNodeAddress string) {
			requestURI := fmt.Sprintf("http://%s/monitor/%s",monitorAddress,redisNodeAddress)

			tools.InfoLogger.Println("askMasterIsAlive() : Request To : ",requestURI)
			
			response, err := http.Get(requestURI)
			if err != nil {
				tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 1: ",err)
				outputChannel <- MonitorResponse{Err : fmt.Errorf("Monitor server(IP : %s) response error",monitorAddress)}
			}
			defer response.Body.Close()

			var monitorResponse MonitorResponse
			decoder := json.NewDecoder(response.Body)
			if err := decoder.Decode(&monitorResponse); err != nil {
				tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 2: ",monitorResponse.Err)
				outputChannel <- MonitorResponse{Err : err}
			}
	
			if monitorResponse.RedisNodeAddress == redisNodeAddress {
				tools.InfoLogger.Println("askMasterIsAlive() : Response - is alive : ",monitorResponse.IsAlive)
				outputChannel <- MonitorResponse{IsAlive: monitorResponse.IsAlive, Err : nil}
			} else {
				tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 3: ",monitorResponse.Err)
				outputChannel <- MonitorResponse{Err : fmt.Errorf("Reuqested Redis Node Address Not Match with Response")}
			}
		} (outputChannel, eachMonitorNodeAddress, redisNodeAddress)
	}

	votes := 0
	for i:=0; i<numberOfmonitorNode; i++ {
		// 임의의 Goroutine에서 보낸 Response 처리 (Buffered channel 이라 Goroutine이 async)
		// Wait til Channel gets Response
		monitorResponse := <- outputChannel
		if monitorResponse.Err != nil {
			return 0, monitorResponse.Err
		}

		if monitorResponse.IsAlive {
			votes++
		}
	}

	return votes, nil
}

func GetRedisClientWithAddress(address string) (RedisClient, error) {

	clientsList := make([]RedisClient, len(redisMasterClients) + len(redisSlaveClients))
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

func RemoveRedisClient(address string, redisClientsList []RedisClient) (RedisClient, error){
	var removedRedisClient RedisClient

	for i, eachClient := range redisClientsList {
		if eachClient.Address == address {
			removedRedisClient = eachClient

			redisClientsList[i] = redisClientsList[len(redisClientsList)-1]
			redisClientsList[len(redisClientsList)-1] = RedisClient{}
			redisClientsList = redisClientsList[:len(redisClientsList)-1]
			
			return removedRedisClient, nil
		}
	}
	
	return RedisClient{}, fmt.Errorf("removeRedisClient() : No Matching Redis Client to Remove")
}