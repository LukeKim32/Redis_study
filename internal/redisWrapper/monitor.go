package redisWrapper

import (
	"encoding/json"
	"fmt"
	"interface_hash_server/configs"
	"interface_hash_server/tools"
	"net/http"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

var monitorNodeAddressList = []string{configs.MonitorNodeOneAddress, configs.MonitorNodeTwoAddress}

type MonitorServerResponse struct {
	RedisNodeAddress string
	IsAlive          bool
	ErrorMessage     string
}

type MasterSlaveMessage struct {
	MasterNode RedisClient
	SlaveNode  RedisClient
	// isCurrentSlaveDead is the status of current Struct Variable's "SlaveNode" field
	isCurrentSlaveDead bool
}

// MasterSlaveChannelMap is a map (Master Node Address -> Channel) for Master And Slave Nodes' Communication
var MasterSlaveChannelMap map[string](chan MasterSlaveMessage)

var errorChannel chan error

// MonitorNodes monitors passed @redisClients
func StartMonitorNodes() {
	errorChannel = make(chan error)
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:

				// start := time.Now()
				// fmt.Printf("타이머 시간 재기 시작 : %v\n", start)

				if err := checkRedisClientSetup(); err != nil {
					errorChannel <- err
				}

				for _, eachMasterNode := range redisMasterClients {
					go startGoRoutineMonitor(eachMasterNode, errorChannel)
				}

				// fmt.Printf("GetRedisclient 걸린 시간 %v\n", time.Since(start))

			case <-errorChannel:
				ticker.Stop()
				close(errorChannel)
				tools.ErrorLogger.Println("MonitorMasters() : Error - timer stopped")
				return
			}
		}
	}()
}

func startGoRoutineMonitor(redisNode RedisClient, errorChannel chan error) {

	redisMutexMap[redisNode.Address].Lock()
	defer redisMutexMap[redisNode.Address].Unlock()

	// Redis Node can be discarded from Master nodes if redistribute happens
	if ok := isRedisMaster(redisNode); ok != true {
		return
	}

	if err := checkRedisFailover(redisNode); err != nil {
		errorChannel <- err
		return
	}
}

// askMasterIsAlive returns the "votes" of Monitor Servers' checking if Redis node is alive
/* with given @redisNodeAddress
 * Uses Goroutines to request to every Monitor Nodes
 */
func askRedisIsAliveToMonitors(redisNode RedisClient) (int, error) {

	numberOfmonitorNode := len(monitorNodeAddressList)
	outputChannel := make(chan MonitorServerResponse, numberOfmonitorNode) // Buffered Channel - Async

	tools.InfoLogger.Println("askRedisIsAliveToMonitors() : start to request to Monitor servers")

	votes := 0

	// First ping-test by Host
	hostPingResult, _ := redis.String(redisNode.Connection.Do("PING"))
	if strings.Contains(hostPingResult, "PONG") {
		votes++
	}

	for _, eachMonitorNodeAddress := range monitorNodeAddressList {
		// GET request With URI http://~/monitor/{redisNodeAddress}
		go requestToMonitor(outputChannel, eachMonitorNodeAddress, redisNode.Address)
	}

	for i := 0; i < numberOfmonitorNode; i++ {

		tools.InfoLogger.Println("askRedisIsAliveToMonitors() : Waiting for response in channel")

		// 임의의 Goroutine에서 보낸 Response 처리 (Buffered channel 이라 Goroutine이 async)
		// Wait til Channel gets Response
		monitorServerResponse := <-outputChannel
		tools.InfoLogger.Printf("askRedisIsAliveToMonitors() : Buffered channel response : %s\n", monitorServerResponse.ErrorMessage)

		if monitorServerResponse.ErrorMessage != "" {
			return 0, fmt.Errorf(monitorServerResponse.ErrorMessage)
		}

		if monitorServerResponse.IsAlive {
			tools.InfoLogger.Printf("askRedisIsAliveToMonitors() : %s is said to be alive\n", redisNode.Address)
			votes++
		} else {
			tools.InfoLogger.Printf("askRedisIsAliveToMonitors() : %s is said to be dead\n", redisNode.Address)
		}
	}

	return votes, nil
}

// requestToMonitor is a goroutine for request to Monitor servers
func requestToMonitor(outputChannel chan<- MonitorServerResponse, monitorAddress string, redisNodeAddress string) {

	requestURI := fmt.Sprintf("http://%s/monitor/%s", monitorAddress, redisNodeAddress)

	tools.InfoLogger.Println("requestToMonitor() : Request To : ", requestURI)

	// Request To Monitor server
	response, err := http.Get(requestURI)
	if err != nil {
		tools.ErrorLogger.Println("requestToMonitor() : Response - err 1: ", err)
		outputChannel <- MonitorServerResponse{
			ErrorMessage: fmt.Sprintf("requestToMonitor() : Monitor server(IP : %s) response error", monitorAddress),
		}
	}
	defer response.Body.Close()

	// Parse Response
	var monitorServerResponse MonitorServerResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&monitorServerResponse); err != nil {
		tools.ErrorLogger.Println("requestToMonitor() : Response - err 2: ", monitorServerResponse.ErrorMessage)
		outputChannel <- MonitorServerResponse{
			ErrorMessage: err.Error(),
		}
	}

	// Check the result
	if monitorServerResponse.RedisNodeAddress == redisNodeAddress {
		tools.InfoLogger.Println("requestToMonitor() : Response - is alive : ", monitorServerResponse.IsAlive)

		outputChannel <- MonitorServerResponse{
			IsAlive:      monitorServerResponse.IsAlive,
			ErrorMessage: "",
		}
	} else {
		tools.ErrorLogger.Println("requestToMonitor() : Response - err 3: ", monitorServerResponse.ErrorMessage)
		outputChannel <- MonitorServerResponse{
			ErrorMessage: fmt.Sprintf("requestToMonitor() : Reuqested Redis Node Address Not Match with Response"),
		}
	}
}

func checkRedisClientSetup() error {

	if len(redisMasterClients) == 0 {
		return fmt.Errorf("checkRedisClientSetup() error : No Redis Master Clients set up")
	}

	if len(redisSlaveClients) == 0 {
		return fmt.Errorf("checkRedisClientSetup() error : No Redis Slave Clients set up")
	}

	return nil
}
