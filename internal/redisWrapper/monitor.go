package redisWrapper

import (
	"encoding/json"
	"fmt"
	"interface_hash_server/configs"
	"interface_hash_server/tools"
	"net/http"
	"time"
)

var monitorNodeAddressList = []string{configs.MonitorNodeOneAddress, configs.MonitorNodeTwoAddress}

type MonitorResponse struct {
	RedisNodeAddress string
	IsAlive          bool
	ErrorMessage     string
}

type MonitorResult struct {
	RedisAddress string
	isAlive      bool
}

// MasterSlaveChannelMap is a map (Master Node Address -> Channel) for Master And Slave Nodes' Communication
var MasterSlaveChannelMap map[string](chan MonitorResult)

func MonitorMasters() {
	ticker := time.NewTicker(5 * time.Second)
	quit := make(chan error)
	go func() {
		for {
			select {
			case <-ticker.C:

				for _, eachMasterNode := range redisMasterClients {
					votes, err := askRedisIsAliveToMonitors(eachMasterNode.Address)
					if err != nil {
						quit <- err
						break
					}

					numberOfTotalVotes := len(monitorNodeAddressList) + 1
					if votes > (numberOfTotalVotes / 2) { // If more than half says it's not dead
						// setupConnectionAgain()
						MasterSlaveChannelMap[eachMasterNode.Address] <- MonitorResult{RedisAddress: eachMasterNode.Address, isAlive: true}
					} else {
						// if 과반수 says dead,
						// Switch Slave to Master. (Slave까지 죽은것에 대해선 Redis Cluster도 처리 X => Docker restart로 처리해보자)
						if err := promoteSlaveToMaster(masterSlaveMap[eachMasterNode]); err != nil {
							// Slave까지 죽었다면
							// 다른 Master가 Hash slot
							quit <- err
							break
						}

						MasterSlaveChannelMap[eachMasterNode.Address] <- MonitorResult{RedisAddress: eachMasterNode.Address, isAlive: false}
					}
				}

			case <-quit:
				ticker.Stop()
				close(quit)
				tools.ErrorLogger.Println("MonitorMasters() : Error - timer stopped")
				return
			}
		}
	}()

}

func MonitorSlaves() {
	ticker := time.NewTicker(10 * time.Second)
	quit := make(chan error)
	go func() {
		for {
			select {
			case <-ticker.C:

				for _, eachSlaveNode := range redisSlaveClients {
					votes, err := askRedisIsAliveToMonitors(eachSlaveNode.Address)
					if err != nil {
						quit <- err
						break
					}

					numberOfTotalVotes := len(monitorNodeAddressList) + 1
					if votes > (numberOfTotalVotes / 2) { // If more than half says it's not dead
						// setupConnectionAgain()
					} else {
						// if 과반수 says dead,
						masterNode := slaveMasterMap[eachSlaveNode]
						monitorResult := <-MasterSlaveChannelMap[masterNode.Address]

						if monitorResult.isAlive {
							if err := RestartRedisContainer(eachSlaveNode.Address); err != nil {
								quit <- err
								break
							}
						} else {
							// Master까지 죽었다면
							// 다른 Master가 Hash slot
							RemoveRedisClient(masterNode, redisMasterClients)
							redistruibuteHashSlot(masterNode)
							delete(MasterSlaveChannelMap, masterNode.Address)
							delete(masterSlaveMap, masterNode)
							delete(slaveMasterMap, eachSlaveNode)
						}
					}
				}

			case <-quit:
				ticker.Stop()
				close(quit)
				tools.ErrorLogger.Println("MonitorMasters() : Error - timer stopped")
				return
			}
		}
	}()
}

// askMasterIsAlive returns the "votes" of Monitor Servers' checking if Redis node is alive
/* with given @redisNodeAddress
 * Uses Goroutines to request to every Monitor Nodes
 */
func askRedisIsAliveToMonitors(redisNodeAddress string) (int, error) {
	numberOfmonitorNode := len(monitorNodeAddressList)
	outputChannel := make(chan MonitorResponse, numberOfmonitorNode) // Buffered Channel - Async

	for _, eachMonitorNodeAddress := range monitorNodeAddressList {

		// GET request With URI http://~/monitor/{redisNodeAddress}
		go requestToMonitor(outputChannel, eachMonitorNodeAddress, redisNodeAddress)
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

// requestToMonitor is a goroutine for request to Monitor servers
func requestToMonitor(outputChannel chan<- MonitorResponse, monitorAddress string, redisNodeAddress string) {

	requestURI := fmt.Sprintf("http://%s/monitor/%s", monitorAddress, redisNodeAddress)

	tools.InfoLogger.Println("askMasterIsAlive() : Request To : ", requestURI)

	// Request To Monitor server
	response, err := http.Get(requestURI)
	if err != nil {
		tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 1: ", err)
		outputChannel <- MonitorResponse{ErrorMessage: fmt.Sprintf("Monitor server(IP : %s) response error", monitorAddress)}
	}
	defer response.Body.Close()

	// Parse Response
	var monitorResponse MonitorResponse
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&monitorResponse); err != nil {
		tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 2: ", monitorResponse.ErrorMessage)
		outputChannel <- MonitorResponse{ErrorMessage: err.Error()}
	}

	// Check the result
	if monitorResponse.RedisNodeAddress == redisNodeAddress {
		tools.InfoLogger.Println("askMasterIsAlive() : Response - is alive : ", monitorResponse.IsAlive)
		outputChannel <- MonitorResponse{IsAlive: monitorResponse.IsAlive, ErrorMessage: ""}
	} else {
		tools.ErrorLogger.Println("askMasterIsAlive() : Response - err 3: ", monitorResponse.ErrorMessage)
		outputChannel <- MonitorResponse{ErrorMessage: fmt.Sprintf("Reuqested Redis Node Address Not Match with Response")}
	}
}
