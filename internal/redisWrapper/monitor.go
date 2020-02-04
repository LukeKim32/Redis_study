package redisWrapper

import (
	"interface_hash_server/tools"
	"time"
	"interface_hash_server/configs"
)

var monitorNodeAddressList = []string{configs.MonitorNodeOneAddress, configs.MonitorNodeTwoAddress}

type MonitorResponse struct {
	RedisNodeAddress string
	IsAlive          bool
	ErrorMessage     string
}

type MonitorResult struct {
	RedisAddress string
	isAlive bool
}

// MasterSlaveChannelMap is a map (Master Node Address -> Channel) for Master And Slave Nodes' Communication
var MasterSlaveChannelMap map[string](chan MonitorResult)

func MonitorMasters() {
	ticker := time.NewTicker(5 * time.Second)
	quit := make(chan error)
	go func() {
		for {
		   select {
			case <- ticker.C:

				for _, eachMasterNode := range redisMasterClients {
					votes, err := askRedisIsAliveToMonitors(eachMasterNode.Address)
					if err != nil {
						quit <- err
						break;
					}

					numberOfTotalVotes := len(monitorNodeAddressList) + 1
					if votes > (numberOfTotalVotes / 2) { // If more than half says it's not dead
						// setupConnectionAgain()
						MasterSlaveChannelMap[eachMasterNode.Address] <- MonitorResult{RedisAddress : eachMasterNode.Address, isAlive : true}
					} else {
						// if 과반수 says dead,
						// Switch Slave to Master. (Slave까지 죽은것에 대해선 Redis Cluster도 처리 X => Docker restart로 처리해보자)
						if err := promoteSlaveToMaster(MasterSlaveMap[eachMasterNode]); err != nil {
							// Slave까지 죽었다면
							// 다른 Master가 Hash slot
							quit <- err
							break;
						}

						MasterSlaveChannelMap[eachMasterNode.Address] <- MonitorResult{RedisAddress : eachMasterNode.Address, isAlive : false}
					}
				}
				
			case <- quit:
				ticker.Stop()
				close(quit)
				tools.ErrorLogger.Println("MonitorMasters() : Error - timer stopped",)
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
			case <- ticker.C:

				for _, eachSlaveNode := range redisSlaveClients {
					votes, err := askRedisIsAliveToMonitors(eachSlaveNode.Address)
					if err != nil {
						quit <- err
						break;
					}

					numberOfTotalVotes := len(monitorNodeAddressList) + 1
					if votes > (numberOfTotalVotes / 2) { // If more than half says it's not dead
						// setupConnectionAgain()
					} else {
						// if 과반수 says dead,
						masterNode := SlaveMasterMap[eachSlaveNode]
						monitorResult := <- MasterSlaveChannelMap[masterNode.Address]
	
						if monitorResult.isAlive {
							if err := RestartRedisContainer(eachSlaveNode.Address); err != nil {
								quit <- err
								break;
							}
						} else {
							// Master까지 죽었다면
							// 다른 Master가 Hash slot
						}
					}
				}
				
			case <- quit:
				ticker.Stop()
				close(quit)
				tools.ErrorLogger.Println("MonitorMasters() : Error - timer stopped",)
				return
			}
		}
	 }()

}