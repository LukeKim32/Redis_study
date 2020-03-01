package cluster

import (
	"interface_hash_server/internal/cluster/templates"
	"interface_hash_server/tools"
	"time"
)

// @Deprecated
// MasterSlaveChannelMap : 마스터 IP 주소 -> 슬레이브 Client, 마스터-슬레이브간 메세지 교환 채널
var MasterSlaveChannelMap map[string](chan MasterSlaveMessage)

// @Deprecated
// MasterSlaveMessage : 마스터-슬레이브간 메세지 포맷
type MasterSlaveMessage struct {
	MasterNode RedisClient
	SlaveNode  RedisClient
	// isCurrentSlaveDead is the status of current Struct Variable's "SlaveNode" field
	isCurrentSlaveDead bool
}

// StartMonitorNodes : 매 초 Redis Client들의 상태 확인/처리
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

				for _, eachMasterClient := range redisMasterClients {
					go eachMasterClient.handleIfDeadWithLock(errorChannel)
				}

				// fmt.Printf("GetRedisclient 걸린 시간 %v\n", time.Since(start))

			case <-errorChannel:
				ticker.Stop()
				close(errorChannel)
				tools.ErrorLogger.Println(templates.MonitorNodesError)
				return
			}
		}
	}()
}
