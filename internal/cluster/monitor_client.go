package cluster

import (
	"encoding/json"
	"fmt"
	"interface_hash_server/configs"
	"interface_hash_server/internal/cluster/templates"
	"interface_hash_server/tools"
	"net/http"
	"strings"

	"github.com/gomodule/redigo/redis"
)

// MonitorClient : Monitor Server들에게 요청을 보낼 Client
type MonitorClient struct {
	ServerAddressList []string
}

var monitorClient MonitorClient

// MonitorServerResponse : Monitor Server의 응답 container
type MonitorServerResponse struct {
	RedisNodeAddress string
	IsAlive          bool
	ErrorMessage     string
}

var errorChannel chan error

func init() {
	if len(monitorClient.ServerAddressList) == 0 {
		monitorClient.ServerAddressList = []string{configs.MonitorNodeOneAddress, configs.MonitorNodeTwoAddress}
	}
}

// askAlive : MonitorClient에 등록되어 있는 모니터 서버들에게 @redisNode의 생존 여부 확인 요청
//  고루틴을 이용하여 각 모니터 서버들에게 요청 전송
//
func (monitorClient MonitorClient) askAlive(redisNode RedisClient) (int, error) {

	numberOfmonitorNode := len(monitorClient.ServerAddressList)

	// Buffered Channel : 송신측은 메세지 전송 후 고루틴 계속 진행
	outputChannel := make(chan MonitorServerResponse, numberOfmonitorNode)

	tools.InfoLogger.Println(templates.StartToRequestToMonitors)

	votes := 0

	// Host인 인터페이스 서버의 Ping 테스트 (투표 1)
	hostPingResult, _ := redis.String(redisNode.Connection.Do("PING"))
	if strings.Contains(hostPingResult, "PONG") {
		votes++
	}

	// 각 모니터 서버의 Ping 테스트 요청 (투표 n)
	for _, eachMonitorServer := range monitorClient.ServerAddressList {

		// GET request With URI http://~/monitor/{redisNodeIp}
		go monitorClient.requestToServer(
			outputChannel,
			eachMonitorServer,
			redisNode.Address,
		)
	}

	for i := 0; i < numberOfmonitorNode; i++ {

		tools.InfoLogger.Println(templates.WaitForResponseFromMonitors)

		// 모니터 서버 응답 대기 & 처리
		// To-Do : 모니터 서버 응답이 오지 않는 경우 Timeout 설정
		monitorServerResponse := <-outputChannel
		tools.InfoLogger.Printf(templates.ChannelResponseFromMonitor, monitorServerResponse.ErrorMessage)

		if monitorServerResponse.ErrorMessage != "" {
			return 0, fmt.Errorf(monitorServerResponse.ErrorMessage)
		}

		if monitorServerResponse.IsAlive {
			tools.InfoLogger.Printf(templates.RedisCheckedAlive, redisNode.Address)
			votes++
		} else {
			tools.InfoLogger.Printf(templates.RedisCheckedDead, redisNode.Address)
		}
	}

	return votes, nil
}

// requestToServer : @redisNodeIp 레디스 노드의 생존여부 @monitorServerIp 해당하는 모니터 서버에 확인 요청
//
func (monitorClient MonitorClient) requestToServer(outputChannel chan<- MonitorServerResponse, monitorServerIp string, redisNodeIp string) {

	requestURI := fmt.Sprintf("http://%s/monitor/%s", monitorServerIp, redisNodeIp)

	tools.InfoLogger.Println(templates.RequestTargetMonitor, requestURI)

	// 모니터 서버에 요청
	response, err := http.Get(requestURI)
	if err != nil {
		tools.ErrorLogger.Printf(templates.ResponseMonitorError, monitorServerIp, err)
		outputChannel <- MonitorServerResponse{
			ErrorMessage: fmt.Sprintf(templates.ResponseMonitorError, monitorServerIp, err),
		}
	}
	defer response.Body.Close()

	// 모니터 서버 응답 파싱
	var monitorServerResponse MonitorServerResponse
	decoder := json.NewDecoder(response.Body)

	if err := decoder.Decode(&monitorServerResponse); err != nil {
		tools.ErrorLogger.Println(templates.ResponseMonitorError, monitorServerIp, monitorServerResponse.ErrorMessage)
		outputChannel <- MonitorServerResponse{
			ErrorMessage: err.Error(),
		}
	}

	// 모니터 서버 테스트 결과 확인
	if monitorServerResponse.RedisNodeAddress == redisNodeIp {
		tools.InfoLogger.Println(templates.ResponseFromTargetMonitor, monitorServerResponse.IsAlive)

		outputChannel <- MonitorServerResponse{
			IsAlive:      monitorServerResponse.IsAlive,
			ErrorMessage: "",
		}
	} else {
		tools.ErrorLogger.Println(templates.ResponseMonitorError, monitorServerIp, monitorServerResponse.ErrorMessage)
		outputChannel <- MonitorServerResponse{
			ErrorMessage: fmt.Sprintf(templates.ResponseMonitorError, monitorServerIp, templates.NoMatchingResponseNode),
		}
	}
}
