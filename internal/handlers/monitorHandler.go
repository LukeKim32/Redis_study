package handlers

import (
	"fmt"
	"interface_hash_server/configs"
	"interface_hash_server/internal/redisWrapper"
	"interface_hash_server/tools"

	"encoding/json"
	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
	"net/http"
)

// ExceptionHandle handles request of unproper URL
func CheckRedisNodeStatus(response http.ResponseWriter, request *http.Request) {

	pathVariables := mux.Vars(request)
	targetRedisAddress := pathVariables["redisNodeAddress"]
	pingResult := redisWrapper.MonitorResponse{
		RedisNodeAddress: targetRedisAddress,
		ErrorMessage:     "",
		IsAlive:          true,
	}

	redisClient, err := redisWrapper.GetRedisClientWithAddress(targetRedisAddress)
	if err != nil {
		pingResult.ErrorMessage = err.Error()
		pingResult.IsAlive = false
	}

	result, err := redis.String(redisClient.Connection.Do("PING"))
	if err != nil {
		tools.ErrorLogger.Printf("Redis Node(%s) Ping test failure\n", redisClient.Address)
		pingResult.ErrorMessage = err.Error()
		pingResult.IsAlive = false
	}

	tools.InfoLogger.Println("CheckRedisNodeStatus() Result : ", result)

	response.Header().Set("Content-Type", configs.JSONContent)
	response.WriteHeader(http.StatusOK)

	encodedPingResult, err := json.Marshal(pingResult)
	if err != nil {
		responseInternalError(response, err, configs.BaseURL)
	}

	tools.InfoLogger.Println("MonitorHandler Response Data : ", string(encodedPingResult))

	fmt.Fprintf(response, string(encodedPingResult))
}
