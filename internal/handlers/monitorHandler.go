package handlers

import (
	"fmt"
	"interface_hash_server/configs"
	"interface_hash_server/tools"
	"interface_hash_server/internal/redisWrapper"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
	"net/http"
	"encoding/json"
)

// ExceptionHandle handles request of unproper URL
func CheckRedisNodeStatus(response http.ResponseWriter, request *http.Request) {

	pathVariables := mux.Vars(request)
	targetRedisAddress := pathVariables["redisNodeAddress"]
	pingResult := redisWrapper.MonitorResponse{
		RedisNodeAddress : targetRedisAddress,
		Err : nil,
		IsAlive : true,
	}

	redisClient, err := redisWrapper.GetRedisClientWithAddress(targetRedisAddress)
	if err != nil {
		pingResult.Err = err
		pingResult.IsAlive = false
	}

	
	result, err := redis.String(redisClient.Connection.Do("PING"))
	if err != nil {
		tools.ErrorLogger.Printf("Redis Node(%s) Ping test failure\n", redisClient.Address)
		pingResult.Err = err
		pingResult.IsAlive = false
	}

	tools.InfoLogger.Println("CheckRedisNodeStatus() Result : ",result)
	
	response.Header().Set("Content-Type", configs.JSONContent)
	response.WriteHeader(http.StatusOK)

	encodedPingResult, err := json.Marshal(&pingResult)
	if err != nil {
		responseInternalError(response, err, configs.BaseURL)
	}

	fmt.Fprintf(response,string(encodedPingResult))
}