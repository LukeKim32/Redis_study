package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"interface_hash_server/configs"
	"interface_hash_server/internal/hash"
	"interface_hash_server/internal/redisWrapper"
	"interface_hash_server/tools"

	"github.com/gomodule/redigo/redis"
)

type requestContainer struct {
	Command   string
	Arguments []string
}

// ForwardToProperNode is a handler function for @POST, processing the reqeust
/* 1) Request Body에서 Key 값을 추출
 * 2) Hash(Key) => HashSlot Index
 * 3) NodeAddressMap[HashSlot Index] 위치의 Redis 노드에 Request 받은 명령 전달
 * 4) Redis 노드의 Response 받아 클라이언트한테 전달
 */
func ForwardToProperNode(response http.ResponseWriter, request *http.Request) {

	// To check if load balancing(Round-robin) works
	tools.InfoLogger.Printf("Interface server(IP : %s) Processing...\n", configs.CurrentIP)

	// Decode Request Body
	var requestContainer requestContainer
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(&requestContainer); err != nil {
		responseInternalError(response, err, configs.BaseURL)
		return
	}

	// Check if request format is kept
	if err := isRequestProper(requestContainer); err != nil {
		responseInternalError(response, err, configs.BaseURL)
		return
	}

	key := requestContainer.Arguments[0]
	hashSlotIndex := hash.GetHashSlotIndex(key)

	// Get Redis Node which handles this hash slot
	redisClient, err := redisWrapper.GetRedisClient(hashSlotIndex)
	if err != nil {
		responseInternalError(response, err, configs.BaseURL)
		return
	}

	var redisResponse string
	curTaskMessage := fmt.Sprintf("%s %s ", requestContainer.Command, key)

	switch requestContainer.Command {
	case "GET":
		// Pass Request to Redis Node
		redisResponse, err = redis.String(redisClient.Connection.Do("GET", key))
		if err == redis.ErrNil {
			redisResponse = "(nil)"
		} else if err != nil {
			responseInternalError(response, err, configs.BaseURL)
			return
		}

	case "SET": // Data modification
		value := requestContainer.Arguments[1]
		redisResponse, err = redis.String(redisClient.Connection.Do("SET", key, value))
		if err == redis.ErrNil {
			redisResponse = "(nil)"
		} else if err != nil {
			responseInternalError(response, err, configs.BaseURL)
			return
		}

		// Propagate to Slave node as Data has been modified
		// same operation to master's slave node
		redisWrapper.ReplicateToSlave(redisClient, "SET", key, value)

		curTaskMessage += fmt.Sprintf("%s ", value)
	}

	responseWithRedisResult(response, curTaskMessage, redisResponse, redisClient.Address)

}

// isRequestProper checks Requested 1) Command, 2) the number of Arguments
func isRequestProper(requestContainer requestContainer) error {
	argumentNumber := len(requestContainer.Arguments)

	switch requestContainer.Command {
	case "GET":
		if argumentNumber != 1 {
			return fmt.Errorf("Argument Error - GET command can only have a single argument")
		}
	case "SET":
		if argumentNumber != 2 {
			return fmt.Errorf("Argument Error - SET command can only have two arguments")
		}
	default:
		return fmt.Errorf("Command Error - Only GET, SET commands are allowed")
	}

	return nil
}

// responseWithRedisResult sends response back to Client
// With the result of Redis Node Communication
func responseWithRedisResult(response http.ResponseWriter, curTaskMessage string, redisResponse string, nodeAddress string) {

	response.Header().Set(configs.ContentType, configs.JSONContent)
	response.Header().Set(configs.CORSheader, "*")

	curTaskMessage += fmt.Sprintf("Success : Handled in Server(IP : %s)", configs.CurrentIP)
	nextTaskMessage := "Main URL"
	nextTaskHyperLink := configs.HTTP + configs.BaseURL

	response.WriteHeader(http.StatusOK)
	fmt.Fprintf(response,
		tools.RedisResponseTemplate,
		curTaskMessage,             /* Main Message */
		redisResponse, nodeAddress, /* Redis data JSON*/
		nextTaskMessage, nextTaskHyperLink, /* Next Status */
	)
	tools.InfoLogger.Println(curTaskMessage, "Successful")
}
