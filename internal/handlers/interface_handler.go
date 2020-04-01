package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"hash_interface/configs"
	"hash_interface/internal/cluster"
	"hash_interface/internal/hash"
	"hash_interface/internal/models"
	"hash_interface/internal/models/response"
	"hash_interface/tools"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/mux"
)

// SetKeyValue is a handler function for @POST, processing the reqeust
//  1) Request Body에서 Key 값을 추출
//  2) Hash(Key) => hashSlot Index
//  3) NodeAddressMap[hashSlot Index] 위치의 Redis 노드에 Request 받은 명령 전달
//  4) Redis 노드의 Response 받아 클라이언트한테 전달
//
func SetKeyValue(res http.ResponseWriter, req *http.Request) {

	// To check if load balancing(Round-robin) works
	tools.InfoLogger.Printf("Interface server(IP : %s) Processing...\n", configs.CurrentIP)

	// 요청 Body 파싱
	var DataRequestContainer models.DataRequestContainer
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&DataRequestContainer); err != nil {
		responseError(res, http.StatusInternalServerError, err)
		return
	}

	var responseTemplate response.SetResultTemplate
	responseTemplate.Results = make([]response.RedisResult, len(DataRequestContainer.Data))

	// 각 Set 요청 Key, Value
	for i, eachKeyValue := range DataRequestContainer.Data {

		key := eachKeyValue.Key
		value := eachKeyValue.Value
		hashSlotIndex := hash.GetHashSlotIndex(key)

		tools.InfoLogger.Printf(
			"SET Key : %s, Value : %s - 해쉬 슬롯 : %d",
			key,
			value,
			hashSlotIndex,
		)

		// Key의 해쉬 슬롯을 담당하는 레디스 획득
		// Get Redis Node which handles this hash slot
		redisClient, err := cluster.GetRedisClient(hashSlotIndex)
		if err != nil {
			responseError(res, http.StatusInternalServerError, err)
			return
		}

		// 레디스에 요청 명령 실행
		_, err = redis.String(redisClient.Connection.Do("SET", key, value))
		if err != nil {
			responseError(res, http.StatusInternalServerError, err)
			return
		}

		// 변경사항 데이터 로그 기록
		err = redisClient.RecordModificationLog("SET", key, value)
		if err != nil {
			responseError(res, http.StatusInternalServerError, err)
			return
		}

		// 슬레이브에게 전파
		redisClient.ReplicateToSlave("SET", key, value)

		responseTemplate.Results[i].NodeAdrress = redisClient.Address
		responseTemplate.Results[i].Result = fmt.Sprintf(
			"%s %s %s",
			"SET",
			key,
			value,
		)
	}

	curMsg := fmt.Sprintf(
		"SET completed Success : Handled in Server(IP : %s)",
		configs.CurrentIP,
	)
	nextMsg := "Main URL"
	nextLink := configs.HTTP + configs.BaseURL

	responseBody, err := responseTemplate.Marshal(curMsg, nextMsg, nextLink)
	if err != nil {
		tools.ErrorLogger.Println(err.Error())
		return
	}

	responseOK(res, responseBody)
}

// GetValueFromKey is a handler function for @GET, processing the reqeust
// URI로 전달받은 Key값을 가져온다.
//
func GetValueFromKey(res http.ResponseWriter, req *http.Request) {

	// To check if load balancing(Round-robin) works
	tools.InfoLogger.Printf("Interface server(IP : %s) Processing...\n", configs.CurrentIP)

	params := mux.Vars(req)
	key := params["key"]
	hashSlotIndex := hash.GetHashSlotIndex(key)

	// Key의 해쉬 슬롯을 담당하는 레디스 획득
	redisClient, err := cluster.GetRedisClient(hashSlotIndex)
	if err != nil {
		responseError(res, http.StatusInternalServerError, err)
		return
	}

	// 레디스에 요청 명령 실행
	redisResponse, err := redis.String(redisClient.Connection.Do("GET", key))
	if err == redis.ErrNil {
		redisResponse = "nil(없음)"

	} else if err != nil {
		responseError(res, http.StatusInternalServerError, err)
		return
	}

	curMsg := fmt.Sprintf(
		"GET %s completed Success : Handled in Server(IP : %s)",
		key,
		configs.CurrentIP,
	)
	nextMsg := "Main URL"
	nextLink := configs.HTTP + configs.BaseURL

	responseTemplate := response.GetResultTemplate{}
	responseTemplate.Result = redisResponse
	responseTemplate.NodeAdrress = redisClient.Address

	responseBody, err := responseTemplate.Marshal(curMsg, nextMsg, nextLink)
	if err != nil {
		responseError(res, http.StatusInternalServerError, err)
		return
	}

	responseOK(res, responseBody)
}

// AddNewClient is a handler function for @GET, processing the reqeust
// URI로 전달받은 Key값을 가져온다.
//
func AddNewClient(res http.ResponseWriter, req *http.Request) {

	// To check if load balancing(Round-robin) works
	tools.InfoLogger.Printf("Interface server(IP : %s) Processing...\n", configs.CurrentIP)

	// 요청 Body 파싱
	var newClientRequest models.NewClientRequestContainer
	decoder := json.NewDecoder(req.Body)
	if err := decoder.Decode(&newClientRequest); err != nil {
		responseError(res, http.StatusInternalServerError, err)
		return
	}

	// 요청 오류 체크
	if newClientRequest.IsEmpty() {
		err := fmt.Errorf("AddNewClient() : request body of 'client' is empty")
		responseError(res, http.StatusBadRequest, err)
		return
	}

	switch newClientRequest.Role {
	case cluster.MasterRole:
		err := cluster.AddNewMaster(newClientRequest.Address)
		if err != nil {
			responseError(res, http.StatusInternalServerError, err)
			return
		}

	case cluster.SlaveRole:

		// 타겟 마스터 확인
		if newClientRequest.MasterAddress == "" {
			err := fmt.Errorf("마스터 주소 없음")
			tools.ErrorLogger.Printf(
				"AddNewClient() : 슬레이브 추가 에러 - %s",
				err.Error(),
			)
			responseError(res, http.StatusBadRequest, err)
			return
		}

		targetMaster, err := cluster.GetMasterWithAddress(newClientRequest.MasterAddress)
		if err != nil {
			tools.ErrorLogger.Printf(
				"AddNewClient() : 슬레이브 추가 에러 - %s",
				err.Error(),
			)
			responseError(res, http.StatusBadRequest, err)
			return
		}

		err = cluster.AddNewSlave(newClientRequest.Address, *targetMaster)
		if err != nil {
			tools.ErrorLogger.Printf(
				"AddNewClient() : 슬레이브 추가 에러 - %s",
				err.Error(),
			)
			responseError(res, http.StatusInternalServerError, err)
			return
		}

	default:
		err := fmt.Errorf("AddNewClient() : 지원하지 않는 %s role", newClientRequest.Role)
		tools.ErrorLogger.Printf(err.Error())
		responseError(res, http.StatusBadRequest, err)
		return
	}

	responseTemplate := response.RedisListTemplate{
		Masters: cluster.GetMasterClients(),
		Slaves:  cluster.GetSlaveClients(),
	}

	curMsg := fmt.Sprintf(
		"신규 레디스(%s) (역할 : %s) 등록 성공",
		newClientRequest.Address,
		newClientRequest.Role,
	)
	nextMsg := "Main URL"
	nextLink := configs.HTTP + configs.BaseURL

	// JSON marshaling(Encoding to Bytes)
	responseBody, err := responseTemplate.Marshal(curMsg, nextMsg, nextLink)
	if err != nil {
		tools.ErrorLogger.Println(err.Error())
		return
	}

	responseOK(res, responseBody)
}

// GetClients is a handler function for @GET, processing the reqeust
// URI로 전달받은 Key값을 가져온다.
//
func GetClients(res http.ResponseWriter, req *http.Request) {

	// To check if load balancing(Round-robin) works
	tools.InfoLogger.Printf("Interface server(IP : %s) Processing...\n", configs.CurrentIP)

	responseTemplate := response.RedisListTemplate{
		Masters: cluster.GetMasterClients(),
		Slaves:  cluster.GetSlaveClients(),
	}

	curMsg := fmt.Sprintf(
		"현재 레디스 클라이언트",
	)
	nextMsg := "Main URL"
	nextLink := configs.HTTP + configs.BaseURL

	// JSON marshaling(Encoding to Bytes)
	responseBody, err := responseTemplate.Marshal(curMsg, nextMsg, nextLink)
	if err != nil {
		tools.ErrorLogger.Println(err.Error())
		return
	}

	responseOK(res, responseBody)

}
