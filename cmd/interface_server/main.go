package main

import (
	"net/http"
	"strconv"

	"interface_hash_server/configs"
	"interface_hash_server/internal/handlers"
	"interface_hash_server/internal/redisWrapper"
	"interface_hash_server/tools"

	"github.com/gorilla/mux"
)

// @title Redis Cluster Interface Test Server
// @version 1.0

// @contact.name 김예찬
// @contact.email burgund32@gmail.com
// @BasePath /api/v1/projects
func main() {

	var err error

	tools.SetUpLogger(tools.LogDirectory)

	// To Check Load Balancing By Proxy
	if configs.CurrentIP, err = tools.GetCurrentServerIP(); err != nil {
		tools.ErrorLogger.Fatalln("Error - Get Go-App IP error : ", err.Error())
	}

	// Redis Master Containers들과 Connection설정
	if err := redisWrapper.NodeConnectionSetup(redisWrapper.GetInitialMasterAddressList(), redisWrapper.Default); err != nil {
		tools.ErrorLogger.Fatalln("Error - Node connection error : ", err.Error())
	}

	// create Hash Map (Index -> Redis Node)
	if err := redisWrapper.MakeRedisAddressHashMap(); err != nil {
		tools.ErrorLogger.Fatalln("Error - Redis Node Address Mapping to Hash Map failure: ", err.Error())
	}

	// Redis Slave Containers들과 Connection설정
	if err := redisWrapper.NodeConnectionSetup(redisWrapper.GetInitialSlaveAddressList(), redisWrapper.SlaveSetup); err != nil {
		tools.ErrorLogger.Fatalln("Error - Node connection error : ", err.Error())
	}

	redisWrapper.MonitorMasters()
	redisWrapper.MonitorSlaves()

	router := mux.NewRouter()

	// Interface Server Router 설정
	interfaceRouter := router.PathPrefix("/interface").Subrouter()
	interfaceRouter.HandleFunc("", handlers.ForwardToProperNode).Methods(http.MethodPost)

	// 허용하지 않는 URL 경로 처리
	router.PathPrefix("/").HandlerFunc(handlers.ExceptionHandle)
	http.Handle("/", router)

	tools.InfoLogger.Println("Server start listening on port ", configs.Port)

	tools.ErrorLogger.Fatal(http.ListenAndServe(":"+strconv.Itoa(configs.Port), router))
}
