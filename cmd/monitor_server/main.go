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

	tools.SetUpLogger("monitor_server")

	// To Check Load Balancing By Proxy
	if configs.CurrentIP, err = tools.GetCurrentServerIP(); err != nil {
		tools.ErrorLogger.Fatalln("Error - Get Go-App IP error : ", err.Error())
	}

	// Redis Master Containers들과 Connection설정
	if err := redisWrapper.NodeConnectionSetup(configs.GetInitialMasterAddressList(), redisWrapper.Default); err != nil {
		tools.ErrorLogger.Fatalln("Error - Node connection error : ", err.Error())
	}

	// Redis Slave Containers들과 Connection설정
	if err := redisWrapper.NodeConnectionSetup(configs.GetInitialSlaveAddressList(), redisWrapper.SlaveSetup); err != nil {
		tools.ErrorLogger.Fatalln("Error - Node connection error : ", err.Error())
	}

	router := mux.NewRouter()

	// Interface Server Router 설정
	moniterRouter := router.PathPrefix("/monitor").Subrouter()
	moniterRouter.HandleFunc("/{redisNodeAddress}", handlers.CheckRedisNodeStatus).Methods(http.MethodGet)

	// 허용하지 않는 URL 경로 처리
	router.PathPrefix("/").HandlerFunc(handlers.ExceptionHandle)
	http.Handle("/", router)

	tools.InfoLogger.Println("Server start listening on port ", configs.Port)

	tools.ErrorLogger.Fatal(http.ListenAndServe(":"+strconv.Itoa(configs.Port), router))
}
