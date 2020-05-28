package main

import (
	"net/http"
	"strconv"

	"hash_interface/configs"
	"hash_interface/internal/cluster"
	"hash_interface/internal/handlers"
	"hash_interface/internal/routers"
	"hash_interface/tools"

	_ "hash_interface/docs"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
)

// @title Redis-Cluster/Sentinel-Like Server
// @version 1.1
// @description This service presents group of Redis nodes and the interface of them
// @description to test master/slave replication, data persistence, failover redemption

// @contact.name 김예찬 (Luke Kim)
// @contact.email burgund32@gmail.com

// @host localhost:8888
// @BasePath /api/v1
func main() {

	var err error

	tools.SetUpLogger("hash_server")

	// To Check Load Balancing By Proxy
	if configs.CurrentIP, err = tools.GetCurrentServerIP(); err != nil {
		tools.ErrorLogger.Fatalln(
			"Error - Get Go-App IP error : ",
			err.Error(),
		)
	}

	// Redis Master Containers들과 Connection설정
	err = cluster.NodeConnectionSetup(
		configs.GetInitialMasterAddressList(),
		cluster.Default,
	)
	if err != nil {
		tools.ErrorLogger.Fatalln(
			"Error - Node connection error : ",
			err.Error(),
		)
	}

	// create Hash Map (Index -> Redis Master Nodes)
	if err := cluster.MakeHashMapToRedis(); err != nil {
		tools.ErrorLogger.Fatalln(
			"Error - Redis Node Address Mapping to Hash Map failure: ",
			err.Error(),
		)
	}

	// Redis Slave Containers들과 Connection설정
	err = cluster.NodeConnectionSetup(
		configs.GetInitialSlaveAddressList(),
		cluster.InitSlaveSetup,
	)
	if err != nil {
		tools.ErrorLogger.Fatalln(
			"Error - Node connection error : ",
			err.Error(),
		)
	}

	cluster.PrintCurrentMasterSlaves()

	/* Set Data modification Logger for each Nodes*/
	cluster.SetUpModificationLogger(
		configs.GetInitialTotalAddressList(),
	)

	// 타이머로 Redis Node들 모니터링 시작
	// cluster.StartMonitorNodes()

	router := mux.NewRouter()

	router.PathPrefix("/api/v1/docs/").
		Handler(httpSwagger.WrapHandler)

	// Interface Server Router 설정
	routers.SetUpInterfaceRouter(router)

	// 허용하지 않는 URL 경로 처리
	router.PathPrefix("/").HandlerFunc(handlers.ExceptionHandle)
	http.Handle("/", router)

	tools.InfoLogger.Println("Server start listening on port ", configs.Port)

	tools.ErrorLogger.Fatal(
		http.ListenAndServe(":"+strconv.Itoa(configs.Port), router),
	)
}
