package redisWrapper

import "interface_hash_server/configs"

var monitorNodeAddressList = []string{configs.MonitorNodeOneAddress, configs.MonitorNodeTwoAddress}

type MonitorResponse struct {
	RedisNodeAddress string
	IsAlive   bool
	Err error
}