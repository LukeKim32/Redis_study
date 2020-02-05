package redisWrapper

import "interface_hash_server/configs"

// RedisNodeAddressList are Address list of Redis Nodes

func GetInitialMasterAddressList() []string {
	return []string{
		configs.RedisMasterOneAddress,
		configs.RedisMasterTwoAddress,
		configs.RedisMasterThreeAddress,
	}
}

func GetInitialSlaveAddressList() []string {
	return []string{
		configs.RedisSlaveOneAddress,
		configs.RedisSlaveTwoAddress,
		configs.RedisSlaveThreeAddress,
	}
}
