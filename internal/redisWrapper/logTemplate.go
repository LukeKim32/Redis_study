package redisWrapper

const (
	FailOverVoteResult    = "GetRedisClient() : %s failover Vote result : (%d / %d)\n"
	MasterSlaveMapNotInit = "promoteSlaveToMaster() - Master Slave Map is not set up"
	BothMasterSlaveDead   = "promoteSlaveToMaster() : Both Master and Slave are dead"
	PromotingSlaveNode    = "promoteSlaveToMaster() : Slave Node(%s) is succeessing..\n"
)
