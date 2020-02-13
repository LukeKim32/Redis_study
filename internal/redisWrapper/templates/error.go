package templates

const (
	/* Error Log Templates */
	MasterSlaveMapNotInit    = "promoteSlaveToMaster() - Master Slave Map is not set up"
	BothMasterSlaveDead      = "promoteSlaveToMaster() : Both Master and Slave are dead"
	ConnectionFailure        = "Redis Node(%s) connection failure - %s\n"
	RedisMasterNotSetUpYet   = "Redis Master Node should be set up first"
	RedisSlaveNotSetUpYet    = "Redis Slave Node should be set up first"
	NotAnyRedisSetUpYet      = "GetRedisClientWithAddress() error : No Redis Clients set up"
	SlaveNumberMustBeLarger  = "The number of Slave Nodes should be bigger than Master's"
	NoMatchingSlaveToRemove  = "RemoveSlaveFromList() : No Matching Redis Client to Remove - %s"
	NoMatchingMasterToRemove = "RemoveMasterFromList() : No Matching Redis Client to Remove - %s"
	NoMatchingRedisToFind    = "GetRedisClientWithAddress() error : No Matching Redis Client With passed address"
	NoHashRangeIsAssigned    = "redistruibuteHashSlot() : No Hash Range is assigned to Node(%s)"
	MonitorNodesError        = "MonitorMasters() : Error - timer stopped"
	ResponseMonitorError     = "requestToMonitor() :Monitor server(IP : %s) response error : %s\n"
	NoMatchingResponseNode   = "Reuqested Redis Node Address Not Match with Response"
)
