package redisWrapper

import (
	"fmt"
	"interface_hash_server/internal/redisWrapper/templates"
	"interface_hash_server/tools"

	"github.com/gomodule/redigo/redis"
)

var clientHashRangeMap map[string][]HashRange

type HashRange struct {
	startIndex uint16
	endIndex   uint16
}

// assignHashSlotMap assigns Hash slots (@start ~ @end) to passed @redisNode
/* It basically unrolls the loop with 16 states for cahching
 * And If the range Is not divided by 16, Remains will be handled with single statement loop
 */
func assignHashSlotMap(start uint16, end uint16, redisNode RedisClient) {

	tools.InfoLogger.Printf(templates.HashSlotAssignStart, redisNode.Address)

	var i uint16
	nextSlotIndex := start + 16
	// Replace Hash Map With Slave Client
	for i = start; nextSlotIndex < end; i += 16 {
		HashSlotMap[i] = redisNode
		HashSlotMap[i+1] = redisNode
		HashSlotMap[i+2] = redisNode
		HashSlotMap[i+3] = redisNode
		HashSlotMap[i+4] = redisNode
		HashSlotMap[i+5] = redisNode
		HashSlotMap[i+6] = redisNode
		HashSlotMap[i+7] = redisNode
		HashSlotMap[i+8] = redisNode
		HashSlotMap[i+9] = redisNode
		HashSlotMap[i+10] = redisNode
		HashSlotMap[i+11] = redisNode
		HashSlotMap[i+12] = redisNode
		HashSlotMap[i+13] = redisNode
		HashSlotMap[i+14] = redisNode
		HashSlotMap[i+15] = redisNode
		nextSlotIndex += 16
	}

	for ; i < end; i++ {
		HashSlotMap[i] = redisNode
	}

	tools.InfoLogger.Printf(templates.HashSlotAssignFinish, redisNode.Address)

}

//redistruibuteHashSlot distributes @srcNode's Hash Slots into other Master nodes
// Splits down @srcNode's each Hash Slot evenly with remaining number of master nodes,
// and Append on remaining masters' hash slots
func redistruibuteHashSlot(srcNode RedisClient) error {

	tools.InfoLogger.Printf(templates.HashSlotRedistributeStart, srcNode.Address)
	tools.InfoLogger.Printf(templates.DeadRedisNodeInfo, srcNode.Address, srcNode.Role)

	if len(clientHashRangeMap[srcNode.Address]) == 0 {
		return fmt.Errorf(templates.NoHashRangeIsAssigned, srcNode.Address)
	}

	redistributeSlotMutex.Lock()
	defer redistributeSlotMutex.Unlock()

	restOfMasterNumber := len(redisMasterClients) - 1

	// For Each Hash Range Source node is managing
	for _, eachHashRangeOfClient := range clientHashRangeMap[srcNode.Address] {

		srcHashSlotStart := eachHashRangeOfClient.startIndex
		srcHashSlotEnd := eachHashRangeOfClient.endIndex
		srcHashSlotRange := srcHashSlotEnd - srcHashSlotStart + 1

		i := 0 // i is order of remaining master nodes
		// Redisdribute each hash range to other masters
		for _, eachMasterNode := range redisMasterClients {

			if eachMasterNode.Address != srcNode.Address {

				// arithmatic order fixed to prevent Mantissa Loss
				normalizedHashSlotStart := uint16(float64(i) / float64(restOfMasterNumber) * float64(srcHashSlotRange))
				normalizedhashSlotEnd := uint16(float64(i+1) / float64(restOfMasterNumber) * float64(srcHashSlotRange))

				hashSlotStart := normalizedHashSlotStart + srcHashSlotStart
				hashSlotEnd := normalizedhashSlotEnd + srcHashSlotStart
				assignHashSlotMap(hashSlotStart, hashSlotEnd, eachMasterNode)

				newHashRange := HashRange{
					startIndex: hashSlotStart,
					endIndex:   hashSlotEnd,
				}

				clientHashRangeMap[eachMasterNode.Address] = append(clientHashRangeMap[eachMasterNode.Address], newHashRange)
				i++
			}
		}
	}

	// Record @srcNode's data into other nodes
	if err := recordDataToOtherNodes(srcNode); err != nil {
		return err
	}

	// Remove Source Node's Hash Range => Let Garbace Collect
	clientHashRangeMap[srcNode.Address] = nil
	delete(clientHashRangeMap, srcNode.Address)

	slaveNode := masterSlaveMap[srcNode.Address]

	if _, err := RemoveMasterFromList(srcNode); err != nil {
		return err
	}

	if _, err := RemoveSlaveFromList(slaveNode); err != nil {
		return err
	}
	delete(MasterSlaveChannelMap, srcNode.Address)
	delete(masterSlaveMap, srcNode.Address)
	delete(slaveMasterMap, slaveNode.Address)

	for _, eachMaster := range redisMasterClients {
		tools.InfoLogger.Println(templates.RefreshedMasters, eachMaster.Address)
	}
	for _, eachSlave := range redisSlaveClients {
		tools.InfoLogger.Println(templates.RefreshedSlaves, eachSlave.Address)
	}
	tools.InfoLogger.Printf(templates.HashSlotRedistributeFinish, srcNode.Address)

	return nil
}

// recordDataToOtherNodes reads @srcNodes's data logs and records to other appropriate nodes
func recordDataToOtherNodes(srcNode RedisClient) error {

	isSrcNodeAlive := true // Hash Slot 재분배 도중 @srcNode가 죽을 경우를 처리하기 위한 플래그

	// Read source node's data log file
	hashIndexToLogFormatMap := make(map[uint16][]logFormat)
	if err := readDataLogs(srcNode.Address, hashIndexToLogFormatMap); err != nil {
		return err
	}

	// Record source node's data into other appropriate nodes
	for hashIndex, dataLogListOfHashIndex := range hashIndexToLogFormatMap {
		redisClient := HashSlotMap[hashIndex]

		for _, eachDataLog := range dataLogListOfHashIndex {

			if isSrcNodeAlive {

				if _, err := redis.String(redisClient.Connection.Do(eachDataLog.Command, eachDataLog.Key, eachDataLog.Value)); err != nil {

					// 명령이 실패한 경우 - redistruibuteHashSlot() Hash Slot 할당 과정에서 죽었을 수도 있으므로
					numberOfTotalVotes := len(monitorNodeAddressList) + 1
					votes, err := askRedisIsAliveToMonitors(srcNode)
					if err != nil {
						return fmt.Errorf("recordDataToOtherNodes() : ask Redis Node is alive failed - %s", err.Error())
					}

					tools.InfoLogger.Printf(templates.FailOverVoteResult, srcNode.Address, votes, numberOfTotalVotes)

					if votes > (numberOfTotalVotes / 2) {
						// Retry the command ignoring if error exists
						redisClient.Connection.Do(eachDataLog.Command, eachDataLog.Key, eachDataLog.Value)

					} else { // 죽었다고 판단할 경우, 명령어 수행은 그만하되 Log file에는 기록을 해놓는다
						isSrcNodeAlive = false
					}
				}
			}

			// Save Modification
			if err := RecordModification(redisClient.Address, eachDataLog.Command, eachDataLog.Key, eachDataLog.Value); err != nil {
				return fmt.Errorf("redistruibuteHashSlot() : logging for recording dead node(%s)'s data failed", srcNode.Address)
			}
		}
	}

	return nil

}
