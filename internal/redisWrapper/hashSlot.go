package redisWrapper

import (
	"fmt"
	"interface_hash_server/internal/redisWrapper/templates"
	"interface_hash_server/tools"
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

	// Migrate @srcNode's data into other nodes
	if err := migrateDataToOtherNodes(srcNode); err != nil {
		return err
	}

	// Remove Source Node's Hash Range => Let Garbace Collect
	if err := cleanUpDeadMasterSlave(srcNode); err != nil {
		return err
	}

	// Print Current Updated Masters
	for _, eachMaster := range redisMasterClients {
		tools.InfoLogger.Println(templates.RefreshedMasters, eachMaster.Address)
	}
	// Print Current Updated Slaves
	for _, eachSlave := range redisSlaveClients {
		tools.InfoLogger.Println(templates.RefreshedSlaves, eachSlave.Address)
	}
	tools.InfoLogger.Printf(templates.HashSlotRedistributeFinish, srcNode.Address)

	return nil
}

func cleanUpDeadMasterSlave(masterNode RedisClient) error {

	clientHashRangeMap[masterNode.Address] = nil
	delete(clientHashRangeMap, masterNode.Address)

	slaveNode := masterSlaveMap[masterNode.Address]

	if _, err := RemoveMasterFromList(masterNode); err != nil {
		return err
	}

	if _, err := RemoveSlaveFromList(slaveNode); err != nil {
		return err
	}
	delete(MasterSlaveChannelMap, masterNode.Address)
	delete(masterSlaveMap, masterNode.Address)
	delete(slaveMasterMap, slaveNode.Address)

	return nil
}
