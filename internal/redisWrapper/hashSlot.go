package redisWrapper

var clientHashRangeMap map[string][]HashRange

type HashRange struct {
	startIndex uint16
	endIndex   uint16
}

//redistruibuteHashSlot distributes @srcNode's Hash Slots into other Master nodes
// Splits down @srcNode's each Hash Slot evenly with remaining number of master nodes,
// and Append on remaining masters' hash slots
func redistruibuteHashSlot(srcNode RedisClient) {

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

				// Record slave address -> Hash slots Range
				newHashRange := HashRange{
					startIndex: hashSlotStart,
					endIndex:   hashSlotEnd,
				}

				// Record slave address -> Hash slots Range
				clientHashRangeMap[eachMasterNode.Address] = append(clientHashRangeMap[eachMasterNode.Address], newHashRange)
				i++
			}
		}
	}

	// Remove Source Node's Hash Range => Let Garbace Collect
	clientHashRangeMap[srcNode.Address] = nil
	delete(clientHashRangeMap, srcNode.Address)

}
