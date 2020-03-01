package cluster

import (
	"fmt"
	"interface_hash_server/internal/cluster/templates"
	"interface_hash_server/tools"
	"sync"
)

// hashSlot is a map of (Hash Slot -> Redis Node Client)
var hashSlot map[uint16]RedisClient

// redistributeSlotMutex : 해쉬 슬롯 재분배 시 전체 Client Lock
// used for sync in accessing Hash Maps After Redistribution
var redistributeSlotMutex = &sync.Mutex{}

// clientHashRangeMap : Redis Client 주소 -> 담당하는 해쉬 슬롯 구간들
var clientHashRangeMap map[string][]HashRange

type HashRange struct {
	startIndex uint16
	endIndex   uint16
}

// assignHashSlot : 인스턴스에 [@start, @end) 범위에 해당하는 해쉬 슬롯 할당
// Loop unrolling을 이용, 퍼포먼스 최적화
// assigns Hash slots (@start ~ @end) to passed @redisClient
// It basically unrolls the loop with 16 states for cahching
// And If the range Is not divided by 16, Remains will be handled with single statement loop
//
func (redisClient RedisClient) assignHashSlot(start uint16, end uint16) {

	tools.InfoLogger.Printf(templates.HashSlotAssignStart, redisClient.Address)

	var i uint16
	nextSlotIndex := start + 16
	// Replace Hash Map With Slave Client
	for i = start; nextSlotIndex < end; i += 16 {
		hashSlot[i] = redisClient
		hashSlot[i+1] = redisClient
		hashSlot[i+2] = redisClient
		hashSlot[i+3] = redisClient
		hashSlot[i+4] = redisClient
		hashSlot[i+5] = redisClient
		hashSlot[i+6] = redisClient
		hashSlot[i+7] = redisClient
		hashSlot[i+8] = redisClient
		hashSlot[i+9] = redisClient
		hashSlot[i+10] = redisClient
		hashSlot[i+11] = redisClient
		hashSlot[i+12] = redisClient
		hashSlot[i+13] = redisClient
		hashSlot[i+14] = redisClient
		hashSlot[i+15] = redisClient
		nextSlotIndex += 16
	}

	for ; i < end; i++ {
		hashSlot[i] = redisClient
	}

	tools.InfoLogger.Printf(templates.HashSlotAssignFinish, redisClient.Address)

}

//distributeHashSlot : srcClient 인스턴스에게 할당된 해쉬 슬롯을 다른 Redis 마스터 Client 들에게 분배.
//
func (srcClient RedisClient) distributeHashSlot() error {

	tools.InfoLogger.Printf(templates.HashSlotRedistributeStart, srcClient.Address)
	tools.InfoLogger.Printf(templates.DeadRedisNodeInfo, srcClient.Address, srcClient.Role)

	if len(clientHashRangeMap[srcClient.Address]) == 0 {
		return fmt.Errorf(templates.NoHashRangeIsAssigned, srcClient.Address)
	}

	redistributeSlotMutex.Lock()
	defer redistributeSlotMutex.Unlock()

	restOfMasterNumber := len(redisMasterClients) - 1

	if restOfMasterNumber < 1 {
		tools.ErrorLogger.Println("살아있는 Master Node가 없습니다.")

		// To-Do 모두 초기화 처리

		return fmt.Errorf("No Remaining Master Nodes Alive")
	}

	// srcClient가 담당하던 해쉬 슬롯 범위에 대해
	for _, eachHashRangeOfClient := range clientHashRangeMap[srcClient.Address] {

		srcHashSlotStart := eachHashRangeOfClient.startIndex
		srcHashSlotEnd := eachHashRangeOfClient.endIndex
		srcHashSlotRange := srcHashSlotEnd - srcHashSlotStart + 1

		i := 0 // 임의의 마스터 클라이언트 인덱스
		// 다른 마스터에게 해쉬 슬롯 균일 분배
		for _, eachMasterNode := range redisMasterClients {

			if eachMasterNode.Address != srcClient.Address {

				// 소수부 손실을 막기 위한 계산 순서
				// arithmatic order fixed to prevent Mantissa Loss
				normalizedHashSlotStart := uint16(float64(i) / float64(restOfMasterNumber) * float64(srcHashSlotRange))
				normalizedhashSlotEnd := uint16(float64(i+1) / float64(restOfMasterNumber) * float64(srcHashSlotRange))

				hashSlotStart := normalizedHashSlotStart + srcHashSlotStart
				hashSlotEnd := normalizedhashSlotEnd + srcHashSlotStart
				eachMasterNode.assignHashSlot(hashSlotStart, hashSlotEnd)

				newHashRange := HashRange{
					startIndex: hashSlotStart,
					endIndex:   hashSlotEnd,
				}

				clientHashRangeMap[eachMasterNode.Address] = append(clientHashRangeMap[eachMasterNode.Address], newHashRange)
				i++
			}
		}
	}

	// srcClient가 저장하고 있던 데이터 Migration
	if err := srcClient.migrateDataToOthers(); err != nil {
		return err
	}

	if err := srcClient.cleanUpMemory(); err != nil {
		return err
	}

	// Print Current Updated Masters
	PrintCurrentMasterSlaves()

	tools.InfoLogger.Printf(templates.HashSlotRedistributeFinish, srcClient.Address)

	return nil
}

func PrintCurrentMasterSlaves() {

	for _, eachMaster := range redisMasterClients {
		tools.InfoLogger.Printf(templates.RefreshedMasters, eachMaster.Address)
		tools.InfoLogger.Printf("%s의 역할 : %s\n", eachMaster.Address, eachMaster.Role)
	}

	for _, eachSlave := range redisSlaveClients {
		tools.InfoLogger.Printf(templates.RefreshedSlaves, eachSlave.Address)
		tools.InfoLogger.Printf("%s의 역할 : %s\n", eachSlave.Address, eachSlave.Role)
	}
}
