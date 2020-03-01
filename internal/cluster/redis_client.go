package cluster

import (
	"fmt"
	"interface_hash_server/internal/cluster/templates"
	"interface_hash_server/tools"
	"sync"

	"github.com/gomodule/redigo/redis"
)

const (
	MasterRole = "master"
	SlaveRole  = "slave"
)

type RedisClient struct {
	Connection redis.Conn
	Address    string
	Role       string
}

var redisMasterClients []RedisClient
var redisSlaveClients []RedisClient

// masterSlaveMap : 마스터 주소 -> 슬레이브 노드
var masterSlaveMap map[string]RedisClient

// slaveMasterMap : 슬레이브 주소 -> 마스터 노드
var slaveMasterMap map[string]RedisClient

// redisMutexMap : IP주소 -> 뮤텍스, 각 마스터-슬레이브 그룹 별 요청 동기화용
// used for sync of request to each Redis <Master-Slave> Set With key of "Address"
var redisMutexMap map[string]*sync.Mutex

/************ Master Client Method ********************************/

// handleIfDead : 인스턴스의 생존여부를 확인하고, failover 발생 시 처리
//  1. masterClient 인스턴스가 살아있는지 확인
//  2. 죽었을 경우
//   1) 매핑된 Slave를 새로운 마스터로 승격
//   2) 죽은 masterClient는 재시작
//  3. 새로운 마스터 승격이 실패할 경우 (Slave 죽은 것으로 판단) 남은 Master Client들에게 해쉬슬롯 재분배
//
func (masterClient RedisClient) handleIfDead() error {

	if masterClient.Role != MasterRole {
		return fmt.Errorf("handleIfDead() Error : Method is only allowed to Master")
	}

	// 모니터 서버에게 생존 확인 요청
	numberOfTotalVotes := len(monitorClient.ServerAddressList) + 1
	votes, err := monitorClient.askAlive(masterClient)
	if err != nil {
		return err
	}

	tools.InfoLogger.Printf(templates.FailOverVoteResult, masterClient.Address, votes, numberOfTotalVotes)

	if votes > (numberOfTotalVotes / 2) {
		return nil
	}

	// 과반수 이상 죽었다고 판단한 경우
	tools.InfoLogger.Printf(templates.PromotinSlaveStart, masterClient.Address)

	slaveClient, isSet := masterSlaveMap[masterClient.Address]
	if isSet == false {
		return fmt.Errorf(templates.MasterSlaveMapNotInit)
	}

	if err := slaveClient.promoteToMaster(); err != nil {

		if err.Error() == templates.BothMasterSlaveDead {
			if err := masterClient.distributeHashSlot(); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	// 새로운 마스터로 승격이 성공한 경우
	// 죽은 기존 마스터는 재시작 (using docker API)

	tools.InfoLogger.Printf("handleIfDead() : Promotion Success, New Slave(Previous Master %s) container restart \n", masterClient.Address)
	if err := docker.restartRedisContainer(masterClient.Address); err != nil {
		return err
	}

	tools.InfoLogger.Printf(templates.SlaveAsNewMaster, slaveMasterMap[masterClient.Address].Address)

	return nil
}

// getSlave : 자신의 슬레이브가 살아있을 경우 반환, 죽어있을 경우 빈 인스턴스 반환
// To-Do : 1-1 매핑이 아닌, 슬레이브가 여러 대일 경우 처리
//
func (masterClient RedisClient) getSlave() (RedisClient, error) {

	slaveClient, isSet := masterSlaveMap[masterClient.Address]
	if isSet == false {
		return RedisClient{}, fmt.Errorf("")
	}

	// 모니터 서버에 슬레이브 생존 여부 확인
	numberOfTotalVotes := len(monitorClient.ServerAddressList) + 1
	votes, err := monitorClient.askAlive(slaveClient)
	if err != nil {
		return RedisClient{}, fmt.Errorf("")
	}

	tools.InfoLogger.Printf(templates.FailOverVoteResult, slaveClient.Address, votes, numberOfTotalVotes)

	// 과반수 이상이 죽었다고 판단
	if votes < (numberOfTotalVotes / 2) {
		return RedisClient{}, fmt.Errorf("")
	}

	return slaveClient, nil
}

// handleIfDeadWithLock : Monitor 루틴에 사용되는 메소드
// handleIfDead() 의 확장으로 Lock을 이용한다
//
func (masterClient RedisClient) handleIfDeadWithLock(errorChannel chan error) {

	redisMutexMap[masterClient.Address].Lock()
	defer redisMutexMap[masterClient.Address].Unlock()

	// Redis Node can be discarded from Master nodes if redistribute happens
	if masterClient.Role != MasterRole {
		return
	}

	if err := masterClient.handleIfDead(); err != nil {
		errorChannel <- err
		return
	}
}

/************ Slave Client Method ********************************/

// promoteToMaster : slaveClient 인스턴스 생존 여부 확인, 새로운 마스터로 승격
//  1. 기존 마스터가 담당하던 해쉬 슬롯 할당
//  2. 마스터, 슬레이브 관련 변수 초기화
//
func (slaveClient RedisClient) promoteToMaster() error {

	// 슬레이브가 살아있는지 확인
	numberOfTotalVotes := len(monitorClient.ServerAddressList) + 1
	votes, err := monitorClient.askAlive(slaveClient)
	if err != nil {
		return err
	}

	tools.InfoLogger.Printf(templates.FailOverVoteResult, slaveClient.Address, votes, numberOfTotalVotes)

	// 과반수 이상이 죽었다고 판단한 경우
	if votes <= (numberOfTotalVotes / 2) {
		return fmt.Errorf(templates.BothMasterSlaveDead)
	}

	tools.InfoLogger.Printf(templates.PromotingSlaveNode, slaveClient.Address)

	masterClient, isSet := slaveMasterMap[slaveClient.Address]
	if isSet == false {
		return fmt.Errorf("promoteToMaster() : slaveMasterMap not init")
	}

	// 새로운 마스터로 승격 시작
	// 1. 기존 마스터와, 새로운 마스터 간 설정 스왑
	if err := swapMasterSlaveConfigs(&masterClient, &slaveClient); err != nil {
		return err
	}

	// 2. 기존 마스터의 해쉬 슬롯을 이어 받음
	for _, eachHashRangeOfClient := range clientHashRangeMap[masterClient.Address] {
		hashSlotStart := eachHashRangeOfClient.startIndex
		hashSlotEnd := eachHashRangeOfClient.endIndex

		// 2-1. 해쉬 맵 업데이트
		slaveClient.assignHashSlot(hashSlotStart, hashSlotEnd)

		newHashRange := HashRange{
			startIndex: hashSlotStart,
			endIndex:   hashSlotEnd,
		}

		// 2-2. 담당하는 해쉬 슬롯 범위에 추가
		clientHashRangeMap[slaveClient.Address] = append(clientHashRangeMap[slaveClient.Address], newHashRange)
	}

	// 기존 마스터의 해쉬 슬롯 범위 제거 (Garbage Collect)
	clientHashRangeMap[masterClient.Address] = nil
	delete(clientHashRangeMap, masterClient.Address)

	return nil
}

/************ General Method ********************************/

func (redisClient RedisClient) removeFromList() error {

	tools.InfoLogger.Printf(templates.SlaveNodeInfo, redisClient.Role, redisClient.Address)

	switch redisClient.Role {
	case MasterRole:
		for i, eachClient := range redisMasterClients {
			if eachClient.Address == redisClient.Address {

				redisMasterClients[i] = redisMasterClients[len(redisMasterClients)-1]
				redisMasterClients[len(redisMasterClients)-1] = RedisClient{}

				redisMasterClients = redisMasterClients[:len(redisMasterClients)-1]

				return nil
			}
		}
	case SlaveRole:
		for i, eachClient := range redisSlaveClients {
			if eachClient.Address == redisClient.Address {

				redisSlaveClients[i] = redisSlaveClients[len(redisSlaveClients)-1]
				redisSlaveClients[len(redisSlaveClients)-1] = RedisClient{}

				redisSlaveClients = redisSlaveClients[:len(redisSlaveClients)-1]

				return nil
			}
		}
	}

	return fmt.Errorf("removeFromList() : Redis Client(%s) Role has not been set!", redisClient.Address)

}

// cleanUpMemory : masterClient 인스턴스와 이에 매핑된 Slave Client 의 메모리 해제
// Garbace Collect
// To-Do : 여러 Slave Client들에 대한 처리
//
func (masterClient RedisClient) cleanUpMemory() error {

	clientHashRangeMap[masterClient.Address] = nil
	delete(clientHashRangeMap, masterClient.Address)

	slaveNode := masterSlaveMap[masterClient.Address]

	if err := masterClient.removeFromList(); err != nil {
		return err
	}

	if err := slaveNode.removeFromList(); err != nil {
		return err
	}
	delete(MasterSlaveChannelMap, masterClient.Address)
	delete(masterSlaveMap, masterClient.Address)
	delete(slaveMasterMap, slaveNode.Address)

	return nil
}
