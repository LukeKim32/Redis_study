package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"hash_interface/internal/cluster"
	"hash_interface/internal/models"
	"hash_interface/internal/models/response"
)

const baseUrl = "http://10.113.93.194:8001"

func requestAddClientToServer(dataFlags clientFlag) error {

	requestURI := fmt.Sprintf("%s/clients", baseUrl)

	client := &http.Client{}

	requestData := models.NewClientRequestContainer{}
	requestData.Address = dataFlags.Address

	if dataFlags.Slave {

		if dataFlags.SlaveOf != "" {
			return fmt.Errorf("새로운 Slave의 Master 주소를 입력해주세요")
		}

		requestData.Role = "slave"
		requestData.MasterAddress = dataFlags.SlaveOf

	} else {
		requestData.Role = "master"
	}

	encodedData, err := json.Marshal(requestData)
	if err != nil {
		return err
	}

	requestBody := bytes.NewBuffer(encodedData)

	setRequest, err := http.NewRequest("POST", requestURI, requestBody)
	if err != nil {
		return err
	}

	res, err := client.Do(setRequest)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// 해쉬 서버 응답 파싱
	var hashServerResponse response.RedisListTemplate
	decoder := json.NewDecoder(res.Body)

	if err := decoder.Decode(&hashServerResponse); err != nil {
		return err
	}

	fmt.Printf("  Add New Client (%s) 명령 수행 : \n", dataFlags.Address)
	fmt.Printf("    - 결과 : %s\n", hashServerResponse.Message)
	fmt.Printf("    - 현재 등록된 마스터 : \n")
	for i, eachMaster := range hashServerResponse.Masters {
		fmt.Printf("        %d) : %s\n", i+1, eachMaster.Address)
	}
	fmt.Printf("    - 현재 등록된 슬레이브 : \n")
	for i, eachSlave := range hashServerResponse.Slaves {
		fmt.Printf("        %d) : %s\n", i+1, eachSlave.Address)
	}
	return nil
}

func requestClientListToServer() error {

	requestURI := fmt.Sprintf("%s/clients", baseUrl)

	// 모니터 서버에 요청
	res, err := http.Get(requestURI)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// 해쉬 서버 응답 파싱
	var hashServerResponse response.RedisListTemplate
	decoder := json.NewDecoder(res.Body)

	if err := decoder.Decode(&hashServerResponse); err != nil {
		return err
	}

	fmt.Printf("  Get Client List 명령 수행 : \n")
	fmt.Printf("    - 현재 등록된 마스터 : \n")
	for i, eachMaster := range hashServerResponse.Masters {
		fmt.Printf("        %d) : %s\n", i+1, eachMaster.Address)
	}
	fmt.Printf("    - 현재 등록된 슬레이브 : \n")
	for i, eachSlave := range hashServerResponse.Slaves {
		fmt.Printf("        %d) : %s\n", i+1, eachSlave.Address)
	}

	return nil
}

func requestGetToServer(key string) error {
	requestURI := fmt.Sprintf("%s/hash/data/%s", baseUrl, key)

	// 모니터 서버에 요청
	res, err := http.Get(requestURI)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// 해쉬 서버 응답 파싱
	var hashServerResponse response.GetResultTemplate
	decoder := json.NewDecoder(res.Body)

	if err := decoder.Decode(&hashServerResponse); err != nil {
		return err
	}

	fmt.Printf("  Get %s 명령 수행 : \n", key)
	fmt.Printf("    - 결과 : %s\n", hashServerResponse.Result)
	fmt.Printf("    - 처리한 레디스 주소 : %s\n", hashServerResponse.NodeAdrress)

	return nil
}

func requestSetToServer(dataFlags dataFlag) error {

	requestURI := fmt.Sprintf("%s/hash/data", baseUrl)

	client := &http.Client{}

	KeyValue := cluster.KeyValuePair{
		Key:   dataFlags.Key,
		Value: dataFlags.Value,
	}

	requestData := models.DataRequestContainer{}
	requestData.Data = append(requestData.Data, KeyValue)

	encodedData, err := json.Marshal(requestData)
	if err != nil {
		return err
	}

	requestBody := bytes.NewBuffer(encodedData)

	setRequest, err := http.NewRequest("POST", requestURI, requestBody)
	if err != nil {
		return err
	}

	res, err := client.Do(setRequest)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// 해쉬 서버 응답 파싱
	var hashServerResponse response.SetResultTemplate
	decoder := json.NewDecoder(res.Body)

	if err := decoder.Decode(&hashServerResponse); err != nil {
		return err
	}

	fmt.Printf("  Set %s 명령 수행 : \n", dataFlags.Key)
	fmt.Printf("    - 결과 : %s\n", hashServerResponse.Results[0].Result)
	fmt.Printf("    - 처리한 레디스 주소 : %s\n", hashServerResponse.Results[0].NodeAdrress)

	return nil
}
