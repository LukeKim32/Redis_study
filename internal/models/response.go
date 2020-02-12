package models

type ResponseFormat struct {
	Response []RedisResponse `json:"response"`
}

type RedisResponse struct {
	Result      string `json:"result"`
	NodeAdrress string `json:"node_address"`
}
