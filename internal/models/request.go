package models

type RequestContainer struct {
	Data []KeyValuePair `json:"data"`
}

type KeyValuePair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
