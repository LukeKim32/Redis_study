# interface-hash-server
interface-hash-db is a interface server communicates with Users externally, Redis containers internally at the same time
tried to *implement Redis-Cluster-like interface server* to study in-memory, hash database
(Study about Redis will be updated later)

## Features

- Simple "SET", "GET" command used in Redis supported
- Hash Slot implemented with CRC16 key modulo 16384 (Similar like Redis-Cluster) 
- Reverse Proxy (Nginx) Load Balancing(RR)
- Other Containers (except Proxy) Unreachable (port not binded to machine)
- Specific Persistence Option will be later updated

## Installation

- 1. Clone the repository
- 2. "docker-compose up -d --build"
- 3. Send @POST Request to "http://localhost/interface" (localhost can be replaced with public IP)
> Request Body format : { "Command" : "SET"/"GET", "Arguments" : ["key","value"(if needed),...,] } <br><br>
ex : ``` { 
  "Command" : "SET",
	"Arguments" : ["foo", "bar"]
 } ```

<br>

> Response : <br> 1. "message" : Request Command Success/Fail result + Interface Server IP(to check LoadBalance) <br> 2. "redis": 1) response of Redis container & 2) Redis Node address(to check if Hash works) <br><br> ex : ``` {
    "message": "SET foo bar Success : Handled in Server(IP : 172.29.0.3)",
    "redis": { 
    "response": "OK",
    "node_address": "172.29.0.5:8001" 
    },
    "_links": {
        "message": "Main URL",
		"href": "http://localhost/interface"
	}
}```

## Server 
  
- 서버 구성도 :
  
  ![스크린샷 2020-01-28 오전 11 05 32](https://es.naverlabs.com/storage/user/3125/files/25ae6800-41be-11ea-8644-718649e9f689)

- API docs 현재 미사용

