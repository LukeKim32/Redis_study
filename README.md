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
- 2. "make run"
- 3-1. To Set Key-Value, Send @POST Request to "http://localhost:8001/hash/data" (localhost can be replaced with public IP)
> Request Body format : { "data" : ["key","value"}] } <br><br>
ex : ``` { 
  "data" : [{
  	"key" : 문자,
	"value" : 문자
	},
	{
  	"key" : 문자,
	"value" : 문자
	}, ...
]
 } ```

<br>

> Response : <br> 1. "message" : Request Command Success/Fail result + Interface Server IP(to check LoadBalance) <br> 2. "redis": 1) response of Redis container & 2) Redis Node address(to check if Hash works) <br><br> ex : ``` {
    "message": "SET foo bar Success : Handled in Server(IP : 172.29.0.3)",
    "response" : [{
    	"result" : 보낸 key-value 명령,
	"handle_node" : 명령을 수행한 Redis Node 주소 
    }, ... , 
    ],
    "_links": {
        "message": "Main URL",
		"href": "http://localhost/interface"
	}
}```

- 3-2. To Get Value With Key, Send @GET Request to "http://localhost:8001/hash/data/{key}" 

<br>

> Response :
<br> ex : ``` {
    "message": "SET foo bar Success : Handled in Server(IP : 172.29.0.3)",
    "response" : Value,
    "_links": {
        "message": "Main URL",
		"href": "http://localhost/interface"
	}
}```

## Server 
  
- 서버 구성도 :
 <img width="765" alt="스크린샷 2020-02-14 오전 11 13 26" src="https://user-images.githubusercontent.com/48001093/74495405-2d3e7280-4f1b-11ea-9e4d-783e88ca2011.png">

- API docs 현재 미사용

