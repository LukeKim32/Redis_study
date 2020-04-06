# interface-hash-server
interface-hash-db is a interface server communicates with Users externally, Redis containers internally at the same time
tried to *implement Redis-Cluster-like interface server* to study in-memory, hash database
(Study about Redis will be updated later)

## Features

- "SET", "GET" command used in Redis supported
- Hash Slot implemented with CRC16 key modulo 16384 (Similar like Redis-Cluster) 
- Master-Slave Replication
- Failover Recovery : Slave Dead, Restarts Container / Both Master-Slave dead, Redistribute Data and Hash slots
- Other Containers (except Proxy) Unreachable (port not binded to machine)
- Deprecated *(Reverse Proxy (Nginx) Load Balancing(RR))*

## Installation

- 0. ***Currently Only Linux supported, with Docker daemon installed with socket opened for DooD***
> How to open Docker socket for DooD in Linux ?
> 1. open /lib/systemd/system/docker.service 
> 2. 파일의 "ExecStart" 부분에 Open할 주소 명시 후 Reload
> - ExecStart=/usr/bin/dockerd -H fd:// --containerd=/run/containerd/containerd.sock ***-H tcp://0.0.0.0:2375***

- 1. Clone the repository
- 2. "make run"
- 3. To test, run the cli packaged in ./main/cli,
-    Or Directly HTTP request to Server (Document : "http://localhost:8888/api/v1/docs"

## Server 
  
- 서버 구성도 :
 <img width="765" alt="스크린샷 2020-02-14 오전 11 13 26" src="https://user-images.githubusercontent.com/48001093/74495405-2d3e7280-4f1b-11ea-9e4d-783e88ca2011.png">

