# Redis_study
This repository is for studying Redis

Redis : "Re"mote "Di"ctionary "S"erver

1. Stores data as "Key:Value"

2. No-SQL : Use multiple data structures to store data (No tables or Select, Insert Query)

3. Interaction with data is "command based"

4. In-memory DB = Fast (Persistency Support)

Written in C

Why to use Redis?

Performance : https://redis.io/topics/benchmarks

Simple & Flexible : No need of defining tables, No Select/Insert/Update/Delete , With "Commands"

Durable : Option to write on disk(configurable), Used as Caching & Full-fledge db

Language support : https://redis.io/clients

Compatibility : Used as 2nd db to make transactions/queries faster

일반적인 DB와 request/response는 Disk에서 가져와 Time consuming
<->
Redis가 Cache 역할. 

Built-in Master-slave replication feature supported (replicate data to any slaves)

<img width="623" alt="스크린샷 2020-01-26 오후 8 12 36" src="https://user-images.githubusercontent.com/48001093/73134292-4076c480-4078-11ea-9cb8-e7c2d013d09a.png">

만약 Master가 죽으면, Slave가 자연스럽게 처리 가능(No downtime)

Single text file for all config

Single Threaded - One action at a time

Pipelining : cluster multiple command


==================

Redis Datatypes : Strings, Lists, Sets, Sorted Sets, Hashes, Bitmaps, Hyperlogs, Geospatial Indexes

MongoDB + memchached

