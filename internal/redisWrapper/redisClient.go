package redisWrapper

import "github.com/gomodule/redigo/redis"

type RedisClient struct{
	Connection redis.Conn
	Address	string
}