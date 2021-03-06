package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * jedis连接池
  */
object JedisConnectionPool {
  //获取到配合对象
  val config = new JedisPoolConfig()
  //设置最大连接数
  config.setMaxTotal(20)
  //设置最大空闲连接数
  config.setMaxIdle(10)
  val pool = new JedisPool(config, "192.168.138.101", 6379, 1000)

  def getConnection():Jedis={
    pool.getResource
  }
}

