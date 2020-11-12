package com.wang930126.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

    private var pool:JedisPool = null

    /**
      * 获取到jedis的连接
      */
    def getResource() = {
        if(pool == null){
            val conf: JedisPoolConfig = new JedisPoolConfig()
            conf.setMaxTotal(100)
            conf.setTestOnBorrow(false)
            conf.setMaxIdle(20)
            conf.setMinIdle(20)
            conf.setBlockWhenExhausted(true)
            conf.setMaxWaitMillis(2000)
            val redisHost = PropertiesUtil.getConfig("redis.host")
            val redisPort = PropertiesUtil.getConfig("redis.port").toInt
            pool = new JedisPool(conf,redisHost,redisPort)
        }
        pool.getResource
    }

    def returnResource(conn:Jedis):Unit = {
        if(conn != null){
            conn.close()
        }
    }

    def main(args:Array[String]):Unit = {

        val conn: Jedis = getResource()

        conn.sadd("dau_20201107","testmember")

        returnResource(conn)
    }

}
