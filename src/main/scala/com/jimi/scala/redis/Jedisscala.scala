package com.jimi.scala.redis

import redis.clients.jedis.Jedis

object Jedisscala {

  def main(args: Array[String]): Unit = { //连接本地的 Redis 服务

    val jedis: Jedis = new Jedis("172.16.10.104")
    jedis.auth("jimi@123")
    System.out.println("连接成功")

    //查看服务是否运行
    System.out.println("服务正在运行: " + jedis.ping)
    /*val keys = jedis.keys("*")
    val it = keys.iterator
    while ( {
      it.hasNext
    }) {
      val key = it.next
      System.out.println(key)
    }*/

    System.out.println("-------------------------------------")
    System.out.println(jedis.hgetAll("DC_IMEI_APPID"))
    jedis.hgetAll("DC_IMEI_APPID")
    val dc_imei_appid = jedis.hget("DC_IMEI_APPID", "201711071236521")
    System.out.println(dc_imei_appid)
    println(jedis.hget("DC_IMEI_APPID","201711071236523"))
    jedis.close()
  }
}
