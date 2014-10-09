package cn.sharesdk.analysis.common

import redis.clients.jedis.{ShardedJedis, JedisShardInfo, JedisPoolConfig, ShardedJedisPool}
import java.util

/**
 * Created by dempe on 14-6-10.
 */
object RedisUtils {

  private var pool: ShardedJedisPool = null

  val config: JedisPoolConfig = new JedisPoolConfig()

  def init() {
    val conf = new ReportConf()
    val ips = conf.redis_ip.split(",")
    val list: util.LinkedList[JedisShardInfo] = new util.LinkedList[JedisShardInfo]();
    ips.foreach(ip => {
      val jedisShardInfo: JedisShardInfo = new JedisShardInfo(ip, Integer.valueOf(conf.redis_port));
      list.add(jedisShardInfo);
    })
    pool = new ShardedJedisPool(config, list);
  }

  init()

  def getJedis: ShardedJedis = {
    return pool.getResource
  }

  def returnJedis(jedis: ShardedJedis) {
    pool.returnResource(jedis)
  }


  def isNewUser(deviceId: String): Boolean = {
    getJedis.get(deviceId) match {
      case null => true
      case _ => false
    }
  }

  def isActiveUser(key: String, setName: String): Boolean = {
    if (!getJedis.sismember(setName, key)) {
      getJedis.sadd(setName, key)
      true
    } else {
      false
    }

  }

  def getRentionFreq(device: String, strDate: String): Array[Int] = {
    val createDate: String = getJedis.get(device)
    if (createDate == null) {
      getJedis.set(device, strDate)
    }
    var arr = Array(0, 0, 0)
    try {
      arr = DateUtils.getRetentionFreq(strDate, createDate)
    }
    catch {
      case e: Exception => {
      }
    }
    return arr
  }


}
