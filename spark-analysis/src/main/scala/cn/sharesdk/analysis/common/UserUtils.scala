package cn.sharesdk.analysis.common

import redis.clients.jedis.ShardedJedis

/**
 * Created by dempe on 14-6-12.
 */
object UserUtils {
  val shardedJedis: ShardedJedis = RedisUtils.getJedis

  def isNewUser(deviceId: String, createDate: String): Boolean = {
    shardedJedis.get(deviceId) match {
      case null => {
        shardedJedis.set(deviceId, createDate)
        true
      }
      case _ => false
    }
  }

  def isActiveUser(key: String, deviceId: String): Boolean = {
    if (shardedJedis.hget("ad"+R.split+key+R.split+deviceId,"active_device")==null) {
      shardedJedis.hincrBy("ad"+R.split+key+R.split+deviceId,"active_device",1.toLong)
      true
    } else {
      false
    }

  }


  def getRentionFreq(device: String, strDate: String): Array[Int] = {
    val createDate: String = shardedJedis.get(device)
    if (createDate == null) {
      shardedJedis.set(device, strDate)
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
