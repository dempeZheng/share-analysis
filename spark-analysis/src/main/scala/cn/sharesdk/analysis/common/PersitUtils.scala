package cn.sharesdk.analysis.common

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import redis.clients.jedis.ShardedJedisPipeline
import scala.Tuple2
import org.apache.spark.streaming.dstream.DStream


/**
 * Created by dempe on 14-6-4.
 */
object PersitUtils {

  /**
   * save value to reids
   * @param rdd
   * @param field
   */
  def saveToRedis[T: ClassTag](rdd: RDD[Tuple2[String, T]], field: String) {
    val jdedis = RedisUtils.getJedis

    val pipeline: ShardedJedisPipeline = jdedis.pipelined
    rdd.toLocalIterator.foreach(value => {
      pipeline.hset(value._1, field, value._2.toString)
    })
    pipeline.sync()
    RedisUtils.returnJedis(jdedis)
  }

  /**
   * save report value to reids
   * @param rdd
   * @param field
   * @param reportName
   */
  def saveReportToRedis[T: ClassTag](rdd: RDD[Tuple2[String, T]], field: String, reportName: String) {
    val jdedis = RedisUtils.getJedis
    val pipeline: ShardedJedisPipeline = jdedis.pipelined
    rdd.toLocalIterator.foreach(value => {
      pipeline.hset(reportName + R.split + value._1, field, value._2.toString)
    })
    pipeline.sync()
    RedisUtils.returnJedis(jdedis)
  }

  /**
   *
   * @param rdd
   * @param field
   * @param fileName
   * @tparam T
   */
  def saveFiledsToRedis[T: ClassTag](rdd: RDD[Tuple2[String, T]], field: String, fileName: String) {
    val jdedis = RedisUtils.getJedis
    val pipeline: ShardedJedisPipeline = jdedis.pipelined
    rdd.toLocalIterator.foreach(value => {
      pipeline.hset(value._1, field + R.split + fileName, value._2.toString)
    })
    pipeline.sync()
    RedisUtils.returnJedis(jdedis)
  }


  /**
   *
   * @param ds
   * @param field
   * @tparam V
   */
  def saveStreamToRedis[V: ClassTag](ds: DStream[(String, V)], field: String) {
    ds.foreachRDD((rdd, time) => {
      rdd.take(2).foreach(println)
      saveToRedis(rdd, field)
    })
  }


  /**
   * save value to reids
   * @param rdd
   * @param field
   */
  def hincrBy(rdd: RDD[Tuple2[String, Long]], field: String) {
    val jdedis = RedisUtils.getJedis

    val pipeline: ShardedJedisPipeline = jdedis.pipelined
    rdd.toLocalIterator.foreach(value => {
      pipeline.hincrBy(value._1, field, value._2)
    })
    pipeline.sync()
    RedisUtils.returnJedis(jdedis)
  }

  /**
   *
   * @param ds
   * @param field
   */
  def hincrByStream(ds: DStream[(String, Long)], field: String) {
    ds.foreachRDD((rdd, time) => {
      rdd.take(2).foreach(println)
      hincrBy(rdd, field)
    })
  }

  def saveStreamKeySetToRedis[V: ClassTag](ds: DStream[(String, V)], field: String) {
    ds.foreachRDD((rdd, time) => {
      val jdedis = RedisUtils.getJedis
      val pipeline: ShardedJedisPipeline = jdedis.pipelined
      rdd.toLocalIterator.foreach(value => {
        //rdd.take(2).foreach(println)
        pipeline.sadd(field + R.split + value._2, value._1)
      })
      pipeline.sync()
      RedisUtils.returnJedis(jdedis)
    })

  }

  /**
   *
   * @param ds
   * @tparam V
   */
  def saveRetentionStreamToRedis[V: ClassTag](ds: DStream[(String, V)], f: String) {
    ds.foreachRDD((rdd, time) => {
      val jdedis = RedisUtils.getJedis
      val pipeline: ShardedJedisPipeline = jdedis.pipelined
      rdd.toLocalIterator.foreach(value => {
        val key = value._1.substring(0, value._1.lastIndexOf(R.split))
        val filed = value._1.substring(value._1.lastIndexOf(R.split), value._1.length)
        pipeline.hset(key, filed + f, value._2.toString)
      })
      pipeline.sync()
      RedisUtils.returnJedis(jdedis)
    })
  }


}
