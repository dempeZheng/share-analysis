package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-4.
 */
class Retention {

  def runReport(message: DStream[StreamMessage]) = {
    val device = message.flatMap(line => {
      line.launchData.toArray().map(ld => {
        val createDate = DateUtils.formatStr(ld.asInstanceOf[JSONObject].get(R.CREATE_DATE).asInstanceOf[String], "yyyyMMdd")
        val datas = Map(R.CREATE_DATE -> createDate, R.DEVICE_ID -> line.deviceId)
        (line.appkey + R.split + line.appver + R.split + line.channelName + R.split + createDate, datas)
      })
    })

    val active_device = device.filter(value => UserUtils.isActiveUser(value._1, value._2.get(R.DEVICE_ID).get)).cache()

    val retentionDevice = active_device.map(value => {
      val deviceId = value._2.get(R.DEVICE_ID).get
      val createDate = value._2.get(R.CREATE_DATE).get
      val rps = UserUtils.getRentionFreq(deviceId, createDate)
      (value._1, rps)
    }).cache()
    retentionDevice.map(value=>value._2(0)).print()

    val dailyRetention = retentionDevice.map(value => {
      (value._1, value._2(0))
    }).filter(value => value._2.toInt != 0).map(value => {
      ("retention"+R.split+value._1 + R.split + value._2, 1.toLong)
    }).reduceByKey(_ + _)
    dailyRetention.print()
    PersitUtils.saveStreamToRedis(dailyRetention, "d")


    val weeklyRetention = retentionDevice.map(value => {
      (value._1, value._2(1))
    }).filter(value => value._2.toInt != 0).map(value => {
      ("retention"+R.split+value._1 + R.split + value._2, 1.toLong)
    }).reduceByKey(_ + _)
    weeklyRetention.print()
    PersitUtils.saveStreamToRedis(weeklyRetention, "w")

    val monthlyRetention = retentionDevice.map(value => {
      (value._1, value._2(2))
    }).filter(value => value._2.toInt != 0).map(value => {
      ("retention"+R.split+value._1 + R.split + value._2, 1.toLong)
    }).reduceByKey(_ + _)
    PersitUtils.saveStreamToRedis(monthlyRetention, "m")

  }

}
