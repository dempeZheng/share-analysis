package cn.sharesdk.analysis.spark

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import com.alibaba.fastjson.{JSONObject, JSONArray}
import cn.sharesdk.analysis.common._

/**
 * 使用频率=>日启动次数分布[sum_browsing_frequency]
 * Created by dempe on 14-5-28.
 */
class DailyStartFrequencyReport extends Logging {

  def runReport(message: RDD[Message]) {
    val startDevice = message.filter(value => {
      value.launchData != null
    }).flatMap(value => {
      value.launchData.asInstanceOf[JSONArray].toArray.map(ldata => {
        val ld = ldata.asInstanceOf[JSONObject]
        val createDate = DateUtils.formatStr(ld.get(R.CREATE_DATE).asInstanceOf[String], "yyyyMMdd")
        (value.appkey + R.split + value.appver + R.split + value.channelName + R.split + createDate + R.split + value.deviceId, createDate)
      })

    }).map(value => (value._1, 1)).reduceByKey(_ + _).cache()
    val dailyStartFrequency = startDevice.map(value => {
      (value._1.substring(0, value._1.lastIndexOf(R.split)) + R.split + FrequencyHelper.getStartFrequency(value._2), 1)
    }).reduceByKey(_ + _).cache()
    logInfo("tarting persist sum_browsing_frequency ...")
    dailyStartFrequency.take(10).foreach(value => logInfo(value.toString()))
    PersitUtils.saveReportToRedis(dailyStartFrequency, R.deviceNum, R.sum_browsing_frequency)
    logInfo("finished persist dailyStartFrequency sum_browsing_frequency")

  }


}

