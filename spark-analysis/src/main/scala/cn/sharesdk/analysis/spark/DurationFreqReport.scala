package cn.sharesdk.analysis.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import com.alibaba.fastjson.{JSONObject, JSONArray}
import cn.sharesdk.analysis.common._
import org.apache.spark.SparkContext._

/**
 * Created by dempe on 14-6-6.
 */
class DurationFreqReport extends Logging {
  def runReport(message: RDD[Message]) {

    val duration = message.filter(value => {
      value.exitData != null
    }).cache()
    // day
    val dayDuration = duration.flatMap(value => {
      val exitData = value.exitData.asInstanceOf[JSONArray].toArray()
      exitData.map(edata => {
        val ed = edata.asInstanceOf[JSONObject]
        val createDate = ed.get(R.CREATE_DATE).asInstanceOf[String]
        val endDate = ed.get(R.END_DATE).asInstanceOf[String]
        val duration = DateUtils.getDatePeriod(createDate, endDate)
        (value.appkey + R.split + value.appver + R.split + value.channelName + R.split +
          DateUtils.formatStr(createDate, "yyyyMMdd") + R.split + value.deviceId, duration)
      })
    }).reduceByKey(_ + _).cache()

    val dayDurationFreq = dayDuration.map(value => {
      (value._1.substring(0, value._1.lastIndexOf(R.split)) + R.split + FrequencyHelper.getDayUseDurationFreq(value._2), 1)
    }).reduceByKey(_ + _)
    //save
    logInfo("starting persist sum_browsing_duration ...")
    dayDurationFreq.take(10).foreach(value => logInfo(value.toString()))
    PersitUtils.saveReportToRedis(dayDurationFreq, R.deviceNum, R.sum_browsing_duration)
    logInfo("finished persist  sum_browsing_duration ...")

    //per
    val perDurationfreq = duration.flatMap(value => {
      val exitData = value.exitData.asInstanceOf[JSONArray].toArray()
      exitData.map(edata => {
        val ed = edata.asInstanceOf[JSONObject]
        val createDate = ed.get(R.CREATE_DATE).asInstanceOf[String]
        val endDate = ed.get(R.END_DATE).asInstanceOf[String]
        val duration = DateUtils.getDatePeriod(createDate, endDate)
        val freq = FrequencyHelper.getDayUseDurationFreq(duration)
        (value.appkey + R.split + value.appver + R.split + value.channelName + R.split + DateUtils.formatStr(createDate, "yyyyMMdd") + R.split + freq, 1)
      })
    }).reduceByKey(_ + _)
    logInfo("starting persist sum_browsing_duration ...")
    perDurationfreq.take(10).foreach(value => logInfo(value.toString()))
    PersitUtils.saveReportToRedis(perDurationfreq, R.runNum, R.sum_browsing_duration)
    logInfo("finished persist  sum_browsing_duration ...")


  }

}
