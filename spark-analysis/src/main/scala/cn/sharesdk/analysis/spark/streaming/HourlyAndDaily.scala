package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-4.
 */
class HourlyAndDaily {
  def runReport(message: DStream[StreamMessage]) = {
    val ver_hourly = message.flatMap(line => {
      line.launchData.toArray().map(ld =>
        (line.appkey + R.split + DateUtils.formatStr(ld.asInstanceOf[JSONObject].get(R.CREATE_DATE).asInstanceOf[String], "yyyyMMddHH"), 1.toLong))
    }).cache()
    val hourly = ver_hourly.reduceByKey(_ + _)
    PersitUtils.hincrByStream(hourly, R.runNum)

    val daily = hourly.map(value => {
      (value._1.substring(0, value._1.length - 2), value._2)
    }).reduceByKey(_ + _)

    PersitUtils.hincrByStream(daily, R.runNum)


  }

}
