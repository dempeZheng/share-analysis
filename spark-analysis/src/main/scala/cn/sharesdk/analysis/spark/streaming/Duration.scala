package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-4.
 */
class Duration {

  def runReport(message: DStream[StreamMessage]) = {

    val durationMap = message.flatMap(m => {
      val exitData = m.exitData.toArray()
      exitData.map(edata => {
        val ed = edata.asInstanceOf[JSONObject]
        val createDate = ed.get(R.CREATE_DATE).asInstanceOf[String]
        val endDate = ed.get(R.END_DATE).asInstanceOf[String]
        val duration = DateUtils.getDatePeriod(createDate, endDate)
        (m.appkey + R.split + m.appver + R.split + m.channelName + R.split + DateUtils.formatStr(createDate, "yyyyMMdd"), duration)
      })
    }).cache()

    //
    durationMap.reduceByKey(_ + _)
    PersitUtils.hincrByStream(durationMap, "duration")

    val dailyDuration = durationMap.map(value => {
      val keys = value._1.split(R.split)
      (keys(0) + R.split + keys(3), value._2)
    }).reduceByKey(_ + _)
    PersitUtils.hincrByStream(dailyDuration, "duration")


  }


}
