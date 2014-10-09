package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-5.
 */
class Interval {

  def runReport(message: DStream[StreamMessage]) = {

    val intervalFreq = message.flatMap(line => {

      line.launchData.toArray.map(ldata => {
        val ld = ldata.asInstanceOf[JSONObject]
        val createDate = ld.get(R.CREATE_DATE).asInstanceOf[String]
        val lastEndDate = ld.get(R.LAST_END_DATE).asInstanceOf[String]
        val intervalFreq = FrequencyHelper.getDayIntervalFreq(createDate, lastEndDate)
        ("interval"+R.split+line.appkey + R.split + line.appver + R.split + line.channelName + R.split +
          DateUtils.formatStr(createDate, "yyyyMMdd") + R.split + intervalFreq, 1.toLong)
      }
      )
    }).reduceByKey(_ + _)
    PersitUtils.hincrByStream(intervalFreq, R.runNum)
  }

}
