package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-4.
 */
class ErrorReport {

  def runReport(message: DStream[StreamMessage]) = {

    val errMap = message.flatMap(m => {
      m.errorData.toArray().map(edata => {
        val ed = edata.asInstanceOf[JSONObject]
        val errLog = ed.get(R.ERROR_LOG).asInstanceOf[String]
        val createDate = ed.get(R.CREATE_DATE).asInstanceOf[String]
        val stackTrace = ed.get(R.STACK_TRACE).asInstanceOf[String]
        (m.appkey + R.split + m.appver + R.split + m.channelName + R.split + DateUtils.formatStr(createDate, "yyyyMMdd") + R.split + stackTrace + R.split + errLog, 1.toLong)
      })
    })

    //
    val sum_err = errMap.reduceByKey(_ + _)
    PersitUtils.hincrByStream(sum_err, "num")
  }

}
