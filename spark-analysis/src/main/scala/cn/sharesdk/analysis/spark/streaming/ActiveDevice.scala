package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-4.
 */
class ActiveDevice {

  def runReport(message: DStream[StreamMessage]) = {

    val activeDevice = message.flatMap(line => {
      line.launchData.toArray().map(ld => {
        val createDate = DateUtils.formatStr(ld.asInstanceOf[JSONObject].get(R.CREATE_DATE).asInstanceOf[String], "yyyyMMdd")
        ("ad" + R.split + line.appkey + R.split + line.appver + R.split + line.channelName + R.split + createDate + R.split + line.deviceId, 1.toLong)
      })
    })

    PersitUtils.hincrByStream(activeDevice, "active_device")
  }

}
