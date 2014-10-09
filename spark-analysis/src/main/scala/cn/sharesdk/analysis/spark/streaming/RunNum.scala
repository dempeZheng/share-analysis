package cn.sharesdk.analysis.spark.streaming

import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.alibaba.fastjson.JSONObject

/**
 * Created by dempe on 14-6-3.
 */
class RunNum extends Serializable {


  def runReport(message: DStream[StreamMessage]) = {

    val ver_chanel_daily = message.flatMap(line => {
      val datas = Map(R.MODEL -> line.model, R.SCREENSIZE -> line.screensize,
        R.CHANNEL_NAME -> line.channelName, R.SYSVER -> line.sysver, R.CARRIER -> line.carrier,
        R.NETWORKTYPE -> line.networktype, R.PROVINCE -> line.province)
      line.launchData.toArray().map(ld =>
        (line.appkey + R.split + line.appver + R.split + line.channelName + R.split +
          DateUtils.formatStr(ld.asInstanceOf[JSONObject].get(R.CREATE_DATE).asInstanceOf[String], "yyyyMMdd"), datas))
    }).cache()
    val runNum = ver_chanel_daily.map(value => (value._1, 1.toLong)).reduceByKey(_ + _)
    PersitUtils.hincrByStream(runNum, R.runNum)



    val propList = List(R.MODEL, R.CHANNEL_NAME, R.SCREENSIZE, R.SYSVER, R.CARRIER, R.NETWORKTYPE, R.PROVINCE)
    propList.map(prop => {
      val runNumProp = ver_chanel_daily.map(value => (value._1 + R.split + value._2.get(prop).getOrElse(), 1.toLong)).
        reduceByKey(_ + _)
      PersitUtils.hincrByStream(runNumProp, R.runNum)

    })
  }


}
