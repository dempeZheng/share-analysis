package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.streaming.dstream.DStream
import cn.sharesdk.analysis.common._
import org.apache.spark.streaming.StreamingContext._
import com.alibaba.fastjson.JSONArray

/**
 * Created by dempe on 14-6-4.
 */
class NewDevice {

  def runReport(message: DStream[StreamMessage]) = {

    val new_device_daily = message.filter(_.launchData.size() > 0).cache()
    //daily & hourly
    val hourly_new_device = new_device_daily.map(line => {
      val createDate = line.launchData.asInstanceOf[JSONArray].getJSONObject(0).get(R.CREATE_DATE).asInstanceOf[String]
      (line.appkey + R.split + DateUtils.formatStr(createDate, "yyyyMMddHH"), 1.toLong)
    }).reduceByKey(_ + _)
    PersitUtils.hincrByStream(hourly_new_device, R.newDevice)

    val daily_new_device = hourly_new_device.map(value => {
      (value._1.substring(0, value._1.length - 2), value._2)
    }).reduceByKey(_ + _)
    PersitUtils.hincrByStream(daily_new_device, R.newDevice)

    val ver_chanel_daily = new_device_daily.map(line => {
      val datas = Map(R.MODEL -> line.model, R.SCREENSIZE -> line.screensize,
        R.SYSVER -> line.sysver, R.CARRIER -> line.carrier,
        R.NETWORKTYPE -> line.networktype, R.PROVINCE -> line.province)
      //
      val createDate = line.launchData.asInstanceOf[JSONArray].getJSONObject(0).get(R.CREATE_DATE).asInstanceOf[String]
      (line.appkey + R.split + line.appver + R.split + line.channelName + R.split +
        DateUtils.formatStr(createDate, "yyyyMMdd"), datas)
    }).cache()

    val newDeviceNum = ver_chanel_daily.map(value => (value._1, 1.toLong)).reduceByKey(_ + _)
    PersitUtils.hincrByStream(newDeviceNum, R.newDevice)

    val propList = List(R.MODEL, R.CHANNEL_NAME, R.SCREENSIZE, R.SYSVER, R.CARRIER, R.NETWORKTYPE, R.PROVINCE)
    propList.foreach(prop => {
      val newDevicePropNum = ver_chanel_daily.map(value => (value._1 + R.split + value._2.get(prop).getOrElse(), 1.toLong)).
        reduceByKey(_ + _)
      PersitUtils.hincrByStream(newDevicePropNum, R.newDevice)
    })


  }


}
