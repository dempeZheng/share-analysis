package cn.sharesdk.analysis.common

import com.alibaba.fastjson.{JSON, JSONObject}

/**
 * Created by dempe on 14-6-6.
 */
class Message(val deviceId: String, val appver: String, val channelName: String, val launchData: Any, val exitData: Any,
              val appkey: String) {


}

object Message {

  def fromString(line: String): Message = {
    try {
      val jo: JSONObject = JSON.parseObject(line)
      val m = jo.getJSONObject(R.M)
      val deviceData = m.getJSONObject(R.DEVICE_DATA)
      val deviceId = deviceData.getString(R.DEVICE_ID)
      val appver = deviceData.getString(R.APPVER)
      val channelName = deviceData.getString(R.CHANNEL_NAME)
      val launchData = m.getJSONArray(R.LAUNCH_DATA)
      val exitData = m.getJSONArray(R.EXIT_DATA)
      val appkey = jo.getString(R.APPKEY)
      new Message(deviceId, appver, channelName, launchData, exitData, appkey)
    } catch {
      case ex: Exception => {
        println(ex);
        null
      }
    }


  }

}
