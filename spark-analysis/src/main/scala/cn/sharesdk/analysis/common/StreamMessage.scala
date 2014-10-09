package cn.sharesdk.analysis.common

import com.alibaba.fastjson.{JSONArray, JSON, JSONObject}


/**
 * Created by dempe on 14-6-11.
 */
class StreamMessage(val deviceId: String, val appver: String, val channelName: String, val model: String, val screensize: String,
                    val carrier: String, val sysver: String, val networktype: String, val province: String, val launchData: JSONArray, val exitData: JSONArray,
                    val errorData: JSONArray, val appkey: String) {


}

object StreamMessage {

  def fromString(line: String): StreamMessage = {


    val jo: JSONObject = JSON.parseObject(line)
    val gunzipM = jo.getString(R.M)

    try {
      val strm: String = Utils.gunzip(gunzipM)
      val m: JSONObject = JSON.parseObject(strm)

      val deviceData = m.getJSONObject(R.DEVICE_DATA)
      val deviceId = deviceData.getString(R.DEVICE_ID)
      val appver = deviceData.getString(R.APPVER)
      val channelName = deviceData.getString(R.CHANNEL_NAME)
      val model = deviceData.getString(R.MODEL)
      val screensize = deviceData.getString(R.SCREENSIZE)
      val carrier = deviceData.getString(R.CARRIER)
      val sysver = deviceData.getString(R.SYSVER)
      val networktype = deviceData.getString(R.NETWORKTYPE)
      val province = deviceData.getString(R.PROVINCE)
      var launchData = m.getJSONArray(R.LAUNCH_DATA)
      var exitData = m.getJSONArray(R.EXIT_DATA)
      var errorData = m.getJSONArray(R.ERROR_DATA)
      if (launchData == null) {
        launchData = new JSONArray()
      }
      if (exitData == null) {
        exitData = new JSONArray()
      }
      if (errorData == null) {
        errorData = new JSONArray()
      }
      val appkey = jo.getString(R.APPKEY)
      new StreamMessage(deviceId, appver, channelName, model, screensize, carrier, sysver, networktype,
        province, launchData, exitData, errorData, appkey)


    }
    catch {
      case e: Exception => {
        e.printStackTrace
        null
      }
    }


  }

}