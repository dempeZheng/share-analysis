package cn.sharesdk.analysis.common

import java.text.{SimpleDateFormat, DateFormat}
import java.util.Date
import org.apache.spark.Logging

/**
 * Created by dempe on 14-6-11.
 */
class DateUtil extends Logging {

  var format: DateFormat = new SimpleDateFormat("yyyyMMdd")
  var dateArr = Array(1, 2, 3, 4, 5, 6, 7, 14, 30)
  private final val TIMENUM: Int = 1000
  private var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def formatStrDate(strDate: String): Date = {
    var date: Date = null
    try {
      date = sdf.parse(strDate)
    }
    catch {
      case e: Exception => {
        logError(e.toString)
        date = new Date
      }
    }
    return date
  }

  def getDatePeriod(startDate: String, endDate: String): Long = {
    return (formatStrDate(endDate).getTime - formatStrDate(startDate).getTime) / TIMENUM
  }

  def getCurMonth: String = {
    return format.format(new Date).substring(0, 8)
  }

  /**
   * @param dateStr
   * @param regex
   * @return
   */
  def formatStr(dateStr: String, regex: String): String = {
    val year: String = dateStr.substring(0, 4)
    val month: String = dateStr.substring(5, 7)
    val day: String = dateStr.substring(8, 10)
    val hour: String = dateStr.substring(11, 13)
    val min: String = dateStr.substring(14, 16)
    val sec: String = dateStr.substring(17, 19)
    return regex.toUpperCase.replace("YYYY", year).replace("MM", month).replace("DD", day).replace("HH", hour).replace("MI", min).replace("SS", sec)
  }

}
