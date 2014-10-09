package cn.sharesdk.analysis.spark

import org.apache.spark.{Logging, SparkContext, SparkConf}
import cn.sharesdk.analysis.common.Message

/**
 * Created by dempe on 14-6-6.
 */
object ReportSimpleTest extends Logging {
  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setAppName("SumReport")

    conf.set("spark.executor.memory", "4g")
    conf.set("spark.core.max", "4")


    val report_home = System.getenv("REPORT_HOME")
    if (report_home == null) {
      logError("REPORT_HOME must be set first!")
      System.exit(0)
    }
    val conf_dir = report_home + "/conf"

    val sc = new SparkContext(conf)

    val prefixPath = conf_dir + "/sample.txt"

    val textData = sc.textFile(prefixPath)
    val message = textData.map(value => Message.fromString(value)).filter(value => {
      value != null
    }).cache()


    // 使用频率=>日启动次数分布[sum_browsing_frequency]
    val dailyStartFrequencyReport = new DailyStartFrequencyReport
    dailyStartFrequencyReport.runReport(message)



    sc.stop()
  }

}
