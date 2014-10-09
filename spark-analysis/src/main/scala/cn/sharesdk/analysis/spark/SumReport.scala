package cn.sharesdk.analysis.spark

import cn.sharesdk.analysis.common.{Message, R, DateUtils}
import org.apache.spark.{Logging, SparkContext, SparkConf}

/**
 * Created by dempe on 14-6-9.
 */
object SumReport extends Logging {

  def main(args: Array[String]) {

    val hdfsHost = if (args.length > 0) args(0) else "hdfs://localhost:9000"
    val redisIp = if (args.length > 1) args(1) else "localhost"
    val redisPort = if (args.length > 2) args(2) else "6379"
    val monthTime = if (args.length > 3) args(3) else DateUtils.getCurMonth
    val conf = new SparkConf()
    conf.setSparkHome(System.getenv("SPARK_HOME"))
    conf.setAppName("SumReport")

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "cn.sharesdk.common.MyRegistrator")

    System.setProperty("redis.ip", redisIp)
    System.setProperty("redis.port", redisPort)

    val sc = new SparkContext(conf)

    val prefixPath = hdfsHost + R.reportDirect + monthTime + "*"

    logDebug("report==>starting read textDate from hdfs")
    val textData = sc.textFile(prefixPath)
    val message = textData.map(value => Message.fromString(value)).filter(value => {
      value != null
    }).cache()
    logDebug("report==>finish validator data and cache data to rdd")
    // 使用频率=>日启动次数分布[sum_browsing_frequency]
    val dailyStartFrequencyReport = new DailyStartFrequencyReport
    dailyStartFrequencyReport.runReport(message)



    sc.stop()
  }

}
