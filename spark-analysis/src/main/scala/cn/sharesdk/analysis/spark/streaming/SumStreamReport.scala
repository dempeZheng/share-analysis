package cn.sharesdk.analysis.spark.streaming

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import cn.sharesdk.analysis.spark.streaming.receiver.QueueReceiver
import cn.sharesdk.analysis.common.StreamMessage
import org.apache.log4j.{Level, Logger}

/**
 * Created by dempe on 14-6-3.
 */
object SumStreamReport extends Logging {

  def main(args: Array[String]) {


    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming ")
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val sparkConf = new SparkConf()
    sparkConf.setAppName("QueueReceiver")
    sparkConf.setMaster("local[2]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "cn.sharesdk.report.common.MyRegistrator")
    sparkConf.set("spark.executor.memory", "2g")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("/app/data/spark/checkpoint")

    val lines = ssc.receiverStream(new QueueReceiver("192.168.1.112", 8000))
    val message = lines.map(StreamMessage.fromString(_)).filter(_ != null).cache()

//    val runNum = new RunNum
//    runNum.runReport(message)
//
//    val newDevice = new NewDevice
//    newDevice.runReport(message)
//
//    val activeDevice = new ActiveDevice
//    activeDevice.runReport(message)
//
//    val duration = new Duration
//    duration.runReport(message)
//
//    val hourlyAndDaily = new HourlyAndDaily
//    hourlyAndDaily.runReport(message)
//
//    val errorReport = new ErrorReport
//    errorReport.runReport(message)

    val retention = new Retention
    retention.runReport(message)

//    val interval = new Interval
//    interval.runReport(message)




    ssc.start()
    ssc.awaitTermination()

  }

}
