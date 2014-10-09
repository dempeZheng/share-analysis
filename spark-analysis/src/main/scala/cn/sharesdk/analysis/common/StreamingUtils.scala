package cn.sharesdk.analysis.common

import org.apache.spark.Logging

/**
 * Created by dempe on 14-6-12.
 */
object StreamingUtils extends Logging {

  def updateFuc = (values: Seq[Int], state: Option[Int]) => {

    val currentCount = values.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)

  }

  def newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap(t => {
      val currentCount = t._2.foldLeft(0)(_ + _)
      val previousCount = t._3.getOrElse(0)
      Some(currentCount + previousCount)
    }.map(s => {
      logWarning("=============" + t._1.toString + " : " + s.toString)
      (t._1, s)
    }))
  }


}
