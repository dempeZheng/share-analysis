package cn.sharesdk.analysis.common

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import cn.sharesdk.analysis.spark.{DurationFreqReport, DailyStartFrequencyReport}


/**
 * Created by dempe on 14-5-28.
 */


class MyRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Message])
    kryo.register(classOf[StreamMessage])
    kryo.register(classOf[DailyStartFrequencyReport])
    kryo.register(classOf[DurationFreqReport])
  }
}