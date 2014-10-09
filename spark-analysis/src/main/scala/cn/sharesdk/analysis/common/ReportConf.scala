package cn.sharesdk.analysis.common

import org.apache.commons.configuration.PropertiesConfiguration

/**
 * Created by dempe on 14-6-10.
 */
class ReportConf {


  val report_home = System.getenv("REPORT_HOME")


  val conf_dir = report_home + "/conf/"


  val config: PropertiesConfiguration = new PropertiesConfiguration(conf_dir + "report.properties")
  val redis_port = config.getString("redis.port")
  val redis_ip = config.getString("redis.ip")


}
