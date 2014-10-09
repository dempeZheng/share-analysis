import com.alibaba.fastjson.JSONArray

/**
 * Created by dempe on 14-6-10.
 */
object ReportHomeTest {
  def main(args: Array[String]) {
    val arr = new JSONArray()
    arr.toArray().foreach(value => {
      println("---------")
    })
    println(arr.toArray().size)

  }

}
