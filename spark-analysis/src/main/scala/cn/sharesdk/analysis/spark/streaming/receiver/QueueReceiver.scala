package cn.sharesdk.analysis.spark.streaming.receiver

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import cn.sharesdk.queue.tcp.SocketQueueClient

/**
 * Created by dempe on 14-6-11.
 */
class QueueReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Queue Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socketQueueClient: SocketQueueClient = null
    new SocketQueueClient(host, port)
    var userInput: String = null
    try {
      socketQueueClient = new SocketQueueClient(host, port)
      logInfo("Connected to " + host + ":" + port)

      while (!isStopped) {
        if (socketQueueClient.size() > 0) {
          userInput = new String(socketQueueClient.poll())
          store(userInput)
        } else {
          Thread.sleep(100)
        }

      }

      logInfo("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }

}
