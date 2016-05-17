package githubstats

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import scalaj.http.Http
import scalaj.http.HttpResponse
import net.liftweb.json

class GitHubReceiver[T : Manifest]() extends Receiver[T](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

    val token = sys.env("GITHUB_TOKEN")
    
    while (!isStopped) {
      Thread sleep 2000
      
      val response = Http("https://api.github.com/events?access_token=" + token + "&per_page=100").asString
      
      val rateRemaining = response.headers("X-RateLimit-Remaining")
      if (rateRemaining == 0) {
        val resetTime = response.headers("X-RateLimit-Reset") 
        println("Ran out of API requests, stopping. X-RateLimit-Reset = " + resetTime)
        println("Message body = " )
        println(response.body)
      }
      
      implicit val formats = GitHubFormats.format
      val events =  json.parse(response.body)
        .extract[List[T]]
      
       
      events.foreach { store(_) } 
    }
  
  }
}