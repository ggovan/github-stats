package githubstats

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.redislabs.provider.redis._

object GithubStreaming {
  
   def main(args: Array[String]): Unit = {
     
      val sc = new SparkContext()
      val ssc = new StreamingContext(sc, Seconds(2))
      val eventStream = ssc.receiverStream(new GitHubReceiver())
      
      val eventTypes = eventStream.map( event => event.`type`)
        .countByValue()
        .map( { case (eventType, count) => (eventType, count.toString) })
      
      eventTypes.print()
      
      val redisDB = (sys.env("REDIS_HOST"), sys.env("REDIS_PORT").toInt)
      eventTypes.foreachRDD { rdd => {
          sc.toRedisZSET(rdd, "event:types", redisDB)
        }
      }
      
      ssc.start()
      ssc.awaitTermination()
   }
   
  
}


