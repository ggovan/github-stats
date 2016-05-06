package githubstats

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.redislabs.provider.redis._
import net.liftweb.json
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JObject
import java.util.Calendar

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
        
        val emptyObject: JObject = JObject(List())
        val eventCounts = rdd.aggregate(emptyObject) (
          (acc, value) => ( acc ~ value),
          (acc1, acc2) => ( acc1 ~ acc2 )
        )
       
        val now = Calendar.getInstance()
        val eventsJson = json.compact(json.render(eventCounts))
        val outputRdd = sc.parallelize(List((now.getTimeInMillis().toString(), eventsJson)))
        
        sc.toRedisHASH(outputRdd, "event:counts", redisDB)}
      }
      
      ssc.start()
      ssc.awaitTermination()
   }
   
  
}


