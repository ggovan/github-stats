package githubstats

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import net.liftweb.json
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JObject
import java.util.Calendar
import redis.RedisClient

object GithubStreaming {
  
   def main(args: Array[String]): Unit = {
     
      val sc = new SparkContext()
      val ssc = new StreamingContext(sc, Seconds(2))
      val eventStream = ssc.receiverStream(new GitHubReceiver())
      
      val eventTypes = eventStream.map( event => event.`type`)
        .countByValue()
        .map( { case (eventType, count) => (eventType, count.toString) })
      
      eventTypes.print()
      
      eventTypes.foreachRDD { rdd => {
              
        val emptyObject: JObject = JObject(List())
        val eventCounts = rdd.aggregate(emptyObject) (
          (acc, value) => ( acc ~ value),
          (acc1, acc2) => ( acc1 ~ acc2 )
        )
       
        if (!eventCounts.values.isEmpty) {
          val now = Calendar.getInstance().getTimeInMillis().toString()
          
          val timestamp = ("timestamp", now)
          val eventsJson = json.compact(json.render(eventCounts ~ timestamp))
         
          implicit val akkaSystem = akka.actor.ActorSystem()
          val redisClient = RedisClient(sys.env("REDIS_HOST"), sys.env("REDIS_PORT").toInt)
          redisClient.publish("spark-stream", eventsJson)
        }
        
      }}
      
      ssc.start()
      ssc.awaitTermination()
   }
   
  
}


