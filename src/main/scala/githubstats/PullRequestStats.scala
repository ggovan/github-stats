package githubstats

import java.nio.file.{Files => nioFiles}

import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel

import scalaj.http._

import net.liftweb.json
import net.liftweb.json.DefaultFormats

/**
 * Spark application that polls the GitHub events API,
 */
object PullRequestStats {

  @transient var wss: WebSocketServer = _

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(nioFiles.createTempDirectory("spark-checkpoint").toString)

    val token = sys.env("GITHUB_TOKEN")

    val eventStream = ssc.receiverStream(new GitHubReceiver[GitHubEvent[Payload]])
    wss = new WebSocketServer(5001)

    val models = eventStream.flatMap { event =>
      if (event.`type` == "PullRequestEvent")
        Some(event.asInstanceOf[GitHubEvent[PullRequestPayload]])
      else
        None
    }.map { event =>
      PRSummaryModel(event.id,
        event.payload.pull_request.base.repo.name,
        event.payload.pull_request.base.repo.language,
        event.payload.pull_request.commits,
        event.payload.pull_request.additions,
        event.payload.pull_request.deletions,
        event.payload.pull_request.changed_files,
        event.payload.pull_request.created_at,
        event.payload.pull_request.closed_at)
    }
  
    val newModels = IdUtil.filterDuplicates(models)

    newModels.map { pr =>
      implicit val formats = GitHubFormats.format
      json.Serialization.write(pr)
    }
      .foreachRDD(rdd =>
        wss.send(rdd.collect().mkString("[", ",", "]")))

    wss.start

    ssc.start()
    ssc.awaitTermination()
  }
} 
