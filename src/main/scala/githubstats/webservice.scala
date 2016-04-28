package githubstats

import spray.routing._
import spray.http._
import spray.json._
import spray.httpx.SprayJsonSupport._
import MediaTypes._

import GithubStatsProtocol._

class StatsServiceActor extends akka.actor.Actor with StatsService {

  def actorRefFactory = context

  def receive = runRoute(filesRoute ~ updateRoute)
}

trait StatsService extends HttpService {

  val filesRoute =
    path("files") {
      get {
        respondWithMediaType(`application/json`) {
          complete { GithubStats.gitHubStats.fileCounts().map{(FilesCount.apply _).tupled} }
        }
      }
    }
  
    val updateRoute =
    path("update") {
      get {
        respondWithMediaType(`text/html`) {
          complete { GithubStats.gitHubStats.update().toString() }
        }
      }
    }
}