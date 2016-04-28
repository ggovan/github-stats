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

  val AccessControlAllowAll = HttpHeaders.RawHeader(
    "Access-Control-Allow-Origin", "*"
  )
  val AccessControlAllowHeadersAll = HttpHeaders.RawHeader(
    "Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept"
  )
  val ContentTypeHeader = HttpHeaders.RawHeader(
     "Content-Type", "application/json"
  )
  
  val filesRoute =
    path("files") {
      get {
        parameters("limit" ? 50) { (limit) =>
          respondWithHeaders(AccessControlAllowAll, AccessControlAllowHeadersAll, ContentTypeHeader) {
            complete { GithubStats.gitHubStats.fileCounts(limit).map{(FilesCount.apply _).tupled} }
          }
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