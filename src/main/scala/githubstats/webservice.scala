package githubstats

import spray.can.Http
import spray.routing._
import spray.http._
import spray.json._
import spray.httpx.SprayJsonSupport._
import MediaTypes._

import akka.actor.ActorSystem
import akka.actor.Props
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration.DurationInt

import GithubStatsProtocol._

object StatsService {
  def start(githubStats: GithubStats){
    implicit val system = ActorSystem("on-spray-can")

    // create and start our service actor
    val service = system.actorOf(Props(new StatsServiceActor(githubStats)), "demo-service")

    implicit val timeout = Timeout(200.seconds)
    // start a new HTTP server on port 8080 with our service actor as the handler
    IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 9021)
  }
}

class StatsServiceActor(var githubStats: GithubStats) extends akka.actor.Actor with StatsService {

  def actorRefFactory = context

  def receive = runRoute(filesRoute ~ updateRoute ~ commitsRoute)
  
  override def currentState() = githubStats
  override def currentState(state: GithubStats) { this.githubStats = state }
}

trait StatsService extends HttpService {
  
  def currentState():GithubStats
  def currentState(state:GithubStats): Unit

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
          if(limit <= 0){
            respondWithStatus(StatusCodes.BadRequest) {
              complete("Limit must be positive")
            }
          }
          else {
            respondWithHeaders(AccessControlAllowAll, AccessControlAllowHeadersAll, ContentTypeHeader) {
              complete { currentState.fileCounts(limit).map{(FilesCount.apply _).tupled} }
            }
          }
        }
      }
    }
  
  val commitsRoute = 
    path("commits") {
      get {
          respondWithHeaders(AccessControlAllowAll, AccessControlAllowHeadersAll, ContentTypeHeader) {
            complete { currentState.commitFileCounts().map{(CommitDetails.apply _).tupled} }
          }
      }
  }
  
    val updateRoute =
    path("update") {
      get {
        respondWithMediaType(`text/html`) {
          complete { 
            currentState(currentState.update())
            true.toString()
          }
        }
      }
    }
}