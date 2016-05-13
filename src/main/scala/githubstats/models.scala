package githubstats

import net.liftweb.json.CustomSerializer
import net.liftweb.json.JsonAST._

trait HasId {
  def id: String
}

case class BasicEvent(id: String, `type`: String, actor: Actor, repo: Repo) extends HasId

case class GitHubEvent[+P <: Payload](id: String, `type`: String, actor: Actor, repo: Repo, payload: P, public: Boolean, created_at: String) extends HasId

/**
 * Payload for a [[GitHubEvent]].
 * Different Payloads are used for different event types.
 * [[PushEventPayload]] and [[PullRequestPayload]] are currently the only ones in use. 
 */
sealed trait Payload
case class JObjPayload(obj: JValue) extends Payload
case class PushEventPayload(push_id: String, size: Int, commits: List[CommitSummary]) extends Payload
case class PullRequestPayload(action: String, pull_request: PullRequest) extends Payload

case class PullRequest(base: Base, commits: Option[Int], additions: Option[Int], deletions: Option[Int], changed_files: Option[Int])
case class Base(repo: RepoSummary)
case class RepoSummary(language: String, name: String)

case class PRSummaryModel(id:String, repo: String, language: String, commits: Option[Int], additions: Option[Int], deletions:Option[Int], changedFiles: Option[Int]) 
extends HasId

/**
 * JSON deserialiser for GitHubEvents.
 * Deserilaises payloads into the correct [[Payload]] type for their contents.
 * [[PushEventPayload]] and [[PullRequestPayload]] are currently the only ones in use.
 * They are not serialised.
 */
class GitHubEventSerializer extends CustomSerializer[GitHubEvent[_]](implicit format => (
    {case o: JObject =>
      val payload = o\"type" match {
        case JString("PushEvent") => (o\"payload").extract[PushEventPayload]
        case JString("PullRequestEvent") => (o\"payload").extract[PullRequestPayload]
        case _ => JObjPayload(o\"payload")
      }
      GitHubEvent((o\"id").extract[String],(o\"type").extract[String],(o\"actor").extract[Actor],(o\"repo").extract[Repo],payload,(o\"public").extract[Boolean],(o\"created_at").extract[String])
    },
    {case p: GitHubEvent[_] => ???}
))

case class CommitSummary(sha: String, url: String, message: String, author: Author, distinct: Boolean)

case class Actor(id: Double, login: String, gravatar_id: String, url: String, avatar_url: String)
case class Repo(id: Double, name: String, url: String)
case class Author(name: String, email: String, date: Option[String])
case class Tree(sha: String, url: String)
case class Commit(author: Author, committer: Author, message: String, tree: Tree, url: String, comment_count: Int)
case class Contributor(login: String, id: Double, avatar_url: String, gravatar_id: String, url: String, html_url: String,
  followers_url: String, following_url: String, gists_url: String, starred_url: String, subscriptions_url: String,
  organizations_url: String, repos_url: String, events_url: String, received_events_url: String, `type`: String,
  site_admin: Boolean
  )
case class Parents(sha: String, url: String, html_url: String)
case class Stats(total: Option[Int], additions: Option[Int], deletions: Option[Int])
case class Files(sha: String, filename: String, status: String, additions: Int, deletions: Int, changes: Int,
  blob_url: String, raw_url: String, contents_url: String, patch: Option[String])
    
case class CommitResponse(sha: String, commit: Commit, url: String, html_url: String, comments_url: String, author: JObject/*Contributor*/,
  committer: JObject/*Contributor*/, parents: List[Parents], stats: Stats, files: List[Files])
  
import spray.json._
  
object GithubStatsProtocol extends DefaultJsonProtocol {
  implicit val filesCountFormat = jsonFormat2(FilesCount)
  implicit val commitDetailsFormat = jsonFormat2(CommitDetails)
}

case class FilesCount(extension: String, count: Long)
case class CommitDetails(mostCommonExtension: String, totalCount: Int)