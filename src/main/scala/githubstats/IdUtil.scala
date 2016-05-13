package githubstats

import org.apache.spark.streaming.dstream.DStream
import scala.util.Try
import scala.reflect.ClassTag

object IdUtil {
  
  def filterDuplicates[T <: HasId:ClassTag](models: DStream[T]): DStream[T] = {
    val maxAndPrevIds = getMaxAndPreviousIds(models)
    
     models.map(((), _))
      .join(maxAndPrevIds)
      .map(_._2)
      .filter { case (model, (_, maxId)) => maxId.fold(true)(_ < model.id.toLong) }
      .map { case (model, _) => model }
  }
  
   /**
   * Gets the (latest,previous_latest) ids as nested options for maximum safety.
   * It's also keyed by Unit,
   * 1) that's how the update state works, has to be done by key as there is
   *    no method for the whole rdd
   * 2) we need it for the join afterwards
   */
  private def getMaxAndPreviousIds[T <: HasId](models: DStream[T]): DStream[(Unit,(Option[Long],Option[Long]))] =
    models.map(e => ((), e.id.toLong))
      .updateStateByKey(getMaxAndPrev)
  
  /**
   * Extracted from [[#getMaxAndPreviousIds]] for testing.
   */
  private[githubstats] def getMaxAndPrev(ids: Seq[Long], state: Option[(Option[Long], Option[Long])]): Option[(Option[Long], Option[Long])] = 
    Some((
      Try(ids.max).toOption
        .fold(state.fold[Option[Long]](None)(_._1))(Some(_)),
      state.fold[Option[Long]](None)(_._1)))
}