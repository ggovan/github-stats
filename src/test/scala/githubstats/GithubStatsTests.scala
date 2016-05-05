package githubstats

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers._
import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary

class GithubStatsTests extends FunSuite with SharedSparkContext {
  
  test("Testing initializing spark context") {
    val list = List(1, 2, 3, 4)
    val rdd = sc.parallelize(list)

    assert(rdd.count === list.length)
  }

  val extensions = List("", ".js", ".java", ".scala", ".html", ".rb", ".md", ".h", ".c", ".hs")
  val fileNameGen = for {
    n <- arbitrary[String]
    m <- Gen.oneOf(extensions)
  } yield n + m

  test("file counts returns less than the limit") {
    check(forAll(Gen.listOf(fileNameGen), arbitrary[Int].suchThat(_ >= 0)) {(fileNames: List[String], limit: Int) =>

      val ghs = GithubStats(sc, "path", Nil, sc.emptyRDD, sc.parallelize(fileNames))

      ghs.fileCounts(limit).length <= limit
    })
  }

  test("file counts are ordered descending") {
    check(forAll(Gen.listOf(fileNameGen), arbitrary[Int].suchThat(_ >= 0)) {(fileNames: List[String], limit: Int) =>

      val ghs = GithubStats(sc, "path", Nil, sc.emptyRDD, sc.parallelize(fileNames))

      ghs.fileCounts(limit).sliding(2, 1).forall(xs => xs.length < 2 || xs(0)._2 >= xs(1)._2)
    })
  }
}