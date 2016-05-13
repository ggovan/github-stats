package githubstats

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers._
import com.holdenkarau.spark.testing.SharedSparkContext

import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary

class IdUtilTests extends FunSuite {
  
  type OL = Option[Long]
  type OOLOL = Option[(OL,OL)]
  
  test("getMaxAndPrev always gives a Some") {
    check(forAll{(ids: Seq[Long], state: OOLOL) =>
      IdUtil.getMaxAndPrev(ids, state) match {
        case Some(_) => true
        case _ => false
      }
    })
  }
  
  test("getMaxAndPrev with empty models gives None or (x,x)") {
    check(forAll{(state: OOLOL) =>
      IdUtil.getMaxAndPrev(Nil, state) match {
        case None => true
        case Some((x,y)) => x == y
      }
    })
  }
  
  test("getMaxAndPrev new previous is old max") {
    check(forAll{(ids: Seq[Long], max: OL, prev: OL) =>
      IdUtil.getMaxAndPrev(Nil, Some((max, prev))) match {
        case Some((_,nprev)) => nprev == max
        case _ => false
      }
    })
  }
  
  test("getMaxAndPrev max is the max id") {
    check(forAll{(ids: Seq[Long], max: OL, prev: OL) =>
      IdUtil.getMaxAndPrev(ids, Some((max, prev))) match {
        case Some((None,_)) => max.isEmpty && ids.isEmpty
        case Some((Some(x),_)) => if(ids.isEmpty) Some(x) == max else x == ids.max
        case _ => false
      }
    })
  }
  
}