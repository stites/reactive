package quickcheck

import org.scalacheck.Arbitrary._
import org.scalacheck.Gen._
import org.scalacheck.Prop._
import org.scalacheck._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("emptyTypecheck") = empty.isInstanceOf[H]

  property("insertTypecheck") = forAll { a: Int =>
    insert(a, empty).isInstanceOf[H]
  }

  property("insertOutput") = forAll { a: Int =>
    insert(a, empty) != empty
  }

  property("minOneInput") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("minTwoInputs") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    val smallest = if (a < b) a else b
    findMin(h) == smallest
  }

  property("minVarInputs") = {
    val is = Gen.listOf(Gen.choose(-100, 100)).sample
    val list = is.get
    val smallest = is.get.min
    var heap = empty

    for {
      i <- list
    } heap = insert(i, heap)

    smallest == findMin(heap)
  }

  lazy val genHeap: Gen[H] = for {
    n <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(n, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

  property("isEmptyOutput") = isEmpty(empty)

  property("isEmptyBasic") = forAll { a: Int =>
    val h = insert(a, empty)
    isEmpty(h)
  }

  property("deleteMinTypecheck") = forAll { a: Int =>
    val h = insert(a, empty)
    deleteMin(h).isInstanceOf[H]
  }

  property("deleteMinBasic") = forAll { a: Int =>
    val h = insert(a, empty)
    empty == deleteMin(h)
  }

  property("deleteMinTwoInputs") = forAll { (a: Int, b: Int) =>
    val h = insert(b, insert(a, empty))
    val largest = if (a > b) a else b
    deleteMin(h) == insert(largest, empty)
  }

  property("meldTypecheck") = forAll { (a: Int, b: Int) =>
    val h = meld( insert(a, empty), insert(b, empty) )
    h.isInstanceOf[H]
  }

  property("meldTwoInputs") = forAll { (a: Int, b: Int) =>
    val h = meld( insert(a, empty), insert(b, empty) )
    h == insert(a, insert(b, empty))
  }

  property("hint4") = {
    val a = genHeap.sample.get
    val b = genHeap.sample.get
    val aMin = findMin(a)
    val bMin = findMin(b)

    findMin(meld(a, b)) match {
      case aMin => true
      case bMin => true
    }
  }
}
