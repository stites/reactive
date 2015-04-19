import quickcheck.{BinomialHeap, QuickCheckHeap}
import org.scalacheck._

val qBin = new QuickCheckHeap() with BinomialHeap
val heap = qBin.genHeap.sample.get

val ch = Arbitrary.arbitrary[Int]
Gen.listOf(ch).sample.get

qBin.deleteMin(qBin.insert(22, qBin.empty)) == qBin.empty

qBin.check