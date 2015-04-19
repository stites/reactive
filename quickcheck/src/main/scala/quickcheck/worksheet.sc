import quickcheck._

val qBin = new QuickCheckHeap() with BinomialHeap
qBin.check
val qBin1 = new QuickCheckHeap() with Bogus1BinomialHeap
qBin1.check
val qBin2 = new QuickCheckHeap() with Bogus2BinomialHeap
qBin2.check
val qBin3 = new QuickCheckHeap() with Bogus3BinomialHeap
qBin3.check
val qBin4 = new QuickCheckHeap() with Bogus4BinomialHeap
qBin4.check
val qBin5 = new QuickCheckHeap() with Bogus5BinomialHeap
qBin5.check
