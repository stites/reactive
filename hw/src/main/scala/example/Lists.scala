package example

object Lists {
  /**
   * This method computes the sum of all elements in the list xs.
   *
   * @param xs A list of natural numbers
   * @return The sum of all elements in `xs`
   */
  def sum(xs: List[Int]): Int = xs match {
    case List()   => throw new java.util.NoSuchElementException()
    case List(x)  => x
    case x :: _xs => x + sum(_xs)
  }

  /**
   * This method returns the largest element in a list of integers. If the
   * list `xs` is empty it throws a `java.util.NoSuchElementException`.
   *
   * @param xs A list of natural numbers
   * @return The largest element in `xs`
   * @throws java.util.NoSuchElementException if `xs` is an empty list
   */
  def max(xs: List[Int]): Int = {

    def subroutine (ys: List[Int], m:Int): Int = ys match {
      case List()   => throw new java.util.NoSuchElementException()
      case List(y)  => if (y > m) y else m
      case y :: _ys => if (y > m) subroutine(_ys.tail, y) else subroutine(_ys.tail, m)
    }

    subroutine(xs, Int.MinValue)
  }
}
