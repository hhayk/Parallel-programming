package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def rec(acc: Int, arr: List[Char]): Boolean = arr match {
      case Nil => acc == 0
      case x :: xs => {
        x match {
          case '(' => rec(if (acc < 0) -1 else acc + 1, xs)
          case ')' => rec(if (acc <= 0) -1 else acc - 1, xs)
          case _ => rec(acc, xs)
        }
      }
    }

    rec(0, chars.toList)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) = {
      def rec(acc: (Int, Int), arr: List[Char]): (Int, Int) = arr match {
        case Nil => acc
        case x :: xs => {
          x match {
            case '(' => rec((acc._1 + 1, acc._2), xs)
            case ')' => rec(if (acc._1 > 0) (acc._1 - 1, acc._2) else (acc._1, acc._2 + 1), xs)
            case _ => rec(acc, xs)
          }
        }
      }

      rec((arg1, arg2), chars.slice(idx, until).toList)
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = (until - from) / 2
        val (l, r) = parallel(reduce(from, mid), reduce(mid, until))
        val m = math.min(l._1, r._2)
        (l._1 + r._1 - m, l._2 + r._2 - m)
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
