package popeye.util

import akka.util.ByteString
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

/**
 * @author Andrey Stepachev
 */
class LineDecoder(maxSize: Int = 2048) {

  var buffer: ByteString = ByteString.empty

  /**
   * Try to find delimited string. Delimiters are '\r\n' or '\n'
   * @param input string
   * @return (Some(parsedFragment), Some(reminder))
   */
  def tryParse(input: ByteString): (Option[ByteString], Option[ByteString]) = {
    if (input.length == 0)
      return (None, None)
    val matchPosition = input.indexOf('\n')
    if (matchPosition == -1) {
      if (input.length > maxSize)
        throw new IllegalArgumentException("Line too big")
      (None, Some(input))
    } else {
      if (matchPosition > maxSize)
        throw new IllegalArgumentException("Line too big")
      val remainder = input.drop(matchPosition + 1)
      val cleanInput = cutSlashR(input.take(matchPosition))
      (if (cleanInput.length > 0) Some(cleanInput) else None,
        if (remainder.isEmpty) None else Some(remainder))
    }
  }

  //  def traverse(input: ByteString): Traversable = new Traversable[ByteString] {
  //    def foreach[U](f: (ByteString) => U) {
  //      @tailrec
  //      def next(i: ByteString): ByteString
  //    }
  //  }

  @inline
  private[this] def cutSlashR(input: ByteString): ByteString = {
    if (input.length > 0 && input.last == '\r')
      input.dropRight(1)
    else
      input
  }
}

object LineDecoder {

  def split(str: String, separator: Char, preserveAllTokens: Boolean): mutable.IndexedSeq[String] = {
    val len = str.length
    if (len == 0)
      ArrayBuffer.empty
    val list = new ArrayBuffer[String]
    var i = 0
    var start = 0
    var matched = false
    var lastMatch = false
    while (i < len) {
      if (str(i) == separator) {
        if (matched || preserveAllTokens) {
          list += str.substring(start, i)
          matched = false
          lastMatch = true
        }
        i += 1
        start = i
      } else {
        lastMatch = false
        matched = true
        i += 1
      }
    }
    if (matched || (preserveAllTokens && lastMatch)) {
      list += str.substring(start, i)
    }
    list
  }
}
