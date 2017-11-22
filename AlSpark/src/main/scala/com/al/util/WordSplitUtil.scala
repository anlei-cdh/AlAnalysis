package com.al.util

import java.io.{BufferedReader, FileReader}
import java.util.{Collection, Iterator, StringTokenizer}

import org.ansj.app.keyword.{KeyWordComputer, Keyword}

import scala.collection.mutable.ListBuffer

object WordSplitUtil {

  def main(args: Array[String]): Unit = {
    println(getTrainingSplitList("training/gender.txt"))
  }

  /**
    * 训练集信息
    * Training -> List[(Int, String)]
    * @param path
    * @return
    */
  def getTrainingSplitList(path: String): List[(Int, String)] = {
    val kwc: KeyWordComputer = new KeyWordComputer(10)
    val listBuffer: ListBuffer[(Int, String)] = ListBuffer[(Int, String)]()

    val reader: FileReader  = new FileReader(path)
    val br: BufferedReader  = new BufferedReader(reader)
    var line: String = br.readLine()
    while (line != null) {
      val tokenizerLine: StringTokenizer = new StringTokenizer(line, ",")
      val label: Int = Integer.parseInt(tokenizerLine.nextToken())
      val title: String = tokenizerLine.nextToken()

      /**
        * 提关键词
        */
      val result: Collection[Keyword] = kwc.computeArticleTfidf(title)
      if(result.size() >= 1) {
        val iterator: Iterator[Keyword] = result.iterator()
        val titleSplit: StringBuffer = new StringBuffer
        while (iterator.hasNext) {
          titleSplit.append(iterator.next().getName).append(" ")
        }
        listBuffer.append((label,titleSplit.toString.trim))
      }

      line = br.readLine()
    }

    br.close()
    reader.close()

    return listBuffer.toList
  }

}
