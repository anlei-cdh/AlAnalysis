package com.al.util

import java.io.{BufferedReader, FileReader}
import java.util.StringTokenizer

import com.al.entity.Training

import scala.collection.mutable.ListBuffer

object WordSplitUtil {

  def main(args: Array[String]): Unit = {
    println(getTrainingList("training/gender.txt"))
  }

  /**
    * 训练集信息
    * label,title -> ListBuffer[Training]
    * @param path
    * @return
    */
  def getTrainingList(path: String): ListBuffer[Training] = {
    val list: ListBuffer[Training] = ListBuffer[Training]()
    val reader: FileReader  = new FileReader(path)
    val br: BufferedReader  = new BufferedReader(reader)
    var line: String = br.readLine()
    while (line != null) {
      println(line)
      val training: Training = new Training()
      val tokenizerLine: StringTokenizer = new StringTokenizer(line, ",")
      val label: Int = Integer.parseInt(tokenizerLine.nextToken())
      val title: String = tokenizerLine.nextToken()
      training.label = label
      training.title = title
      list.append(training)
      line = br.readLine()
    }
    br.close()
    reader.close()
    return list
  }

}
