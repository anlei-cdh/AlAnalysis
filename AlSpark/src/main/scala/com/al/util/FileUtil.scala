package com.al.util

import java.io.{BufferedReader, FileReader}
import java.util.{Collection, Iterator, StringTokenizer}

import com.al.entity.Training
import org.ansj.app.keyword.{KeyWordComputer, Keyword}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object FileUtil {
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

	/**
		* ListBuffer[Training] -> ArrayBuffer(1, 特朗普 中国 挑衅)
		* @param list
		* @return
		*/
	def getTrainingArrayBuffer(list: ListBuffer[Training]): ArrayBuffer[String] = {
		val arr = new ArrayBuffer[String]
		val kwc: KeyWordComputer = new KeyWordComputer(10)
		for(i <- 0 until list.size) {
			val training: Training = list(i)
			val result: Collection[Keyword] = kwc.computeArticleTfidf(training.title)
			if(result.size() >= 1) {
				val iterator: Iterator[Keyword] = result.iterator()
				val key: StringBuffer = new StringBuffer
				key.append(training.label).append(",")
				while (iterator.hasNext) {
					key.append(iterator.next().getName).append(" ")
				}
				arr += key.toString.trim
				println(key.toString.trim)
			}
		}
		return arr
	}

	/**
		* 特朗普三天内两度挑衅中国 -> ArrayBuffer(特朗普 中国 挑衅)
		* @param title
		* @return
		*/
	def getTrainingString (title: String): ArrayBuffer[String] = {
		val arr = new ArrayBuffer[String]
		val kwc: KeyWordComputer = new KeyWordComputer(10)
		val result: Collection[Keyword] = kwc.computeArticleTfidf(title)
		if(result.size() >= 2) {
			val iterator: Iterator[Keyword] = result.iterator()
			while (iterator.hasNext) {
				arr += iterator.next().getName
			}
		}
		return arr
	}

	def main(args: Array[String]): Unit = {
		println(getTrainingString("特朗普三天内两度挑衅中国"))
	}
}