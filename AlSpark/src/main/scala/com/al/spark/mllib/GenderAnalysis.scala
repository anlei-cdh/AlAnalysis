package com.al.spark.mllib

import com.al.config.Config
import com.al.dao.TrainingDao
import com.al.entity.Training
import com.al.util.{FileUtil, SparkUtil}
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.feature.HashingTF

/**
  * Created by An on 2016/12/06.
  * 性别分类计算
  */
object GenderAnalysis {

  def runAnalysis(): Unit = {
    /**
      * 获得SparkContext
      */
    val sc = SparkUtil.getSparkContext(this.getClass)
    /**
      * 读取日志
      */
    val lines = sc.textFile(Config.input_path)
    /**
      * 解析日志并过滤其中的错误内容
      */
    val filter = SparkUtil.getFilterLog(lines).cache()

    val model = SVMModel.load(sc, "model/svm_model")
    val tf = new HashingTF(numFeatures = 10000)

    /**
      * 计算性别信息(map,reduce)
      */
    val map = filter.map(
      log => {
        val training: Training = new Training(pv = 1,uv = 1,ip = 1)
        training.genderId = model.predict(tf.transform(FileUtil.getTrainingString(log.getClearTitle()))).toInt
        training.uvs += log.Uuid
        training.ips += log.Ip
        (training.genderId, training)
      }
    ).cache()
    /**
      * 计算维度pv,uv,ip的通用ReduceByKey
      */
    val reduce = map.reduceByKey((m, n) => {
      m.pv += n.pv
      m.uvs ++= n.uvs
      m.ips ++= n.ips
      m.uv = m.uvs.size
      m.ip = m.ips.size
      (m)
    })

    val list: List[Training] = reduce.values.collect().toList
    list.foreach(item => {
      item.day = Config.day
    })
    sc.stop()
    /**
      * 写入数据库
      */
    TrainingDao.saveGenderList(list)
  }

  def main(args: Array[String]): Unit = {
    runAnalysis()
  }
}
