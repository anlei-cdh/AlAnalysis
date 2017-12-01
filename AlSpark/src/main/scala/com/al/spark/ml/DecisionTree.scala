package com.al.spark.ml

import java.sql.{Connection, PreparedStatement}

import com.al.basic.BasicDao
import com.al.config.Config
import com.al.db.DBHelper
import com.al.entity.DataResult
import com.al.spark.ml.LogisticRegression.Lr
import com.al.util.{MLUtil, TrainingUtil, WordSplitUtil}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object DecisionTree {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

//    val path = "model/dt"
//    val numFeatures = 10000
//
//    /**
//      * 训练集
//      */
//    val trainingDataFrame = spark.createDataFrame(TrainingUtil.trainingData).toDF("id", "text", "label")
//    /**
//      * 分词,向量化
//      */
//    val training = MLUtil.hashingFeatures(trainingDataFrame, numFeatures).select("label", "features")
//
//    /**
//      * 决策树模型
//      */
//    val dt = new DecisionTreeClassifier()
//    /**
//      * 保存模型
//      */
//    dt.fit(training).write.overwrite().save(path)
//
//    /**
//      * 测试集
//      */
//    val testDataFrame = spark.createDataFrame(TrainingUtil.testData).toDF("id", "text")
//    /**
//      * 分词,向量化
//      */
//    val test = MLUtil.hashingFeatures(testDataFrame, numFeatures).select("features")
//    /**
//      * 读取模型
//      */
//    val model = DecisionTreeClassificationModel.load(path)
//    /**
//      * 分类结果
//      */
//    val result = model.transform(test)
//
//    result.show(false)

    // saveDecisionTreeModel(spark)
    // testDecisionTree(spark)

    processDecisionTree(spark)

    spark.stop()
  }

  def processDecisionTree(spark: SparkSession): Unit = {
    val dataframe = spark.read.json(Config.input_path)
    val selectdf = dataframe.selectExpr("uuid", "ip", "title", "title AS text")

    import spark.implicits._
    val dataset = selectdf.as[Lr]
    val wordsplit = dataset.map{lr =>
      lr.text = WordSplitUtil.getWordSplit(lr.title)
      lr
    }.filter(lr => lr.text != null).select("uuid","ip",Config.text)

    val idf = MLUtil.hashingFeatures(wordsplit, Config.numFeatures).select("uuid","ip",Config.features)

    val model = DecisionTreeClassificationModel.load(Config.dt_path)
    val prediction = model.transform(idf).select("uuid","ip","prediction")

    prediction.createOrReplaceTempView("dftable")
    val result = spark.sql("SELECT prediction,COUNT(1) pv,COUNT(DISTINCT(uuid)) uv,COUNT(DISTINCT(ip)) ip FROM dftable GROUP BY prediction")

    result.show(false)

//    result.foreachPartition(records => {
//      if (!records.isEmpty) {
//        val conn: Connection = DBHelper.getConnectionAtFalse()
//        val sql: String = "INSERT INTO mllib_gender_data(genderid,`day`,pv,uv,ip) VALUES (#{prediction},#{day},#{pv},#{uv},#{ip}) on duplicate key update pv = values(pv),uv = values(uv),ip = values(ip)"
//        val pstmt: PreparedStatement = conn.prepareStatement(BasicDao.getRealSql(sql))
//        var count: Int = 0
//
//        records.foreach {
//          record => {
//            val dataResult = new DataResult
//            dataResult.prediction = record.getAs[Double]("prediction").toInt
//            dataResult.pv = record.getAs[Long]("pv").toInt
//            dataResult.uv = record.getAs[Long]("uv").toInt
//            dataResult.ip = record.getAs[Long]("ip").toInt
//            dataResult.day = Config.day
//
//            count += 1
//            DBHelper.setPreparedSqlexecuteBatch(conn, pstmt, sql, count, dataResult)
//          }
//        }
//
//        DBHelper.commitClose(conn, pstmt)
//      }
//    })
  }

  def saveDecisionTreeModel(spark: SparkSession): Unit = {
    val trainingData = WordSplitUtil.getTrainingSplitList(Config.training_channel_path)

    val trainingDataFrame = spark.createDataFrame(trainingData).toDF(Config.label, Config.text)
    val training = MLUtil.hashingFeatures(trainingDataFrame, Config.numFeatures).select(Config.label, Config.features)

    val dt = new DecisionTreeClassifier()
      .setImpurity("entropy")

    dt.fit(training).write.overwrite().save(Config.dt_path)
  }

  def testDecisionTree(spark: SparkSession): Unit = {
    val testDataFrame = spark.createDataFrame(TrainingUtil.testDtData).toDF(Config.id, Config.text)
    val test = MLUtil.hashingFeatures(testDataFrame, Config.numFeatures).select(Config.features)

    val model = DecisionTreeClassificationModel.load(Config.dt_path)
    val result = model.transform(test)

    result.show(false)
  }

}
