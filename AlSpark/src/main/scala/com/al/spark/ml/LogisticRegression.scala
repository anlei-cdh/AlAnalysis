package com.al.spark.ml

import java.sql.{Connection, PreparedStatement}

import com.al.basic.BasicDao
import com.al.config.Config
import com.al.db.DBHelper
import com.al.entity.DataResult
import com.al.util.{MLUtil, TrainingUtil, WordSplitUtil}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object LogisticRegression {

  case class Lr(uuid:String,ip:String,title: String,var text: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    // saveLogisticRegressionModel(spark)
    // testLogisticRegression(spark)
    processLogisticRegression(spark)

    spark.stop()
  }

  def processLogisticRegression(spark: SparkSession): Unit = {
    val dataframe = spark.read.json(Config.input_path)
    val selectdf = dataframe.selectExpr("uuid", "ip", "title", "title AS text")

    import spark.implicits._
    val dataset = selectdf.as[Lr]
    val wordsplit = dataset.map{lr =>
      lr.text = WordSplitUtil.getWordSplit(lr.title)
      lr
    }.filter(lr => lr.text != null).select("uuid","ip",Config.text)

    val idf = MLUtil.idfFeatures(wordsplit, Config.numFeatures).select("uuid","ip",Config.features)

    val model = LogisticRegressionModel.load(Config.lr_path)
    val prediction = model.transform(idf).select("uuid","ip","prediction")

    prediction.createOrReplaceTempView("dftable")
    val result = spark.sql("SELECT prediction,COUNT(1) pv,COUNT(DISTINCT(uuid)) uv,COUNT(DISTINCT(ip)) ip FROM dftable GROUP BY prediction")

    result.foreachPartition(records => {
      if (!records.isEmpty) {
        val conn: Connection = DBHelper.getConnectionAtFalse()
        val sql: String = "INSERT INTO mllib_gender_data(genderid,`day`,pv,uv,ip) VALUES (#{prediction},#{day},#{pv},#{uv},#{ip}) on duplicate key update pv = values(pv),uv = values(uv),ip = values(ip)"
        val pstmt: PreparedStatement = conn.prepareStatement(BasicDao.getRealSql(sql))
        var count: Int = 0

        records.foreach {
          record => {
            val dataResult = new DataResult
            dataResult.prediction = record.getAs[Double]("prediction").toInt
            dataResult.pv = record.getAs[Long]("pv").toInt
            dataResult.uv = record.getAs[Long]("uv").toInt
            dataResult.ip = record.getAs[Long]("ip").toInt
            dataResult.day = Config.day

            count += 1
            DBHelper.setPreparedSqlexecuteBatch(conn, pstmt, sql, count, dataResult)
          }
        }

        DBHelper.commitClose(conn, pstmt)
      }
    })
  }

  def saveLogisticRegressionModel(spark: SparkSession): Unit = {
    val trainingData = WordSplitUtil.getTrainingSplitList(Config.training_gender_path)

    val trainingDataFrame = spark.createDataFrame(trainingData).toDF(Config.label, Config.text)
    val training = MLUtil.idfFeatures(trainingDataFrame, Config.numFeatures).select(Config.label, Config.features)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
      .setFamily("binomial") // binomial | multinomial

    lr.fit(training).write.overwrite().save(Config.lr_path)
  }

  def testLogisticRegression(spark: SparkSession): Unit = {
    val testDataFrame = spark.createDataFrame(TrainingUtil.testLrData).toDF(Config.id, Config.text)
    val test = MLUtil.idfFeatures(testDataFrame, Config.numFeatures).select(Config.features)

    val model = LogisticRegressionModel.load(Config.lr_path)
    val result = model.transform(test)

    result.show(false)
  }

}
