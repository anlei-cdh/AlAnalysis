package com.al.spark.ml

import java.sql.{Connection, PreparedStatement}

import com.al.basic.BasicDao
import com.al.config.Config
import com.al.db.DBHelper
import com.al.entity.DataResult
import com.al.spark.ml.LogisticRegression.Lr
import com.al.util.{MLUtil, TrainingUtil, WordSplitUtil}
import org.apache.spark.ml.classification._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DecisionTree {

  val modeltype = "lr" // lr(逻辑回归) | bayes(贝叶斯) | dt(决策树) | rf(随机森林)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    saveDecisionTreeModel(spark)
//    testDecisionTree(spark)
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

    val hashing = MLUtil.hashingFeatures(wordsplit, Config.numFeatures).select("uuid","ip",Config.features)

    val prediction = loadModelTransform(hashing).select("uuid","ip","prediction")

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

    createModelSave(training)
  }

  def testDecisionTree(spark: SparkSession): Unit = {
    val testDataFrame = spark.createDataFrame(TrainingUtil.testDtData).toDF(Config.id, Config.text)
    val test = MLUtil.hashingFeatures(testDataFrame, Config.numFeatures).select(Config.features)

    val result = loadModelTransform(test)
    result.show(false)
  }

  def createModelSave(training: DataFrame): Unit = {
    if(modeltype.equals("lr")) {
      val model = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.001)
        .setFamily("multinomial") // binomial | multinomial
      model.fit(training).write.overwrite().save(Config.dt_path)
    } else if(modeltype.equals("bayes")) {
      val model = new NaiveBayes()
        .setModelType("multinomial")
      model.fit(training).write.overwrite().save(Config.dt_path)
    } else if(modeltype.equals("dt")) {
      val model = new DecisionTreeClassifier()
        .setImpurity("entropy")
      model.fit(training).write.overwrite().save(Config.dt_path)
    } else if(modeltype.equals("rf")) {
      val model = new RandomForestClassifier()
        .setSubsamplingRate(0.8) // 1.0
        .setNumTrees(15) // 10
      model.fit(training).write.overwrite().save(Config.dt_path)
    }
  }

  def loadModelTransform(dataframe: DataFrame): DataFrame = {
    if(modeltype.equals("lr")) {
      val model = LogisticRegressionModel.load(Config.dt_path)
      return model.transform(dataframe)
    } else if(modeltype.equals("bayes")) {
      val model = NaiveBayesModel.load(Config.dt_path)
      return model.transform(dataframe)
    } else if(modeltype.equals("dt")) {
      val model = DecisionTreeClassificationModel.load(Config.dt_path)
      return model.transform(dataframe)
    } else if(modeltype.equals("rf")) {
      val model = RandomForestClassificationModel.load(Config.dt_path)
      return model.transform(dataframe)
    }
    return null
  }

}
