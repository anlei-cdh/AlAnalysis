package com.al.util

object TrainingUtil {

  val trainingData = Seq(
    (1, "Hi I heard about Spark", 0.0),
    (2, "I wish Java could use case classes", 1.0),
    (3, "Logistic regression models are neat", 2.0)
  )

  val testData = Seq(
    (1, "Hi I'd like spark"),
    (2, "I wish Java could use goland"),
    (3, "Linear regression models are neat"),
    (4, "Logistic regression models are neat")
  )

  val testLrData = Seq(
    (1, "特朗普 中国 挑衅"),
    (2, "市场经济国 中国 承认 地位"),
    (3, "恒大 中超 亚洲 重返"),
    (4, "辣妈 章泽天 诺奖 得主")
  )

  val testDtData = Seq(
    (1, "特朗普 中国 挑衅"),
    (2, "市场经济国 中国 承认 地位"),
    (3, "恒大 中超 亚洲 重返"),
    (4, "辣妈 章泽天 诺奖 得主")
  )

  val clusteringData = Seq(
    (1001, "时政 国际 军事 体育 体育 国际 体育"),
    (1002, "娱乐 体育"),
    (1003, "体育 体育 国际 体育"),
    (1004, "军事 体育 体育"),
    (1005, "时政 财经 财经 体育 军事")
  )
}
