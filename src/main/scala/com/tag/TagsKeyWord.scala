package com.tag

import org.apache.spark.sql.Row

object TagsKeyWord extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = arg(0).asInstanceOf[Row]
    val stopwords = arg(1).asInstanceOf[Map[String, Int]]
    //关键字
    val keywords = row.getAs[String]("keywords").split("\\|").filter(
      word=>word.length>=3 && word.length<=8 && !stopwords.keySet.contains(word)
    ).foreach(word=>list:+=("K"+word, 1))
    list
  }
}
