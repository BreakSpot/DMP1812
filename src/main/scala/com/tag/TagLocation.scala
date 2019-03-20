package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagLocation extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = arg(0).asInstanceOf[Row]

    val province = row.getAs[String]("provincename")
    if(StringUtils.isNotBlank(province))
      list :+= ("ZP"+province, 1)

    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(city))
      list :+= ("ZC"+city, 1)
    list
  }
}