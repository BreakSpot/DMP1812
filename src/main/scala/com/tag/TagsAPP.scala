package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * App标签
  */
object TagsAPP extends Tags {

  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //获取row类型数据
    val row = arg(0).asInstanceOf[Row]
    //获取字典文件
    val app_dir = arg(1).asInstanceOf[Map[String, String]]
    //获取appname appid
    var appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if(StringUtils.isBlank(appname))
      appname = app_dir.getOrElse(appid, "unknow")
    list:+=("APP"+appname, 1)

    list
  }
}
