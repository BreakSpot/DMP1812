package com.tag

import org.apache.spark.sql.Row

object TagsDevice extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(arg: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = arg(0).asInstanceOf[Row]
    //操作系统
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list :+= ("D00010001", 1)
      case 2 => list :+= ("D00010002", 1)
      case 3 => list :+= ("D00010003", 1)
      case 4 => list :+= ("D00010004", 1)
    }
    //联网方式
    val network = row.getAs[Int]("networkmannerid")
    network match{
      case 3 => list :+= ("D00020001", 1)
      case 5 => list :+= ("D00020002", 1)
      case 2 => list :+= ("D00020003", 1)
      case 1 => list :+= ("D00020004", 1)
      case _  => list :+= ("D00020005", 1)
    }
    //运营商
    val isp = row.getAs[Int]("ispid")
    isp match{
      case 1 => list :+= ("D00030001", 1)
      case 2 => list :+= ("D00030002", 1)
      case 3 => list :+= ("D00030003", 1)
      case 4 => list :+= ("D00030004", 1)
    }
    list
  }
}
