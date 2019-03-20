package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAD extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //参数解析
    val row = args(0).asInstanceOf[Row]
//    val app_dict = args(1).asInstanceOf[Map[String, String]]
//    val stopwords = args(2).asInstanceOf[Map[String, Int]]


    //获取广告参数
    //adspacetype:Int
    //adspacetypename:
    //广告位类型
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list :+= ("LC"+v, 1)
      case v if v > 0 && v <= 9 => list :+= ("LC0"+v, 1)
    }
    val adName = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName, 1)
    }
    //渠道标签
    val channal = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channal,1)
//
//    //app名称
//    var appname = row.getAs[String]("appname")
//    val appid = row.getAs[String]("appid")
//    if(StringUtils.isBlank(appname))
//      appname = app_dict.getOrElse(appid, "unknow")
//    list:+=("APP"+appname, 1)

//    //设备
//    //操作系统
//    val client = row.getAs[Int]("client")
//    client match {
//      case c if c==1 => list :+= ("D00010001", 1)
//      case c if c==2 => list :+= ("D00010002", 1)
//      case c if c==3 => list :+= ("D00010003", 1)
//      case c if c==4 => list :+= ("D00010004", 1)
//    }
//    //联网方式
//    val network = row.getAs[Int]("networkmannerid")
//    network match{
//      case n if n==3 => list :+= ("D00020001", 1)
//      case n if n==5 => list :+= ("D00020002", 1)
//      case n if n==2 => list :+= ("D00020003", 1)
//      case n if n==1 => list :+= ("D00020004", 1)
//      case n  => list :+= ("D00020005", 1)
//    }
//    //运营商
//    val isp = row.getAs[Int]("ispid")
//    isp match{
//      case i if i == 1 => list :+= ("D00030001", 1)
//      case i if i == 2 => list :+= ("D00030002", 1)
//      case i if i == 3 => list :+= ("D00030003", 1)
//      case i if i == 4 => list :+= ("D00030004", 1)
//    }
//
//    //关键字
//    val keywords = row.getAs[String]("keywords").split("\\|")
//    for(word <- keywords){
//      if(word.length>=3 && word.length<=8 && stopwords.get(word)==null){
//        list :+= ("K"+word, 1)
//      }
//    }
//
//    //地域
//    val province = row.getAs[String]("provincename")
//    if(StringUtils.isNotBlank(province))
//      list :+= ("ZP"+province, 1)
//
//    val city = row.getAs[String]("cityname")
//    if(StringUtils.isNotBlank(city))
//      list :+= ("ZC"+city, 1)

    list
  }
}
