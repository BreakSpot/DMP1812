package com.App

import com.utils.LocationUtilV2
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LocationAppV3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      //处理数据，采取scala的序列方式，性能比Java高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //我们要采取snappy压缩方式，因为我们用的是spark1.6版本，到2.0以后就不用配置了
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("F:\\bigdata\\DMP\\out")
    val res = df.map(row=>{
      //获取需要的参数，原始 有效 广告
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
      val requestmode = row.getInt(8)
      val processnode = row.getInt(35)
      val iseffective = row.getInt(30)
      val isbilling = row.getInt(31)
      val isbid = row.getInt(39)
      val adorderid = row.getInt(2)
      val iswin = row.getInt(42)
      val winprice = row.getDouble(41)
      val adpayment = row.getDouble(75)

      //编写工具类。使用集合进行处理
      val reqlist = LocationUtilV2.requestUtil(requestmode,processnode)
      val adlist = LocationUtilV2.requstAD(iseffective,isbilling,isbid,iswin,adorderid, winprice, adpayment)
      val clicklist = LocationUtilV2.rquestShow(requestmode,iseffective)

      ((provincename, cityname), reqlist ++ adlist ++ clicklist)
    }).reduceByKey((list1, list2)=>{
      //List[(Double, Double)]
      //list(1,1,1,0) list(0,1,1,1) 调用zip变成元组list[(1,0),(1,1),(1,1),(0,1)]
      list1.zip(list2).map(t=>t._1+t._2)
      //整理
    }).map(t=>t._1._1+","+t._1._2+","+t._2.mkString(","))

     print(res.collect.toBuffer)
    //存储
//    res.saveAsTextFile("F:\\bigdata\\DMP\\out-LocationApp")

    sc.stop()
  }
}
