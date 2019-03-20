package com.App

import com.utils.{JedisConnectionPool, LocationUtilV2}
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object AppNameV2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("F:/bigdata/DMP/out")

    //读取字典文件
    val dirMap = sc.textFile("F:/bigdata/DMP/app_dict.txt").map(_.split("\t", -1)).filter(_.length>5)
      .map(arr=>(arr(4), arr(1))).collect.toMap

    //广播
    val broadcast = sc.broadcast(dirMap)
    //处理数据
    df.mapPartitions(row=>{
      val jedis = JedisConnectionPool.getConnection()
      var list = new collection.mutable.ListBuffer[(String, List[Double])]
      row.foreach(row=>{
        var appname = row.getAs[String]("appname")
        val appid = row.getAs[String]("appid")
        if(StringUtils.isBlank(appname)){
          appname = jedis.get(appid)
        }
        //所需参数
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
       list += ((appname, reqlist ++ adlist ++ clicklist))
      })
      list.iterator
    }).reduceByKey((list1, list2)=>{
      //List[(Double, Double)]
      //list(1,1,1,0) list(0,1,1,1) 调用zip变成元组list[(1,0),(1,1),(1,1),(0,1)]
      list1.zip(list2).map(t=>t._1+t._2)
      //整理
    }).map(t=>t._1+","+t._2.mkString(",")).saveAsTextFile("f:/bigdata/DMP/appname")

    sc.stop()

  }
}
