package com.App

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.utils.{AppNameUtil, AppSchemUtil, LocationUtil, LocationUtilV2}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MultimediaApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")


    val ipInfo = sc.textFile("F:/bigdata/DMP/app_dict.txt")
      .map(_.split("\t+"))
      .filter(_.length>=6)
      .flatMap(arr=>(Map(arr(4) -> arr(1))))

    val ipInfoArr = ipInfo.collect.toMap
    val broadcastIpInfo = sc.broadcast(ipInfoArr)

    //
    //    val Schema = StructType(
    //      Seq(
    //        StructField("gname", StringType, false),
    //        StructField("gurl", StringType, false)
    //      )
    //    )
    //    val df = sqlContext.createDataFrame(rdd, Schema)
    //    df.registerTempTable("t_game")
    //    val res = sqlContext.sql("select * from t_game where gurl like 'disney.chukong'")
    //    res.show()

    val df = sqlContext.read.parquet("F:/bigdata/DMP/out")

    val row: RDD[(String, String, Int, Int, Int, Int, Int, Int, Int, Double, Double)] = df.rdd.map(row => {
      val ipInfoValue: Map[String, String] = broadcastIpInfo.value
      val url = row.getAs[String]("appid")
      val oldname = row.getAs[String]("appname")
      val name = ipInfoValue.getOrElse(url, oldname)

      val requestmode = row.getInt(8)
      val processnode = row.getInt(35)
      val iseffective = row.getInt(30)
      val isbilling = row.getInt(31)
      val isbid = row.getInt(39)
      val adorderid = row.getInt(2)
      val iswin = row.getInt(42)
      val winprice = row.getDouble(41)
      val adpayment = row.getDouble(75)

      val ysrequset =  LocationUtil.isYsrequset(processnode,processnode)
      val yxrequset = LocationUtil.isYxrequset(requestmode, processnode)
      val adrequest = LocationUtil.isAdrequest(requestmode:Int, processnode:Int)
      val cybid = LocationUtil.isCybid(iseffective:Int, isbilling:Int, isbid:Int)
      val cybidsuccees = LocationUtil.isCybidsuccees(iseffective:Int, isbilling:Int, isbid:Int,adorderid:Int)
      val shows = LocationUtil.isShows(requestmode:Int, iseffective:Int)
      val clicks = LocationUtil.isClicks(requestmode:Int, iseffective:Int)
      val dspcost = LocationUtil.isDspcost(requestmode:Int, iseffective:Int, winprice:Double)
      val dspapy = LocationUtil.isDspapy(requestmode:Int, iseffective:Int,adpayment:Double)

      (url, name, ysrequset, yxrequset, adrequest, cybid, cybid, shows, clicks, dspcost, dspapy)
    })
    row.saveAsTextFile("")


    sc.stop()
  }
}
