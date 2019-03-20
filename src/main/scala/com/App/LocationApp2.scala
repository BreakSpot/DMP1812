package com.App

import com.utils.LocationUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LocationApp2 {
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

    val rdd = df.map(row=>{
      val provincename = row.getString(24)
      val cityname = row.getString(25)
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

      ((provincename, cityname), (ysrequset, yxrequset, adrequest, cybid, cybid, shows, clicks, dspcost, dspapy))
    })

    val res = rdd.groupByKey().map(row => {
      val sum: Iterator[(Int, Int, Int, Int, Int, Int, Int, Double, Double)] = row._2.iterator
      var ysrequset = 0
      var yxrequset = 0
      var adrequest = 0
      var cybid = 0
      var cybidsuccees = 0
      var shows = 0
      var clicks = 0
      var dspcost = 0.0
      var dspapy = 0.0
      while (sum.hasNext) {
        val tuple = sum.next()
        ysrequset += tuple._1
        yxrequset += tuple._2
        adrequest += tuple._3
        cybid += tuple._4
        cybidsuccees += tuple._5
        shows += tuple._6
        clicks += tuple._7
        dspcost += tuple._8
        dspapy += tuple._9
      }
      (row._1._1, row._1._2, ysrequset, yxrequset, adrequest, cybid, cybidsuccees, shows, clicks, dspcost, dspapy)
    })

    print(res.collect.toBuffer)
    sc.stop()
  }

}
