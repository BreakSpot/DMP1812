package com.App

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * 地域维度指标
  */
object LocationApp {
  def main(args: Array[String]): Unit = {
    //模拟企业开发模式，首先判断一下目录是否为空
    if(args.length != 2){
      println("目录不正确，退出程序！")
      sys.exit()
    }
    //创建一个集合，储存一下输入输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      //处理数据，采取scala的序列方式，性能比Java高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //我们要采取snappy压缩方式，因为我们用的是spark1.6版本，到2.0以后就不用配置了
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("F:\\bigdata\\DMP\\out")
    df.registerTempTable("log")
    //处理指标
    val res: DataFrame = sqlContext.sql(
      """
        |select provincename,cityname,
        |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as ysrequset,
        |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as yxrequset,
        |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) as adrequest,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as cybid,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) as cybidsuccees,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as shows,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clicks,
        |sum(case when requestmode=1 and iseffective=1 then winprice/1000 else 0 end) as dspcost,
        |sum(case when requestmode=1 and iseffective=1 then adpayment/1000 else 0 end) as dspapy
        |from log group by provincename, cityname
      """.stripMargin)

    res.show()
    //存入数据库
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    res.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),
      load.getString("jdbc.tbn"), props)

    sc.stop()
  }
}
