package com.App

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.codehaus.jackson.map.MapperConfig.ConfigFeature

/**
  * 根据需求将数据写入mysql
  */
object TestMysql {
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
    //使用sql处理文件
    val res = sqlContext.sql("select provincename,cityname,count(*) as cts from log group by provincename,cityname")
    res.show()
    //储存本地数据文件json格式
//    res.write.json(outputPath)

    //接下来我们将数据进行储存mysql中，需要加载application.conf文件中的配置
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    res.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),
      load.getString("jdbc.tableName"), props)
    sc.stop()

  }
}
