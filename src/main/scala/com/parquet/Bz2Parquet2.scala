package com.parquet

import com.utils.NBF
import com.beans.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object Bz2Parquet2 {
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
    //读取文件
    //    val lines = sc.textFile("F:\\bigdata\\DMP\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH.bz2")
    val lines = sc.textFile(inputPath)

    import sqlContext.implicits._

    val Logs: RDD[Log] = lines.map(t => (t.split(",", t.length)))
      .filter(t=>t.length>=85).map(arr =>Log(arr))

    val df: DataFrame = Logs.toDF()

    df.registerTempTable("t_dmp")

    val res = sqlContext.sql("select count(*) from t_dmp")

    res.show()
    df.write.partitionBy("provincename","cityname").parquet("F:\\bigdata\\DMP\\out-2")
    sc.stop()
  }
}

