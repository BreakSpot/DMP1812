package com.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 处理字典文件数据，存入redis
  */
object AppDir2Redis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val file = sc.textFile("F:/bigdata/DMP/app_dict.txt")
    file.map(_.split("\t",-1)).filter(_.length>=5)
      .map(arr=>(arr(4), arr(1)))
      //调用foreachPartition，减少Rides传输数据
      //Jedis数据和partition数量相等
      .foreachPartition(ite=>{
        //创建连接,拿到Jedis对象
        val jedis = JedisConnectionPool.getConnection()
        ite.foreach(f=>{
          jedis.set(f._1, f._2)
        })
        jedis.close()
      })

  }
}
