package com.tag

import ch.hsr.geohash.GeoHash
import com.utils.{BaiduLBSHandler, JedisConnectionPool}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LongAndLat2Baidu {
  def main(args: Array[String]): Unit = {
    //模拟企业开发模式，首先判断一下目录是否为空
    if(args.length != 1){
      println("目录不正确，退出程序！")
      sys.exit()
    }
    //创建一个集合，储存一下输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      //处理数据，采取scala的序列方式，性能比Java高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //我们要采取snappy压缩方式，因为我们用的是spark1.6版本，到2.0以后就不用配置了
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")
    //拿到数据的经纬度
    sqlContext.read.parquet(inputPath).select("lat","long").filter(
      """
        |cast(long as double) >= 73 and cast(long as double) <= 136 and
        |cast(lat as double) >= 3 and cast(lat as double) <= 54
      """.stripMargin
    ).distinct()
      .foreachPartition(f=>{
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(f=>{
          val long = f.getAs[String]("long")
          val lat = f.getAs[String]("lat")
          //通过百度的逆地址解析，获取到商圈信息
          val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble,8)
          //进行SN校验
          val baiduSN = BaiduLBSHandler.parseBusinessTagBy(long, lat)
          //存入Redis
          jedis.set(geoHash, baiduSN)
        })
        jedis.close()
      })
    sc.stop()
  }
}
