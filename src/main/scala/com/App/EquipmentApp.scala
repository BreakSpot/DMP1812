package com.App

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object EquipmentApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("F:/bigdata/DMP/out")
    df.registerTempTable("dmp")
    val res = sqlContext.sql(
      """
        |select case client when 1 then "android"  when 2 then "ios" else "wp" end as client,
        |count(client),
        |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as ysrequset,
        |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) as yxrequset,
        |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) as adrequest,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) as cybid,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) as cybidsuccees,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) as shows,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) as clicks,
        |sum(case when requestmode=1 and iseffective=1 then winprice/1000 else 0 end) as dspcost,
        |sum(case when requestmode=1 and iseffective=1 then adpayment/1000 else 0 end) as dspapy
        |from dmp
        |group by client
      """.stripMargin)

    res.show()
    sc.stop()

  }
}
