package com.utils

import org.apache.spark.sql.types._

object AppSchemUtil {
  //定义日志的元数据信息
  val schema = StructType(
    Array(
      StructField("url",StringType,true),
      StructField("name",StringType,true),

      StructField("ysrequset",IntegerType,true),
      StructField("yxrequset",IntegerType,true),
      StructField("adrequest",IntegerType,true),
      StructField("cybid",IntegerType,true),
      StructField("cybidsuccees",IntegerType,true),
      StructField("shows",IntegerType,true),
      StructField("clicks",IntegerType,true),

      StructField("dspcost",DoubleType,true),
      StructField("dspapy",DoubleType,true)
    )
  )
}
