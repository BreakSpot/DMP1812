package com.tag

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsBusiness extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(arg: Any*): List[(String, Int)] = {
    val row = arg(0).asInstanceOf[Row]
    val jedis = arg(1).asInstanceOf[Jedis]
    var list = List[(String, Int)]()

    if(row.getAs[String]("long").toDouble>=73
    && row.getAs[String]("long").toDouble<=136
    && row.getAs[String]("lat").toDouble>=3
    && row.getAs[String]("lat").toDouble<=54){
      //取出经纬度
      val long = row.getAs[String]("long")
      val lat = row.getAs[String]("lat")
      //将经纬度转换成geoHash
      val geoHash = GeoHash.
        geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble,8)
      //进行取值
      val geo = jedis.get(geoHash)
      if(StringUtils.isNotBlank(geo)){
        //获取对应的商圈
        geo.split(";").foreach(t=>list:+=(t,1))
      }
    }

    list
  }
}
