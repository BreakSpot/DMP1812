package com.utils

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val text: Seq[(List[String], List[(String, Int)])] = List((List("id1","id2"),List(("xxx",1),("yyy",1))),
                      (List("id2","id3"),List(("zzz",1),("kkk",1))),
                      (List("id1","id4"),List(("ddd",1),("fff",1))))

    val baseRDD = sc.makeRDD(text)

    val result = baseRDD.map(row=>{
      val VD: Seq[(String, Int)] = row._1.map((_, 0))++ row._2
      val tuples: Seq[(Long, Seq[(String, Int)])] = row._1.map(uId => {
        if (row._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
      tuples
    })
//
//    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp=>{
//      tp._1.map(uId=>Edge(tp._1.head.hashCode,uId.hashCode.toLong, 0))
//    })
//    edges.take(25).foreach(println)
//
//    //构建图
//    val graph = Graph(result,edges)
//    //实现图连接
//    val vertices = graph.connectedComponents().vertices
    //认祖归宗
//    vertices.join(result).map{
//      case (uId, (commonId, tagsAndUserId))=>(commonId, tagsAndUserId)
//    }.reduceByKey{
//      case (list1, list2)=>(list1++list2)
//        .groupBy(_._1).mapValues(_.map(_._2).sum).toList
//    }.take(20).foreach(println)


    sc.stop()
  }
}
