package com.tag

import com.typesafe.config.ConfigFactory
import com.utils.{JedisConnectionPool, TagsUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 借助图计算中的连通图
  *
  */
object TagsContextV2 {
  def main(args: Array[String]): Unit = {
    //模拟企业开发模式，首先判断一下目录是否为空
    if (args.length != 5) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    //创建一个集合，储存一下输入输出目录
    val Array(inputPath, outputPath, dirPath, stopWords, day) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      //处理数据，采取scala的序列方式，性能比Java高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    //我们要采取snappy压缩方式，因为我们用的是spark1.6版本，到2.0以后就不用配置了
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    /**
      * Hbase连接
      */
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.table.name")
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.host"))
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbConn.getAdmin
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      println("HBASE Table Name Create")

      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      //将列簇加入表中
      tableDescriptor.addFamily(columnDescriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbConn.close()
    }
    //创建一个jobconf任务
    val jobConf = new JobConf(configuration)
    //指定key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出到那个表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)


    // 读取字典文件
    val dirMap = sc.textFile(dirPath).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collect.toMap
    //广播字典文件
    val broadcast = sc.broadcast(dirMap)
    //修改停用词库
    val stopwordDir = sc.textFile(stopWords).map((_, 0)).collect.toMap
    //广播停用词库
    val stopwords = sc.broadcast(stopwordDir)


    //读取文件数据
    val baseRDD: RDD[(List[String], Row)] = sqlContext.read.parquet(inputPath)
      //过滤数据ID
      .filter(TagsUtils.UserId)
      .map(row => {
        //获取当前所有的用户非空ID
        val list = TagsUtils.getAnyAllUserId(row)
        (list, row)
      })
    //构建点的集合 RDD[(List[string], Row)]
    val result: RDD[(Long, Seq[(String, Int)])] = baseRDD.flatMap(tp => {

      //获取广播的字典文件
      val dict: Map[String, String] = broadcast.value
      //获取停用词库
      val stop: Map[String, Int] = stopwords.value

      //取出row数据
      val row = tp._2
      val adTag = TagsAD.makeTags(row)
      val appTag = TagsAPP.makeTags(row, dict)
      val deviceTag = TagsDevice.makeTags(row)
      val keywordTag = TagsKeyWord.makeTags(row, stop)
      val loactionTag = TagLocation.makeTags(row)
      //商圈标签先不加
//      val tagsBusiness = TagsBusiness(row, jedis)

      //将所有的标签放到一起
      val Rowtag: Seq[(String, Int)] = adTag ++ appTag ++ deviceTag ++ keywordTag ++ loactionTag
      //List[(string,int)] = (标签：V>0, 用户ID>0)
      val VD: Seq[(String, Int)] = tp._1.map((_, 0)) ++ Rowtag
      //只有第一行的第一个ID可以携带顶点ID，其他不携带
      //如果同一行上的多个顶点就乱了，数据就会重复
      val tuples: Seq[(Long, Seq[(String, Int)])] = tp._1.map(uId => {
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
      tuples
    })
    //    result.take(50).foreach(println)
    //构建边集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode.toLong, 0))
    })
    edges.take(25).foreach(println)

    //构建图
    val graph = Graph(result, edges)
    //实现图连接
    val vertices = graph.connectedComponents().vertices
    //认祖归宗
    vertices.join(result).map {
      case (uId, (commonId, tagsAndUserId)) => (commonId, tagsAndUserId)
    }.reduceByKey {
      case (list1, list2) => (list1 ++ list2)
        .groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }
    //RDD[(VertexId, Seq[(String, Int)])]
      .map {
    case (userid, userTags) => {
      val put = new Put(Bytes.toBytes(userid))
      val tags = userTags.map(t=>t._1+","+t._2).mkString(",")
      //将所有的数据输入到Hbase
      put.addImmutable(
        Bytes.toBytes("tags"), Bytes.toBytes(s"$day"),Bytes.toBytes(tags))
      //Hbase返回所有的数据
      (new ImmutableBytesWritable(), put)
    }
  }.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
