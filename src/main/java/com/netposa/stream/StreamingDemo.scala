package com.netposa.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingDemo {
  def main(args: Array[String]): Unit = {
    //构造配置对象,获取系统默认的配置对象
    val conf = new SparkConf().setAppName("StreamingDemo")
    //构造spark streaming上下文对象 参数一是配置对象 参数二是时间间隔
    val streamSC = new StreamingContext(conf,Seconds(1))
    //val streamSC = new StreamingContext(conf,Seconds(args(0).toInt))

    //指定数据源(接收器),参数为hdfs的目录
    //val datas=streamSC.textFileStream("src/main/resources/hello.txt")
    val datas=streamSC.textFileStream(args(1))

    //业务逻辑
    val rs=datas.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //打印结果集
    rs.print

    streamSC.start

    streamSC.awaitTermination

  }

}
