package com.netposa.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingByStatus {



  def main(args: Array[String]): Unit = {

    if(args.length!=3){
      println("usage: <duration> <host> <port>")
      System.exit(1)
    }

    //构造配置对象,获取系统默认的配置对象
    val conf = new SparkConf().setAppName("StreamingDemoByStatus")
    //构造spark streaming上下文对象 参数一是配置对象 参数二是时间间隔
    val streamSC = new StreamingContext(conf,Seconds(args(0).toInt))
    // 设置检查点
    streamSC.checkpoint("/sparktmp")

    //指定数据源(接收器) 会实时实时接收数据
    //参数一:发送socket消息的主机 参数二: 发送端口 参数三: 存储级别 默认为内存+硬盘
    val datas=streamSC.socketTextStream(args(1),args(2).toInt,StorageLevel.MEMORY_AND_DISK)

    //业务逻辑
    val rs=datas.map((_,1)).reduceByKey(_+_)
    println("=======================")
    println("=======================")
    rs.print()

    //状态累加统计
    val status=rs.updateStateByKey(updateFunc)

    //打印结果集

    println("XXXXXXXXXXXXXXXXXXXXXXXX")
    println("XXXXXXXXXXXXXXXXXXXXXXXX")
    status.print

    streamSC.start

    streamSC.awaitTermination
  }

  /**
    * Seq[Int]    scala的一种有序集合,可以存储重复数据,可以快速插入和删除数据
    * Option[Int] 也是一种集合,如果有值,返回Some[A];如果没值,返回NONE
    */


  def updateFunc=(value:Seq[Int],status:Option[Int])=>{
    //累加之前的状态
    val data=value.foldLeft(0)(_ + _)
    //取出过去的状态,取不到则为0
    val lastStatus=status.getOrElse(0)
    //返回
    Some(data+lastStatus)
  }

}
