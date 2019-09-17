package com.netposa.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingByWindowNew {
  def main(args: Array[String]): Unit = {

    if(args.length!=5){
      println("usage: <duration> <host> <port> <dataDuration> <dataHandlerDuration>")
      System.exit(1)
    }

    //构造配置对象,获取系统默认的配置对象
    val conf = new SparkConf().setAppName("StreamingDemoByWindows")
    //构造spark streaming上下文对象 参数一是配置对象 参数二是时间间隔
    val streamSC = new StreamingContext(conf,Seconds(args(0).toInt))
    // 设置检查点 带状态的操作 必须要做checkpoint 特别是 window操作
    streamSC.checkpoint("/sparktmp")

    //指定数据源(接收器) 会实时实时接收数据
    //参数一:发送socket消息的主机 参数二: 发送端口 参数三: 存储级别 默认为内存+硬盘
    val datas=streamSC.socketTextStream(args(1),args(2).toInt,StorageLevel.MEMORY_ONLY)

    //业务逻辑
    /**
      * 参数一:累加函数
      * 参数二:减去t-1状态的函数
      * 参数三:数据时间间隔
      * 参数四:处理时间间隔
      */
    val rs=datas.map((_,1)).reduceByKeyAndWindow(_ + _  , _ - _,Seconds(args(3).toInt),Seconds(args(4).toInt));

    rs.print

    streamSC.start

    streamSC.awaitTermination
  }
}
