package com.advance

import org.apache.spark.{SparkConf, SparkContext}

//取出每个分区中的最大值
object Rdd9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1 = sc.parallelize(List("12","23","345",""),2);

    val rdd3 = rdd1.aggregate("")((x,y)=>math.min(x.length,y.length).toString,(x,y)=>x+y)


    
    println(rdd3) // 10  01

    sc.stop();

  }
}