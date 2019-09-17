package com.advance

import org.apache.spark.{SparkConf, SparkContext}

//取出每个分区中的最大值
object Rdd8 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1 = sc.parallelize(List("12","23","345","4567"),2);

    val rdd2 = rdd1.aggregate("")((x,y)=>math.max(x.length,y.length).toString,(x,y)=>x+y)

    val rdd3 = rdd1.aggregate("")((x,y)=>math.min(x.length,y.length).toString,(x,y)=>x+y)


    
    println(rdd2) // 24  42
    println(rdd3) // 11

    sc.stop();

  }
}

