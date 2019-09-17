package com.advance

import org.apache.spark.{SparkConf, SparkContext}

//取出每个分区中的最大值
object Rdd9_11
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1 =Array("","12","23")

    val rdd2 = rdd1.reduce((x,y)=>math.min(x.length,y.length).toString)

    println(rdd2) //1
    sc.stop();

  }
}