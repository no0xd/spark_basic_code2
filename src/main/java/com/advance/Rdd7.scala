package com.advance

import org.apache.spark.{SparkConf, SparkContext}

//取出每个分区中的最大值
object Rdd7 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1 = sc.parallelize(List("a","b","c","d","e","f"),2);
    val rdd2 = rdd1.aggregate("|")(_ + _ , _ + _);
    
    println(rdd2) //  ||def|abc  //  ||abc|def

    sc.stop();

  }
}

