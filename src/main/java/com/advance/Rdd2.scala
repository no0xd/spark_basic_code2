package com.advance

import org.apache.spark.{SparkConf, SparkContext}

object Rdd2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1=sc.parallelize(Array[Int](1,2,3,4,5,6,7),2);
    val rdd2=rdd1.aggregate(0)(_+_,_+_)

    println(rdd2)

    sc.stop();

  }

}
