package com.advance

import org.apache.spark.{SparkConf, SparkContext}

//取出每个分区中的最大值
object Rdd4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1=sc.parallelize(Array[Int](1,2,3,4,5,6,7,8),2);
    val rdd2 = rdd1.reduce(math.max(_,_))
    val rdd3 = rdd1.aggregate(0)(math.max(_,_),_+_)
    val rdd4 = rdd1.aggregate(10)(math.max(_,_),_+_)

    val rdd5=rdd1.aggregate(9)(math.max(_,_),_+_)
    val rdd6=rdd1.aggregate(9)(_ + _ , _ + _ )

    println(rdd2)   //8
    println(rdd3)   //12
    println(rdd4)   //30
    println(rdd5)   //27
    println(rdd6)   //63

    sc.stop();

  }
}

