package com.advance

import org.apache.spark.{SparkConf, SparkContext,_}

//取出每个分区中的最大值
object Rdd9_12
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);

    val rdd1=sc.parallelize(List( ("cat",2),("cat",5),("mouse",4),("cat,12"),("dog",12),("mouse",2)),2)
    //val rdd2= rdd1.aggregateByKey(0)( _ + _ )( _ + _ )

    println(rdd1)
    sc.stop();

  }
}
