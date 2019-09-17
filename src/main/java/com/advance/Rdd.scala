package com.advance

import org.apache.spark.{SparkConf, SparkContext}

object Rdd {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Advance RDD").setMaster("local[*]");
    val sc=new SparkContext(conf);


    val rdd1=sc.parallelize(Array[Int](1,2,3,4,5,6,7),2);

    //incex 从0开始
    def func(index:Int,iter:Iterator[(Int)]):Iterator[String]={
      iter.toList.map(x=>"[partID: "+index+", val: "+x+"]").iterator
    }

    rdd1.mapPartitionsWithIndex(func).toArray.map(print _)

    println(rdd1)


    sc.stop();




  }

}
