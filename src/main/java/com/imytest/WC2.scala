package com.imytest

import org.apache.spark.{SparkConf, SparkContext}

object WC2 {
  def main(args: Array[String]): Unit = {
    println("aaaa")

    val conf = new SparkConf().setAppName("WC2").setMaster("local[*]");
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf);
    //使用sc创建RDD并执行相应的transformation和action
    val inputFile=sc.textFile("src/main/resources/hello.txt");
    val outputFile=sc.textFile("src/main/resources/hello.txt");

    println(inputFile.collect.toBuffer+"_______________-")
    inputFile.collect.foreach(println(_))

    inputFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_, 1).sortBy(_._2, false).saveAsTextFile("imyspark");
    //停止sc，结束该任务
    sc.stop();

  }
}
