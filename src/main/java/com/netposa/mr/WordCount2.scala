package com.netposa.mr

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object WordCount2 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("WC2").setMaster("local[*]");
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf);
    //使用sc创建RDD并执行相应的transformation和action
    val file=sc.textFile("src/main/resources/hello.txt");

    file.collect.foreach(println(_))

    val seed=new Random().nextInt(10000);

    file.flatMap(_.split(" ")).filter(_.size>4).map((_, 2)).reduceByKey(_+_, 1).sortBy(_._2, false).repartition(1).saveAsTextFile("imyspark"+seed);
    //停止sc，结束该任务
    sc.stop();

  }
}
