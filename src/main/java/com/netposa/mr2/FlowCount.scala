package com.netposa.mr2

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object FlowCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("flowCount").setMaster("local[*]");
    //创建SparkContext，该对象是提交spark App的入口
    val sc = new SparkContext(conf);
    //使用sc创建RDD并执行相应的transformation和action
    val file = sc.textFile("src/main/resources/data_flow.dat");
    //file.collect.foreach(println(_))
    val lines = file.flatMap(_.split('\n'));
    //line.foreach(println(_))


    var lst= new ListBuffer[Flow]
    var lst2=new ListBuffer

    lines.foreach(line => {
      val words = line.split("\t")
      //words.foreach(println(_))
      val phoneNum = words(1);
      val upPackCount = words(6);
      val downPackCount = words(7);
      val upCount = words(8);
      val downCount = words(9);
      val t = (phoneNum, upPackCount, downPackCount, upCount, downCount);
      //lst.append(new Flow(t._1,t._2,t._3,t._4,t._5))
      lst+= new Flow(t._1,t._2,t._3,t._4,t._5);
    })

    println(lst.size)
    lst.foreach(println _)

    //停止sc，结束该任务
    sc.stop();

  }

}

case class Flow(phoneNum: String, upPackCount: String, downPackCount: String, upCount: String, downCount: String)
