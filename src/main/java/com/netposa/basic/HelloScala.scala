package com.netposa.basic

object HelloScala {
  def main(args: Array[String]): Unit = {
    print("hello scala")

    //直接调用java代码
    val hello=new HelloWorld();
    hello.say();

  }

}
