package com.netposa.stream

import java.io.PrintWriter
import java.net.ServerSocket

import com.sun.javafx.binding.StringFormatter

import scala.collection.mutable.ListBuffer

object Sender {
    def generateContent(index:Int):String ={
      val charList=ListBuffer[Char]()
      for( i<- 65 to 90){
        charList+=i.toChar
      }
      val charArray=charList.toArray
      charArray(index).toString
    }

  def index={
    import java.util.Random
    val random=new Random();
    random.nextInt(7)
  }

  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("usage: <port> <millisecond>")
      System.exit(1);
    }

    val listener = new ServerSocket(args(0).toInt)
    while(true){
      val socket=listener.accept();
      new Thread(){
        override def run(): Unit = {
          println("Got client connect from: "+socket.getInetAddress)
          val printWriter = new PrintWriter(socket.getOutputStream,true)
          while(true){
            Thread.sleep(args(1).toLong)
            val content = generateContent(index)
            print(content)
            printWriter.write(content+'\n')
            printWriter.flush()
          }
          socket.close()
        }
      }.start()
    }
  }

}
