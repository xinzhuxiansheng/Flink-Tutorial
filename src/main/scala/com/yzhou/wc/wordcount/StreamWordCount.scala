package com.yzhou.wc.wordcount

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接受一个socket流文本
    val dataStream = env.socketTextStream("localhost", 7777)

    //对每条数据进行处理
    val wordCountDataStream = dataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    //启动executor
    env.execute("steam word count job")

  }
}
