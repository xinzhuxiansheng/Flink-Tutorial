package com.yzhou.wc.wordcount

import org.apache.flink.streaming.api.scala._

/**
 * 脚本启动流程
 * 1. WSL中启动 7777端口 (nc -l 7777)
 * 2. 执行main方法，监听 WSL linux虚机 IP + Port
 */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //利用nc模拟socket通信 nc -l 7777
    //接受一个socket流文本
    val dataStream = env.socketTextStream("172.19.187.24", 7777)

    //对每条数据进行处理
    val wordCountDataStream = dataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //setParallelism 设置并行度， 会根据本地CPU核数决定
    wordCountDataStream.print().setParallelism(1)

    //启动executor
    env.execute("steam word count job")

  }
}
