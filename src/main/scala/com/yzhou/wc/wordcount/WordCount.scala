package com.yzhou.wc.wordcount

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object WordCount {
  def main(args: Array[String]): Unit = {

    //创建一个执行环境
    var env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "E:\\java\\Flink-Tutorial\\src\\main\\resources\\wordCount.txt";
    var inputDataSet = env.readTextFile(inputPath);

    //切分数据得到word，然后再按word做分组聚合
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
