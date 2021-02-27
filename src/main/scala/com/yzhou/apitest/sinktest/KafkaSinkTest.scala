package com.yzhou.apitest.sinktest

import com.yzhou.apitest.SensorReading
import org.apache.flink.streaming.api.scala._

class KafkaSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val inputStream = env.readTextFile("D:\\code\\yzhou\\Flink-Tutorial\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream
      .map(
        data=>{
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
        }
      )
  }

}
