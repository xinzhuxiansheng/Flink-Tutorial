package com.yzhou.apitest

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_2", 1547718199, 35.80018327300259),
      SensorReading("sensor_3", 1547718199, 35.80018327300259),
      SensorReading("sensor_4", 1547718199, 35.80018327300259)
    ))

    val stream4 = env.addSource(new SensorSource())

    stream4.print("stream4")

    env.execute("stream4 execute")
  }
}

class SensorSource() extends SourceFunction[SensorReading] {

  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random();

    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    while (running) {
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      val curTime = System.currentTimeMillis();
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )

      Thread.sleep(500)
    }

  }

  override def cancel(): Unit = {
    running = false;
  }
}
