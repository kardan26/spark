package pl.kardan.spark

import scala.math.max

object MaximalTemperatureByLocation extends BaseSpark {
  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temp = fields(3).toFloat * 0.1f
    (stationId,entryType,temp)
  }

  def main(args: Array[String]): Unit = {
    val path = getClass.getResource("/SparkScala/1800.csv").getPath
    val sc = initSpark(getClass.getName)
    val rdd = sc.textFile(path).map(parseLine)
    val results = rdd
      .filter(_._2 == "TMAX")
      .map(x=> (x._1, x._3))
      .reduceByKey((x,y) =>max(x,y))
      .collect()
    results.foreach(println)
  }
}
