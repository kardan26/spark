package pl.kardan.spark

object AverageFriendsByFirstName extends BaseSpark {
  def parseLine(line: String) = {
    val items = line.split(",")
    val name = items(1)
    val friends = items(3).toInt
    (name,friends)
  }

  def main(args: Array[String]): Unit = {
    val path = getClass.getResource("/SparkScala/fakefriends.csv").getPath
    val sc = initSpark(getClass.getName)
    val rdd = sc.textFile(path)
      .map(parseLine)
      .mapValues((_,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .mapValues((x) => x._1 / x._2)
      .collect()
      .sorted
      .foreach(println)
  }
}
