package pl.kardan.spark
object AverageFriendsByAge extends BaseSpark {
  def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val friends = fields(3).toInt
    (age,friends)
  }

  def main(args: Array[String]): Unit = {
    val path = getClass.getResource("/SparkScala/fakefriends.csv").getPath
    val sc = initSpark(getClass.getName)
    val rdd = sc.textFile(path).map(parseLine)
    val results = rdd
      .mapValues((_,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(x => x._1 / x._2)
      .collect()
    results.sorted.foreach(println)
  }
}
