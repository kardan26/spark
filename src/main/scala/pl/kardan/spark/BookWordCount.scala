package pl.kardan.spark

object BookWordCount extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val path = getClass.getResource("/SparkScala/book.txt").getPath
    val sc = initSpark(getClass.getName)
    val rdd = sc.textFile(path)
    val results = rdd
      .flatMap(_.split("\\W+"))
      .map(s => s.toLowerCase())
      .filter(!_.isEmpty)
      .map(x => (x,1))
      .reduceByKey((x,y) => x+y)
      .sortByKey()
        .map((x)=> s"Word: ${x._1} count: ${x._2}")

    results.foreach(println)
  }
}
