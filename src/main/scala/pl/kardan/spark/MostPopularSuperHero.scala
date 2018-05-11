package pl.kardan

import pl.kardan.spark.BaseSpark

object MostPopularSuperHero extends BaseSpark {
  def parseNames(line: String): Option[(Int,String)] = {
    val fields = line.split('\"')
    if(fields.length > 1) {
      return Some(fields(0).trim.toInt, fields(1))
    }
    None
  }
  def parseGraph(line: String): Option[(Int, Int)] = {
    val splited = line.split(" ")
    if(splited.length>0) {
      return Some((splited(0).trim.toInt, splited.length-1))
    }
    None
  }
  def main(args: Array[String]): Unit = {
    val namesPath = getClass.getResource("/SparkScala/Marvel-names.txt").getPath
    val graphPath = getClass.getResource("/SparkScala/Marvel-graph.txt").getPath
    val sc = initSpark(getClass.getName)
    val namesRdd = sc.textFile(namesPath)
      .flatMap(parseNames)

    val graphRdd = sc.textFile(graphPath)
      .flatMap(parseGraph)
      .reduceByKey((x,y)=> x+y)
    val result = graphRdd
      .sortBy(_._2, false)
      .first()
    val popularName = namesRdd.lookup(result._1)(0)
    println(popularName)
  }
}

