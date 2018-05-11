package pl.kardan.spark

import java.nio.charset.CodingErrorAction

import scala.io.{Codec, Source}

object PopularMovies extends BaseSpark {
  def parseLine(line: String) = {
    val fields = line.split("\t")
    val userId = fields(0).toInt
    val movieId = fields(1).toInt
    val rate = fields(2).toInt
    val timestamp = fields(3).toLong
    (userId,movieId,rate,timestamp)
  }

  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile(getClass.getResource("/ml-100k/u.item").getPath)(codec).getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if(fields.length>1) {
        val key = fields(0).toInt
        val value = fields(1)
        movieNames += (key -> value)
      }
    }
    movieNames
  }

  def main(args: Array[String]): Unit = {
    val path = getClass.getResource("/ml-100k/u.data").getPath
    val sc = initSpark(getClass.getName)
    var names = sc.broadcast(loadMovieNames)
    val rdd = sc.textFile(path).map(parseLine)
    val results = rdd
        .map(x => (x._2,1))
        .reduceByKey((x,y)=> x+y)
        .sortBy(_._2,false)
        .map(x=> (names.value(x._1), x._2))
        .collect()
    results.foreach(println)
  }
}
