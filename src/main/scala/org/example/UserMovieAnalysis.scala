package org.example

import org.apache.spark.{SparkConf, SparkContext}

object UserMovieAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("UserMovieAnalysis").setMaster("local[4]")
    val sc=new SparkContext(sparkConf)
    val  movies =sc.textFile("C:/Users/zheng/Desktop/ml-latest-small/movies.csv").map(_.split(",")).map(movie=>(movie(0),(movie(1),movie(2))))
    movies.collect().take(10).foreach(println)
    val ratings =sc.textFile("C:/Users/zheng/Desktop/ml-latest-small/ratings.csv").map(_.split(",")).map(rating=>(rating(1),(rating(0),rating(2))))
    ratings.collect().take(10).foreach(println)
    val moviesUserRatings =  movies.join(ratings).map(x=>(x._2._2._1,x._1,x._2._1._1,x._2._2._2))
    moviesUserRatings.collect().take(100).foreach(println)
    //movies.join(ratings).collect().take(100).foreach(println)
    moviesUserRatings.reduce()

  }

}
