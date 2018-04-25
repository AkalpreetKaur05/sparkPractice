package com.hashmap
import org.apache.spark.{SparkConf, SparkContext}
class readFile
{
  def main(args: Array[String]) {
    val masterURL = "local[*]"
    val conf = new SparkConf()
      .setAppName("SparkMe Application")
      .setMaster(masterURL)
    val sc = new SparkContext(conf)
    val fileName = util.Try(args(0)).getOrElse("C:\\Users\\hashmap\\Downloads\\HOL - code for practice-20180306T065940Z-001 (1)\\HOL - code for practice\\Lab 1.txt")
    val lines = sc.textFile(fileName).cache()
    val c = lines.count()
    println(s"There are $c lines in $fileName")
  }

}
