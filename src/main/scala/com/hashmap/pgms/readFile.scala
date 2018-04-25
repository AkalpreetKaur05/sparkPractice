package com.hashmap.pgms
import org.apache.spark.{SparkConf, SparkContext}
object readFile extends App
{
   val masterURL = "local[*]"
    val conf = new SparkConf()
      .setAppName("SparkMe Application")
      .setMaster(masterURL)
    val sc = new SparkContext(conf)
    val fileName =("C:\\Users\\hashmap\\Downloads\\HOL - code for practice-20180306T065940Z-001 (1)\\HOL - code for practice\\person.txt")
    val lines = sc.textFile(fileName).cache()
    val c = lines.count()
    println(s"There are $c lines in $fileName")


}

