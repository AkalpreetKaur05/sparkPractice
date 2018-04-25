package com.hashmap.pgms

import org.apache.spark.{SparkConf, SparkContext}

object TransformationsPractice extends App
{
  //val sparkSession=SparkSession.builder().appName("App for transformation").master("local").getOrCreate()
  val masterURL = "local[*]"
  val conf = new SparkConf()
    .setAppName("SparkMe Application")
    .setMaster(masterURL)
  val sc = new SparkContext(conf)

  // val rdd1=sparkSession.sparkContext.parallelize(Seq(10,20,30))
 // val rdd2=sparkSession.sparkContext.parallelize((Seq(80,79)))
  //val rdd3=rdd1.union(rdd2)
  //rdd3.foreach(println)
 // val person=sparkSession.read.json("C:\\jsondata")
  //person.createOrReplaceGlobalTempView("person")
 // person.show()
 // person.printSchema()
 //println( person.count())
  //val info=sparkSession.sql("select * from person where age >30").show()
  val rdd1=sc.parallelize(List(1,3,45,5,6,7,8,0),2)
  val rdd2=rdd1.map(x=>x*2)
  rdd2.foreach(println)
}
