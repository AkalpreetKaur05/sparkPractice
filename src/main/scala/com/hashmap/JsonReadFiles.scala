package com.hashmap

import org.apache.spark.sql.SparkSession

object JsonReadFiles extends App
{
  val sparkSession=SparkSession.builder().appName("App for transformation").master("local").getOrCreate()
  val person=sparkSession.read.json("C:\\jsondata\\person.json")
  person.show()
  person.createOrReplaceTempView("person1")
  val agegroup=sparkSession.sql("select * from person1 where age>25 ")
  agegroup.show()
}
