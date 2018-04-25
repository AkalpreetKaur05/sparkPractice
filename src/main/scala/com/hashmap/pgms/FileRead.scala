package com.hashmap.pgms

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
//import spark.implicits._

object FileRead extends App
{
  val sparkSession=SparkSession.builder().appName("App for transformation").master("local").getOrCreate()
  val file2=sparkSession.read.format("csv")
               .load("C:\\Users\\hashmap\\Downloads\\airline_data (1)\\airline_data")

  val schema = StructType(Array(StructField("YEAR",IntegerType,true),
    StructField("MONTH",IntegerType,true),
    StructField("DAY_OF_MONTH",IntegerType,true),
    StructField("DAY_OF_WEEK",IntegerType,true),
    StructField("CARRIER",StringType,true),
    StructField("FL_NUM",IntegerType,true),
    StructField("ORIGIN",StringType,true),
    StructField("DES",StringType,true),
    StructField("DEP_TIME",IntegerType,true),
    StructField("DEP_DELAY",IntegerType,true),
    StructField("ARR_TIME",IntegerType,true),
    StructField("ARR_DELAY",IntegerType,true),
    StructField("CANCELLED",IntegerType,true),
    StructField("CANCELLATION_CODE",IntegerType,true),
    StructField("AIR_TIME",IntegerType,true),
    StructField("DISTANCE",IntegerType,true)))

  val personDS = sparkSession.read.schema(schema).csv("C:\\Users\\hashmap\\Downloads\\airline_data (1)\\airline_data")
  personDS.show()
  personDS.createOrReplaceTempView("trainData")
  val traindata=sparkSession.sql("select * from trainData")
  traindata.show()
  val countLateTrains: DataFrame =sparkSession.sql( "select DEP_DELAY from trainData where (DEP_DELAY >0) ")
  countLateTrains.show()
  val countTrainsLate=countLateTrains.count()
  //val count=sparkSession.sql("select count(*) from trainData where(DEP_DELAY>0) or (DEP_DELAY<0)")
  println("count is "+countTrainsLate)
  val trainsOnTime=sparkSession.sql( "select DEP_DELAY from trainData where (DEP_DELAY <=0) ")
  val onTimeTrainsCount=trainsOnTime.count()
  println("trains on time "+onTimeTrainsCount)
 // val trainsDelayedWeekly=sparkSession.sql(" from trainData select count(*) where(DEP_DELAY>0) group by DAY_OF_WEEK  ").show()
  val abcd =sparkSession.sql(" select DAY_OF_WEEK, DEP_DELAY from trainData where (DEP_DELAY>0) ")
  val abc=abcd.groupBy("DAY_OF_WEEK").count()
  abc.show()
  //val trainsOnTimeWeekly=sparkSession.sql(" from trainData select count(*) where(DEP_DELAY<=0) group by DAY_OF_WEEK  ").show()
  val abcd1 =sparkSession.sql(" select DAY_OF_WEEK, DEP_DELAY from trainData where (DEP_DELAY<=0) ")
  val abc1=abcd1.groupBy("DAY_OF_WEEK").count()
  abc1.show()
  val myexpression=("(count/1048560)*100")
  val ontimeTrainsData: DataFrame =abc1.withColumn("ontime(percentage)",expr(myexpression))
  ontimeTrainsData.show()
  val DelayedTrainsData:DataFrame =abc.withColumn("delayed(percentage)",expr(myexpression))
  DelayedTrainsData.show()
  abc1.withColumnRenamed("count","countOntime")
  val myexpress="count/countOntime"
  //val abnew=sparkSession.sql("select * from ontimeTrains where ")
  val fullData: DataFrame =ontimeTrainsData.join(DelayedTrainsData,"DAY_OF_WEEK")
  fullData.withColumn("ratio",expr(myexpress)).show()
  //abc1.withColumn("ratio",expr(myexpress)).show()
  val expre="(countTrainsLate/1048576)*100"
  val expre1="(onTimeTrainsCount/1048576)*100"
  println("overall trains late"+expre)
  println("overall trains on time "+expre1)
}

