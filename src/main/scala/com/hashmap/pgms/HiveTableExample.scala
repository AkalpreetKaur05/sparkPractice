package com.hashmap.pgms

//import com.oracle.jrockit.jfr.DataType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object HiveTableExample extends App {
  val spark= SparkSession
              .builder()
                .appName("App name")
                .getOrCreate()

 val schemaString="firstName lastName age"
  val fields=schemaString.split(" ")
    .map(fieldName=>StructField(fieldName, StringType, nullable = true))
  val schema=StructType(fields)


}


