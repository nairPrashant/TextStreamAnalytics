package com.nair

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object TextStreamAnalytics {
  def main(args: Array[String]): Unit = {
    //settigs to restrict log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //sparkConf
    val conf=new SparkConf().setAppName("TextStreamAnalytics").setMaster("local")
    //HADOOP_HOME setting for SPARK
    System.setProperty("hadoop.home.dir", "G:\\hadoop-winutils-2.6.0")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val inputFile=Resources.getResource("data.txt").toString.replace("%20"," ")
    val windowSize=60
    val sqlContext=new HiveContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "false").load(inputFile)
    val outputDF=doProcessing(sqlContext,df, windowSize)
    outputDF.write.format("com.databricks.spark.csv").option("delimiter", "\t").save("output/out")
  }

  def doProcessing(sqlContext:SQLContext,df:DataFrame,windowSize:Int)={
    import sqlContext.implicits._
    val df2=df.withColumn("Time",$"C0".cast(IntegerType))
      .withColumn("Value",$"C1".cast(DoubleType))
      .drop($"C0")
      .drop($"C1")

    val windowFunction = Window.orderBy($"Time").rangeBetween(-1*windowSize,0)
    val finalDF =df2
      .withColumn("N_O",count($"Value").over(windowFunction))
      .withColumn("Roll_sum",round(sum($"Value").over(windowFunction),5))
      .withColumn("Min_Value",min($"Value").over(windowFunction))
      .withColumn("Max_Value",max($"Value").over(windowFunction))
    finalDF.show(10)
    finalDF
  }
}
