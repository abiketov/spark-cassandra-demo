package com.cooking.recipe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.util.Try

object DataAnalysis {

  def main(args: Array[String]): Unit = {

    val appConfig = ConfigFactory.load("application.conf")
    val defautHost = appConfig.getString("cassandra.host")
    val defaultYear = appConfig.getString("year.default")

    val cassandraHost = Try(args(0)).toOption.getOrElse(defautHost)
    val yearParam = Try(args(1)).toOption.getOrElse(defaultYear)

    val sparkMaster = Option(System.getProperty("spark.master")).getOrElse("local")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.master",sparkMaster)

    val spark: SparkSession = SparkSession.builder()
      .appName("Data Loader")
      .config(conf)
      .getOrCreate()


    val readBooksDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "recipe_event", "keyspace" -> "cooking"))
      .load

    val withHours = readBooksDF.withColumn(
      "updated_year",
      year(col("updated_date"))
    ).withColumn(
      "updated_month",
      month(col("updated_date"))
    ).withColumn(
      "updated_day",
      dayofmonth(col("updated_date"))
    ).withColumn(
      "updated_hour",
      hour(col("updated_date"))
    )

    val res = withHours.groupBy(col("updated_year"),
      col("updated_month"),
      col("updated_day"),
      col("updated_hour")).count().alias("count")
            .agg(avg(col("count")).alias("Average_updates_per_hour"))

    res.show()

    val res2 = withHours.groupBy(col("updated_year"),
      col("updated_month"),
      col("updated_day"),
      col("updated_hour")).count()
           .where(s"updated_hour=10 and updated_year=$yearParam")
           .agg(sum(col("count")).alias("Total updated at 10:00"))

    res2.show()
  }
}
