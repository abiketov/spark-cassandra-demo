package com.cooking.recipe

import com.cooking.recipe.DataModel._
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File
import scala.util.Try


object DataLoadDataFrame {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val appConfig = ConfigFactory.load("application.conf")
    val defaultHost = appConfig.getString("cassandra.host")
    val defaultFileName = appConfig.getString("file.name")
    val defaultFilePath = appConfig.getString("file.path")

    val cassandraHost = Try(args(0)).toOption.getOrElse(defaultHost)
    val fileName = Try(args(1)).toOption.getOrElse(defaultFileName)
    val filePath = Try(args(2)).toOption.getOrElse(defaultFilePath)

    val sparkMaster = Option(System.getProperty("spark.master")).getOrElse("local")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra")
      .set("spark.master",sparkMaster)
      .setAppName("Data Loader")

    val spark: SparkSession = SparkSession.builder()
      .appName("Data Loader")
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.format("csv").option("delimiter", ",").option("inferSchema","true")
                       .option("header", "true").load(s"$filePath/$fileName").as[RecipeEvent]

    df.rdd.saveToCassandra("cooking","recipe_event",
      SomeColumns("recipe_id","ingredient","updated_date","active","description","created_date","recipe_name"))

    df.select(col("recipe_id"),col("recipe_name"),col("description"))
      .distinct()
      .rdd.saveToCassandra("cooking","recipe",
      SomeColumns("recipe_id","recipe_name","description"))

    df.select(col("ingredient"),col("recipe_id"),col("recipe_name"))
      .distinct()
      .rdd.saveToCassandra("cooking","ingredient_recipe",
      SomeColumns("ingredient","recipe_id","recipe_name"))

    df.select(col("recipe_id"),col("ingredient"),
             col("active"), col("updated_date")).distinct().orderBy(col("updated_date"))
      .rdd.saveToCassandra("cooking","recipe_ingredient",
      SomeColumns("recipe_id","ingredient","active","updated_date"))

  }

}
