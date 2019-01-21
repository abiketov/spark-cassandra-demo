package com.cooking.recipe

import com.cooking.recipe.DataModel.{IngredientRecipe, Pair, RecipeIngredient}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

import scala.util.Try


object DataLoadStreaming {

  val schema = StructType(
    Array(StructField("recipe_id", StringType),
      StructField("recipe_name", StringType),
      StructField("description", StringType),
      StructField("ingredient", StringType),
      StructField("active", BooleanType),
      StructField("updated_date", DateType),
      StructField("created_date", DateType)
  ))

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val appConfig = ConfigFactory.load("application.conf")
    val defaultHost = appConfig.getString("cassandra.host")
    val defaultFilePath = appConfig.getString("file.path")

    val cassandraHost = Try(args(0)).toOption.getOrElse(defaultHost)
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
      .appName("Stream data Loader")
      .config(sparkConf)
      .getOrCreate()

    //create stream from folder
    val fileStreamDf = spark.readStream
      .option("header", "true")
      .option("inferSchema","true")
      .schema(schema)
      .csv(filePath)
      .toDF()

    val stream = fileStreamDf.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF
          .write
          .cassandraFormat("recipe_event", "cooking")
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    fileStreamDf.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
           batchDF.select(col("recipe_id"),col("recipe_name"),col("description")).distinct()
          .write
          .cassandraFormat("recipe", "cooking")
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    fileStreamDf.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.select(col("ingredient"),col("recipe_id"),col("recipe_name")).distinct()
          .write
          .cassandraFormat("ingredient_recipe", "cooking")
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    fileStreamDf.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.select(col("recipe_id"),col("ingredient"),
                       col("active"), col("updated_date")).distinct()
          .write
          .cassandraFormat("recipe_ingredient", "cooking")
          .mode("append")
          .save()
      }
      .outputMode("update")
      .start()

    stream.awaitTermination()

  }

}
