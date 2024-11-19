// Databricks notebook source


// COMMAND ----------

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.neo4j.spark.DataSource

object MongoToNeo4jWithSpark extends App {

  val spark = SparkSession.builder()
    .appName("MongoToNeo4j")
    .config("spark.mongodb.input.uri", "mongodb+srv://<user>:<password>@cluster.mongodb.net/<database>")
    .config("spark.mongodb.output.uri", "mongodb+srv://<user>:<password>@cluster.mongodb.net/<database>")
    .config("neo4j.url", "neo4j+s://73c212f9.databases.neo4j.io")
    .config("neo4j.authentication.basic.username", "neo4j")
    .config("neo4j.authentication.basic.password", "JmgiKGIcJ7_A0kmuS6nhe9jqKj59b_mDGX4Tbv3roSU")
    .config("neo4j.database", "neo4j")
    .getOrCreate()

  def processCollection(collectionName: String): Unit = {
    val mongoDf = spark.read
      .format("mongo")
      .option("uri", "mongodb+srv://<user>:<password>@cluster.mongodb.net/<database>")
      .option("database", "CVE")
      .option("collection", collectionName)
      .load()

    val transformedDf = mongoDf.select("id", "description", "impactScore")

    transformedDf.write
      .format("neo4j")
      .mode(SaveMode.Overwrite)
      .option("labels", "CVE")
      .option("node.keys", "id")
      .option("relationship", "HAS_DESCRIPTION")
      .save()

    transformedDf.write
      .format("neo4j")
      .mode(SaveMode.Append)
      .option("relationship", "HAS_IMPACT_SCORE")
      .save()
  }

  processCollection("cve_2024")
  processCollection("cve_2023")

  spark.stop()
}

