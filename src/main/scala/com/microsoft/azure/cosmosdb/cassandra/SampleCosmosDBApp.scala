/**
  * <copyright file="SampleCosmosDBApp.scala" company="Microsoft">
  * Copyright (c) Microsoft. All rights reserved.
  * </copyright>
  */

package com.microsoft.azure.cosmosdb.cassandra

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SampleCosmosDBApp extends Serializable {

  // GENERATE RANDOM DATA
  def randomDataPerPartitionId(sc: SparkContext, DoP: Int, start: Int, end: Int, col1: Int, col2: Int): RDD[(Int, Int, Int, String)] = {
    sc.parallelize(start until end, DoP).map { id2 =>
      val id1Val: Int = Random.nextInt(col1)
      val id2Val: Int = Random.nextInt(col2)
      val col1Val: Int = Random.nextInt(col1)
      val col2Val: String = s"text_${Random.nextInt(col2)}"
      (id1Val, id2Val, col1Val, col2Val)
    }
  }

  // MAIN
  def main (arg: Array[String]): Unit = {

    // CONFIG. *NOTE*: Please read the README.md for more details regarding each conf value.
    val conf = new SparkConf(true)
      .setAppName("SampleCosmosDBCassandraApp")
      // Cosmos DB Cassandra API Connection configs
      .set("spark.cassandra.connection.host", "<COSMOSDB_CASSANDRA_ENDPOINT>")
      .set("spark.cassandra.connection.port", "10350")
      .set("spark.cassandra.connection.ssl.enabled", "true")
      .set("spark.cassandra.auth.username", "COSMOSDB_ACCOUNTNAME")
      .set("spark.cassandra.auth.password", "COSMODB_KEY")
      // Parallelism and throughput configs.
      .set("spark.cassandra.output.batch.size.rows", "1")
      // *NOTE*: The values below are meant as defaults for a sample workload. Please read the README.md for more information on fine tuning these conf value.
      .set("spark.cassandra.connection.connections_per_executor_max", "10")
      .set("spark.cassandra.output.concurrent.writes", "100")
      .set("spark.cassandra.concurrent.reads", "512")
      .set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
      .set("spark.cassandra.connection.keep_alive_ms", "60000")
      // Cosmos DB Connection Factory, configured with retry policy for rate limiting.
      .set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.CosmosDbConnectionFactory")


    // SPARK CONTEXT
    val sc = new SparkContext(conf)

    // CREATE KEYSPACE/TABLE, AND ANY ARBITRARY QUERY STRING.
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE kspc WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE kspc.tble (id1 int, id2 int, col1 int, col2 text, PRIMARY KEY(id1, id2))")
    }

    // INSERT DATA
    val collection = sc.parallelize(Seq((1, 1, 1, "text_1"), (2, 2, 2, "text_2")))
    collection.saveToCassandra("kspc", "tble", SomeColumns("id1", "id2", "col1", "col2"))

    // INSERT GENERATED DATA
    randomDataPerPartitionId(sc, DoP= 10, start = 0, end = 10000, col1 = 100000, col2 = 100000).
      saveToCassandra("large", "large", SomeColumns("id1", "id2", "col1", "col2"))

    // SELECT
     val rdd1 = sc.cassandraTable("kspc", "tble").select("id1", "id2", "col1", "col2").where("id1 = ?", 1)

    // SELECT and print lines
    val rdd2 = sc.cassandraTable("kspc", "tble").select("id1", "id2", "col1", "col2").where("id1 = ?", 2).collect().foreach(println)

    sc.stop
  }
}