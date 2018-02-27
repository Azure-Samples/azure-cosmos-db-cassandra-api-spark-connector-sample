/**
  * <copyright file="CosmosDbConnectionFactory.scala" company="Microsoft">
  * Copyright (c) Microsoft. All rights reserved.
  * </copyright>
 */
package com.microsoft.azure.cosmosdb.cassandra

import java.nio.file.{Files, Path, Paths}
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.spark.connector.cql.CassandraConnectorConf.CassandraSSLConf
import com.datastax.spark.connector.cql._
import org.apache.commons.io.IOUtils

object CosmosDbConnectionFactory extends CassandraConnectionFactory {

  /** Returns the Cluster.Builder object used to setup Cluster instance. */
  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    val builder = Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      /**
        * Make use of the custom RetryPolicy for Cosmos DB. This Is needed for retrying scenarios specific to Cosmos DB.
        * Please refer to the "Retry Policy" section of the README.md for more information regarding this.
        */
      .withRetryPolicy(
        new CosmosDbMultipleRetryPolicy(conf.queryRetryCount))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
      .withLoadBalancingPolicy(
        new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
      .withCompression(conf.compression)
      .withQueryOptions(
        new QueryOptions()
          .setRefreshNodeIntervalMillis(0)
          .setRefreshNodeListIntervalMillis(0)
          .setRefreshSchemaIntervalMillis(0))

    if (conf.cassandraSSLConf.enabled) {
      maybeCreateSSLOptions(conf.cassandraSSLConf) match {
        case Some(sslOptions) ⇒ builder.withSSL(sslOptions)
        case None ⇒ builder.withSSL()
      }
    } else {
      builder
    }
  }

  private def getKeyStore(
                           ksType: String,
                           ksPassword: Option[String],
                           ksPath: Option[Path]): Option[KeyStore] = {

    ksPath match {
      case Some(path) =>
        val ksIn = Files.newInputStream(path)
        try {
          val keyStore = KeyStore.getInstance(ksType)
          keyStore.load(ksIn, ksPassword.map(_.toCharArray).orNull)
          Some(keyStore)
        } finally {
          IOUtils.closeQuietly(ksIn)
        }
      case None => None
    }
  }

  private def maybeCreateSSLOptions(conf: CassandraSSLConf): Option[SSLOptions] = {
    lazy val trustStore =
      getKeyStore(conf.trustStoreType, conf.trustStorePassword, conf.trustStorePath.map(Paths.get(_)))
    lazy val keyStore =
      getKeyStore(conf.keyStoreType, conf.keyStorePassword, conf.keyStorePath.map(Paths.get(_)))

    if (conf.enabled) {
      val trustManagerFactory = for (ts <- trustStore) yield {
        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        tmf.init(ts)
        tmf
      }

      val keyManagerFactory = if (conf.clientAuthEnabled) {
        for (ks <- keyStore) yield {
          val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
          kmf.init(ks, conf.keyStorePassword.map(_.toCharArray).orNull)
          kmf
        }
      } else {
        None
      }

      val context = SSLContext.getInstance(conf.protocol)
      context.init(
        keyManagerFactory.map(_.getKeyManagers).orNull,
        trustManagerFactory.map(_.getTrustManagers).orNull,
        new SecureRandom)

      Some(
        JdkSSLOptions.builder()
          .withSSLContext(context)
          .withCipherSuites(conf.enabledAlgorithms.toArray)
          .build())
    } else {
      None
    }
  }

  /** Creates and configures the Cassandra connection */
  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    clusterBuilder(conf).build()
  }

}
