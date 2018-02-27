# Azure Cosmos DB Cassandra API - Datastax Spark Connector Sample
This maven project provides samples and best practices for using the [DataStax Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector) against [Azure Cosmos DB's Cassandra API](https://docs.microsoft.com/azure/cosmos-db/cassandra-introduction).
For the purposes of providing an end-to-end sample, we've made use of an [Azure HDI Spark Cluster](https://docs.microsoft.com/azure/hdinsight/spark/apache-spark-jupyter-spark-sql) to run the spark jobs provided in the example.
All samples provided are in scala, built with maven. 

*Note - this sample is configured against the 2.0.6 version of the spark connector.*

## Running this Sample

### Prerequisites
- Cosmos DB Account configured with Cassandra API
- Spark Cluster

# Quick Start
Information regarding submitting spark jobs is not covered as part of this sample, please refer to Apache Spark's [documentation](https://spark.apache.org/docs/latest/submitting-applications.html).
In order run this sample, correctly configure the sample to your cluster(as discussed below), build the project, generate the required jar(s), and then submit the job to your spark cluster.

## Cassandra API Connection Parameters
In order for your spark jobs to connect with Cosmos DB's Cassandra API, you must set the following configurations: 

*Note - all these values can be found on the ["Connection String" blade](https://docs.microsoft.com/azure/cosmos-db/manage-account#keys) of your CosmosDB Account*

<table class="table">
<tr><th>Property Name</th><th>Value</th></tr>
<tr>
  <td><code>spark.cassandra.connection.host</code></td>
  <td>Your Cassandra Endpoint: <code>ACOUNT_NAME.cassandra.cosmosdb.azure.com</code></td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.port</code></td>
  <td><code>10350</code></td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.ssl.enabled</code></td>
  <td><code>true</code></td>
</tr>
<tr>
  <td><code>spark.cassandra.auth.username</code></td>
  <td><code>COSMOSDB_ACCOUNTNAME</code></td>
</tr>
<tr>
  <td><code>spark.cassandra.auth.password</code></td>
  <td><code>COSMOSDB_KEY</code></td>
</tr>
</table>

## Configurations for Throughput optimization
Because Cosmos DB follows a provisioned throughput model, it is important to tune the relevant configurations of the connector to optimize for this model.
General information regarding these configurations can be found on the [Configuration Reference](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md) page of the DataStax Spark Cassandra Connector github repository.
<table class="table">
<tr><th>Property Name</th><th>Description</th></tr>
<tr>
  <td><code>spark.cassandra.output.batch.size.rows</code></td>
  <td>Leave this to <code>1</code>. This is prefered for Cosmos DB's provisioning model in order to achieve higher throughput for heavy workloads.</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.connections_per_executor_max</code></td>
  <td><code>10*n</code><br/><br/>Which would be equivalent to 10 connections per node in an n-node Cassandra cluster. Hence if you require 5 connections per node per executor for a 5 node Cassandra cluster, then you would need to set this configuration to 25.<br/>(Modify based on the degree of parallelism/number of executors that your spark job are configured for)</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.concurrent.writes</code></td>
  <td><code>100</code><br/><br/>Defines the number of parallel writes that can occur per executor. As batch.size.rows is <code>1</code>, make sure to scale up this value accordingly. (Modify this based on the degree of parallelism/throughput that you want to achieve for your workload)</td>
</tr>
<tr>
  <td><code>spark.cassandra.concurrent.reads</code></td>
  <td><code>512</code><br /><br />Defines the number of parallel reads that can occur per executor. (Modify this based on the degree of parallelism/throughput that you want to achieve for your workload)</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.throughput_mb_per_sec</code></td>
  <td>Defines the total write throughput per executor. This can be used as an upper cap for your spark job throughput, and base it on the provisioned throughput of your Cosmos DB Collection.</td>
</tr>
<tr>
  <td><code>spark.cassandra.input.reads_per_sec</code></td>
  <td>Defines the total read throughput per executor. This can be used as an upper cap for your spark job throughput, and base it on the provisioned throughput of your Cosmos DB Collection.</td>
</tr>
<tr>
  <td><code>spark.cassandra.output.batch.grouping.buffer.size</code></td>
  <td>1000</td>
</tr>
<tr>
  <td><code>spark.cassandra.connection.keep_alive_ms</code></td>
  <td>60000</td>
</tr>
</table>

Regarding throughput and degree of parallelism, it is important to tune the relevant parameters based on the amount of load you expect your upstream/downstream flows to be, the executors provisioned for your spark jobs, and the throughput you have provisioned for your Cosmos DB account.

## Connection Factory Configuration and Retry Policy
As part of this sample, we have provided a connection factory and custom retry policy for Cosmos DB. We need a custom connection factory as that is the only way to configure a retry policy on the connector - [SPARKC-437](https://datastax-oss.atlassian.net/browse/SPARKC-437).
* <code>CosmosDbConnectionFactory.scala</code>
* <code>CosmosDbMultipleRetryPolicy.scala</code>

### Retry Policy
The retry policy for Cosmos DB is configured to handle http status code 429 - Request Rate Large exceptions. The Cosmos Db Cassandra API, translates these exceptions to overloaded errors on the Cassandra native protocol, which we want to retry with back-offs.
The reason for doing so is because Cosmos DB follows a provisioned throughput model, and having this retry policy would protect your spark jobs against spikes of data ingress/egress that would momentarily exceed the allocated throughput for your collection, resulting in the request rate limiting exceptions.

*Note - that this retry policy is meant to only protect your spark jobs against momentary spikes. If you have not configured enough RUs on your collection for the intended throughput of your workload such that the retries don't catch up, then the retry policy will result in rethrows.*

## Known Issues

### Left Join API on CassandraTable
Currently, there is an open bug when using the leftJoinWithCassandraTable on cassandraTable. This will be addressed soon, but in the meantime, please avoid using this API in your read paths of your spark jobs.

### Tokens and Token Range Filters
We do not currently support methods that make use of Tokens for filtering data. Hence please avoid using any APIs that perform table scans.

## Resources
- [DataStax Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector)
- [CosmosDB Cassandra API](https://docs.microsoft.com/en-us/azure/cosmos-db/cassandra-introduction)
- [Apache Spark](https://spark.apache.org/docs/latest/index.html)
- [HDI Spark Cluster](https://docs.microsoft.com/en-us/azure/hdinsight/spark/apache-spark-jupyter-spark-sql)
