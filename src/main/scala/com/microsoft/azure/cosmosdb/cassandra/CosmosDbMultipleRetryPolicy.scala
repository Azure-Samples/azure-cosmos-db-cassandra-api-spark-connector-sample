/**
  * <copyright file="CosmosDbMultipleRetryPolicy.scala" company="Microsoft">
  * Copyright (c) Microsoft. All rights reserved.
  * </copyright>
  */

package com.microsoft.azure.cosmosdb.cassandra

import com.datastax.driver.core.exceptions._
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement}
import com.datastax.spark.connector.cql.MultipleRetryPolicy


/**
  * This retry policy extends the MultipleRetryPolicy, and additionally performs retries with back-offs for overloaded exceptions. For more details regarding this, please refer to the "Retry Policy" section of README.md
  */
class CosmosDbMultipleRetryPolicy(maxRetryCount: Int)
  extends MultipleRetryPolicy(maxRetryCount){

  /**
    * The retry policy performs growing/fixed back-offs for overloaded exceptions based on the max retries:
    * 1. If Max retries == -1, i.e., retry infinitely, then we follow a fixed back-off scheme of 5 seconds on each retry.
    * 2. If Max retries != -1, and is any positive number n, then we follow a growing back-off scheme of (i*1) seconds where 'i' is the i'th retry.
    * If you'd like to modify the back-off intervals, please update GrowingBackOffTimeMs and FixedBackOffTimeMs accordingly.
    */
  val GrowingBackOffTimeMs: Int = 1000
  val FixedBackOffTimeMs: Int = 5000

  // scalastyle:off null
  private def retryManyTimesWithBackOffOrThrow(nbRetry: Int): RetryDecision = maxRetryCount match {
    case -1 =>
      Thread.sleep(FixedBackOffTimeMs)
      RetryDecision.retry(null)
    case maxRetries =>
      if (nbRetry < maxRetries) {
        Thread.sleep(GrowingBackOffTimeMs * nbRetry)
        RetryDecision.retry(null)
      } else {
        RetryDecision.rethrow()
      }
  }

  override def init(cluster: com.datastax.driver.core.Cluster): Unit = {}
  override def close(): Unit = {}

  override def onRequestError(
                               stmt: Statement,
                               cl: ConsistencyLevel,
                               ex: DriverException,
                               nbRetry: Int): RetryDecision = {
    ex match {
      case _: OverloadedException => retryManyTimesWithBackOffOrThrow(nbRetry)
      case _ => RetryDecision.rethrow()
    }
  }
}