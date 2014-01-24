package com.twitter.summingbird.example

import com.twitter.scalding.{Hdfs, Args, TextLine}

import org.apache.hadoop.conf.Configuration
import twitter4j.Status
import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.batch.Timestamp
import com.twitter.summingbird.scalding._
import com.twitter.summingbird.scalding.store.VersionedStore
import com.twitter.summingbird.example.StatusStreamer._
import com.twitter.tormenta.spout.TwitterSpout
import twitter4j.TwitterStreamFactory
import com.twitter.summingbird.{TimeExtractor, Producer}
import com.twitter.summingbird.scalding.state.HDFSState

object ScaldingRunner {
  import Serialization._, ScaldingExampleStreamer._

  final val millisInHour = 60 * 60 * 1000

  val jobDir = "/tmp/scalding-store-test"

  val waitingState =
    HDFSState(
      jobDir + "/waitstate",
      startTime = Some(Timestamp(System.currentTimeMillis() - 2 * millisInHour)),
      numBatches = 3)

  val mode = Hdfs(false, new Configuration())

  val source = Producer.source[Scalding, String](Scalding.pipeFactoryExact(_ => TextLine(jobDir + "/input.txt")))
  val store = new InitialBatchedStore(batcher.currentBatch - 2L, versionedStore)
  val versionedStore: VersionedBatchStore[String, Long, String, (BatchID, Long)] = VersionedStore[String, Long](jobDir + "/store")

  def apply(args: Args): ScaldingExecutionConfig = {
    new ScaldingExecutionConfig {
      override val name = "SummingbirdScaldingExample"

      override def graph = wordCount[Scalding](source, store)

      override def getWaitingState (
        hadoopConfig: Configuration,
        startDate: Option[Timestamp],
        batches: Int) = waitingState
    }
  }

  def main(args: Array[String]) {
    val job =  Scalding("wordcountJob")
    job.run(waitingState, mode, job.plan(wordCount[Scalding](source, store)))

    // Lookup results
    lookup()
  }

  def lookup() {
    println("\nRESULTS: \n")

    val results = store.readLast(StatusStreamer.batcher.currentBatch, mode)
    println(results)
  }
}
