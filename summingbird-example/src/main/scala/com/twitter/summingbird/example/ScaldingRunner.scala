package com.twitter.summingbird.example

import com.twitter.scalding.Args
import com.twitter.scalding.TextLine

import com.twitter.summingbird.scalding.Scalding
import com.twitter.summingbird.scalding.ScaldingExecutionConfig
import com.twitter.summingbird.scalding.ScaldingStore
import com.twitter.summingbird.scalding.store.VersionedStore
import com.twitter.summingbird.example.StatusStreamer._
import com.twitter.tormenta.spout.TwitterSpout
import twitter4j.TwitterStreamFactory
import com.twitter.summingbird.Producer

object ScaldingRunner {
  def apply(args: Args): ScaldingExecutionConfig = {
    new ScaldingExecutionConfig {
      val jobDir = "/tmp/scalding-store-test"

      override val name = "SummingbirdScaldingExample"

      val source = Scalding.pipeFactoryExact(_ => TextLine(jobDir + "/input.txt"))

      val store: ScaldingStore[String, Long] =
        Producer.source[Scalding, String](VersionedStore[String, Long](jobDir + "/store"))

      override def graph = wordCount[Scalding](source, store)

    }
  }
}
