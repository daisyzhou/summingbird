package com.twitter.summingbird.example

import com.twitter.summingbird.store.ClientStore

/**
 * Runs an example hybrid online/offline job for counting words in tweets.
 */
object HybridRunner {

  // --------- Offline store (In this case, on the local filesystem).
  val offlineStore = ScaldingRunner.store

  // --------- Online store (In this case, Storm + memcached from the original example).
  val onlineStore = StormRunner.storeSupplier

  // --------- Hybrid client store.
  val clientStore = ClientStore(offlineStore, onlineStore, 3)

  /**
   * Main method which does the running.
   */

  /**
   * Method to read the values.
   */
}
