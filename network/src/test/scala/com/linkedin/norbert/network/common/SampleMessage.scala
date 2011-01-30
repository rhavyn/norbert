package com.linkedin.norbert.network.common

import com.linkedin.norbert.network.{JavaSerializer, Serializer}

trait SampleMessage {
  class Ping(ts: Long = System.currentTimeMillis)
  val request = new Ping
  implicit val serializer: Serializer[Ping, Ping] = JavaSerializer.build
}