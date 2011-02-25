package com.linkedin.norbert
package network
package common

import logging.Logging
import actors.DaemonActor
import collection.immutable.SortedMap
import jmx.{RequestTimeTracker}
import norbertutils.{Clock, ClockComponent}

class NetworkStatisticsActor[GroupIdType, RequestIdType](clock: Clock, timeWindow: Long) extends DaemonActor with Logging {
  object Stats {
    case class BeginRequest(groupId: GroupIdType, requestId: RequestIdType)
    case class EndRequest(groupId: GroupIdType, requestId: RequestIdType)

    case class GetProcessingStatistics(percentile: Option[Double] = None)

    case class ProcessingEntry(pendingTime: Long,
                               pendingSize: Int,
                               completedTime: Long,
                               completedSize: Int,
                               percentile: Option[Int] = None)

    case class ProcessingStatistics(map: Map[GroupIdType, ProcessingEntry])

    case object GetRequestsPerSecond
    case class RequestsPerSecond(rps: Map[GroupIdType, Int])

    case object Reset

    def average[T : Numeric](total: T, size: Int) = if(size == 0) 0.0 else implicitly[Numeric[T]].toDouble(total) / size

    def average[T, V](map: Map[T, V])(timeFn: (V => Long))( sizeFn: (V => Int)): Double = {
      val (total, size) = map.values.foldLeft((0L, 0)) { case ((t, s), value) =>
          (t + timeFn(value), s + sizeFn(value))
        }
      average(total, size)
    }
  }

  private var timeTrackers = Map.empty[GroupIdType, RequestTimeTracker[RequestIdType]]

  def getOrUpdateTrackers(groupId: GroupIdType, fn: => RequestTimeTracker[RequestIdType]) = {
    timeTrackers.get(groupId) match {
      case Some(value) => value
      case None =>
        val value = fn
        timeTrackers += (groupId -> value)
        value
    }
  }

  def act() = {
    import Stats._

    loop {
      react {
        case BeginRequest(groupId, requestId) =>
          val tracker = getOrUpdateTrackers(groupId, new RequestTimeTracker(clock, timeWindow))
          tracker.beginRequest(requestId)

        case EndRequest(groupId, requestId) =>
          val tracker = timeTrackers.get(groupId).foreach { _.endRequest(requestId) }

        case GetProcessingStatistics(percentile) =>
          reply(ProcessingStatistics(timeTrackers.mapValues { tracker =>
              ProcessingEntry(tracker.pendingRequestTimeTracker.total,
                tracker.pendingRequestTimeTracker.size,
                tracker.finishedRequestTimeTracker.total,
                tracker.finishedRequestTimeTracker.size,
                percentile.map(tracker.finishedRequestTimeTracker.percentile(_)))
          }))

        case GetRequestsPerSecond =>
          reply(RequestsPerSecond(timeTrackers.mapValues(_.finishedRequestTimeTracker.rps)))

        case Reset =>
          timeTrackers = timeTrackers.empty

        case msg => log.error("NetworkStatistics actor got invalid message: %s".format(msg))
      }
    }
  }
}