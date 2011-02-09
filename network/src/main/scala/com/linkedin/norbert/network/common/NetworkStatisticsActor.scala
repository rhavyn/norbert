package com.linkedin.norbert
package network
package common

import logging.Logging
import actors.DaemonActor
import collection.immutable.SortedMap
import jmx.{RequestTimeTracker, RequestsPerSecondTracker}
import util.{Clock, ClockComponent}

class NetworkStatisticsActor[GroupIdType, RequestIdType](averageTimeSize: Int, clock: Clock)(implicit ordering: Ordering[GroupIdType]) extends DaemonActor with Logging {
  object Stats {
    case class BeginRequest(groupId: GroupIdType, requestId: RequestIdType)
    case class EndRequest(groupId: GroupIdType, requestId: RequestIdType)

    case object GetAverageProcessingTime
    case class AverageProcessingTime(map: Map[GroupIdType, Int]) // group_id -> time

    case object GetTotalAverageProcessingTime
    case class TotalAverageProcessingTime(time: Int)

    case object GetAveragePendingTime
    case class AveragePendingTime(map: Map[GroupIdType, Int])

    case object GetTotalAveragePendingTime
    case class TotalAveragePendingTime(time: Int)

    case object GetRequestsPerSecond
    case class RequestsPerSecond(rps: Int)
  }

  private val timeTrackers = collection.mutable.Map.empty[GroupIdType, RequestTimeTracker[RequestIdType]]

  private val rps = new RequestsPerSecondTracker

  def act() = {
    import Stats._

    loop {
      react {
        case BeginRequest(groupId, requestId) =>
          val tracker = timeTrackers.getOrElseUpdate(groupId, new RequestTimeTracker(averageTimeSize, clock))
          tracker.beginRequest(requestId)

        case EndRequest(groupId, requestId) =>
          val tracker = timeTrackers.get(groupId).foreach { _.endRequest(requestId) }
          rps++

        case GetAverageProcessingTime =>
          reply(AverageProcessingTime(timeTrackers.map { case (groupId, tracker) =>
            (groupId, tracker.average)
          }.toMap))

        case GetAveragePendingTime =>
          reply(AveragePendingTime(timeTrackers.map { case (groupId, tracker) =>
            (groupId, tracker.pendingAverage)
          }.toMap))

        case GetTotalAveragePendingTime =>
          reply(TotalAveragePendingTime(timeTrackers.values.foldLeft(0){ _ + _.pendingAverage }))

        case GetTotalAverageProcessingTime =>
          reply(TotalAverageProcessingTime(timeTrackers.values.foldLeft(0) { _ + _.average }))

        case GetRequestsPerSecond => reply(RequestsPerSecond(rps.rps))

        case msg => log.error("NetworkStatistics actor got invalid message: %s".format(msg))
      }
    }
  }
}