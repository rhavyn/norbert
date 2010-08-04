/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package notifications

import actors.DaemonActor
import java.util.concurrent.atomic.AtomicInteger

object Observer {
  def apply(pf: PartialFunction[Notification, Unit]): Observer = new Observer {
    def handleNotification(notification: Notification) = if (pf.isDefinedAt(notification)) pf(notification)
  }
}

trait Observer {
  def handleNotification(notification: Notification): Unit
}

trait Notification

final class ObserverKey(private val value: Int) {
  override def equals(obj: Any) = obj match {
    case key: ObserverKey => key.value == value
    case _ => false
  }

  override def hashCode = value
}

object NotificationCenter {
  val defaultNotificationCenter: NotificationCenter = new NotificationCenter
}

class NotificationCenter {
  private val counter = new AtomicInteger
  private val a = new NotificationCenterActor
  a.start

  def addObserver(observer: Observer): ObserverKey = {
    val key = new ObserverKey(counter.incrementAndGet)
    a ! AddObserver(key, observer)
    key
  }

  def removeObserver(observerKey: ObserverKey): Unit = a ! RemoveObserver(observerKey)
  def postNotification(notification: Notification): Unit = a ! PostNotification(notification)

  private[notifications] def shutdown: Unit = a ! Shutdown

  private case class AddObserver(observerKey: ObserverKey, observer: Observer)
  private case class RemoveObserver(observerKey: ObserverKey)
  private case class PostNotification(notification: Notification)
  private case object Shutdown

  private class NotificationCenterActor extends DaemonActor {
    private var observers = Map.empty[ObserverKey, ObserverActor]

    def act() = {
      loop {
        react {
          case AddObserver(key, observer) =>
            val a = new ObserverActor(observer)
            a.start
            observers += (key -> a)

          case RemoveObserver(key) =>
            observers.get(key).foreach { a =>
              observers -= key
              a ! 'exit
            }

          case PostNotification(n) => observers.values.foreach { _ ! n }

          case Shutdown =>
            observers.values.foreach { _ ! 'exit }
            exit
        }
      }
    }
  }

  private class ObserverActor(observer: Observer) extends DaemonActor {
    def act() = {
      loop {
        react {
          case n: Notification => observer.handleNotification(n)
          case _ => exit
        }
      }
    }
  }
}
