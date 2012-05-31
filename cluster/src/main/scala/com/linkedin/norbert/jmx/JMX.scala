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
package jmx

import management.ManagementFactory
import javax.management.{ObjectInstance, ObjectName, StandardMBean}
import logging.Logging

object JMX extends Logging {
  private val mbeanServer = ManagementFactory.getPlatformMBeanServer

  def register(mbean: AnyRef, name: String): Option[ObjectInstance] = if (System.getProperty("com.linkedin.norbert.disableJMX") == null) try {
    Some(mbeanServer.registerMBean(mbean, new ObjectName(getUniqueName(name))))
  } catch {
    case ex: Exception =>
      log.error(ex, "Error when registering mbean: %s".format(mbean))
      None
  } else {
    None
  }

  val map = collection.mutable.Map.empty[String, Int]

  def getUniqueName(name: String): String = synchronized {
    val id = map.getOrElse(name, -1)
    val unique = if(id == -1) name else name + "-" + id
    map + (name -> (id + 1))
    unique
  }


  def register(mbean: MBean): Option[ObjectInstance] = register(mbean, mbean.name)

  def register(mbean: Option[MBean]): Option[ObjectInstance] = mbean.flatMap(m => register(m, m.name))

  def unregister(mbean: ObjectInstance) = try {
    mbeanServer.unregisterMBean(mbean.getObjectName)
    synchronized { map.remove(mbean.getObjectName.getCanonicalName) }
  } catch {
    case ex: Exception => log.error(ex, "Error while unregistering mbean: %s".format(mbean.getObjectName))
  }

  def name(clientName: Option[String], serviceName: String) =
    if(clientName.isDefined)
      "client=%s,service=%s".format(clientName.get, serviceName)
    else
      "service=%s".format(serviceName)

  class MBean(klass: Class[_], namePropeties: String) extends StandardMBean(klass) {
    def this(klass: Class[_]) = this(klass, null)

    def name: String = {
      val simpleName = klass.getSimpleName
      val mbeanIndex = simpleName.lastIndexOf("MBean")

      val base = "com.linkedin.norbert:type=%s".format(if (mbeanIndex == -1) simpleName else simpleName.substring(0, mbeanIndex))
      if (namePropeties != null) "%s,%s".format(base, namePropeties) else base
    }
  }
}
