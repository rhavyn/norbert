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
package com.linkedin.norbert.jmx

import management.ManagementFactory
import com.linkedin.norbert.logging.Logging
import javax.management.{ObjectName, StandardMBean}

object JMX extends Logging {
  private val mbeanServer = ManagementFactory.getPlatformMBeanServer

  def register(mbean: AnyRef, name: String): Unit = try {
    mbeanServer.registerMBean(mbean, new ObjectName(name))
  } catch {
    case ex: Exception => log.error(ex, "Error when registering mbean: %s".format(mbean))
  }

  def register(mbean: MBean): Unit = register(mbean, mbean.name)

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
