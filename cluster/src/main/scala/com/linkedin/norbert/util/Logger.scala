/*
 * Copyright 2009 LinkedIn, Inc
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
package com.linkedin.norbert.util

import org.apache.log4j.{Level, Logger => l4jLogger}
import Level._

class Logger(wrapped: l4jLogger) {
  val fqcn = this.getClass.getName

  def info(message: String, items: Any*) = logDirect(INFO, message, items: _*)
  def info(cause: Throwable, message: String, items: Any*) = logDirect(INFO, cause, message, items: _*)
  def warn(message: String, items: Any*) = logDirect(WARN, message, items: _*)
  def warn(cause: Throwable, message: String, items: Any*) = logDirect(WARN, cause, message, items: _*)
  def error(message: String, items: Any*) = logDirect(ERROR, message, items: _*)
  def error(cause: Throwable, message: String, items: Any*) = logDirect(ERROR, cause, message, items: _*)
  def fatal(message: String, items: Any*) = logDirect(FATAL, message, items: _*)
  def fatal(cause: Throwable, message: String, items: Any*) = logDirect(FATAL, cause, message, items: _*)

  def ifTrace(message: String, items: Any*) = log(TRACE, message, items: _*)
  def ifTrace(cause: Throwable, message: String, items: Any*) = log(TRACE, cause, message, items: _*)
  def ifDebug(message: String, items: Any*) = log(DEBUG, message, items: _*)
  def ifDebug(cause: Throwable, message: String, items: Any*) = log(DEBUG, cause, message, items: _*)
  def ifInfo(message: String, items: Any*) = log(INFO, message, items: _*)
  def ifInfo(cause: Throwable, message: String, items: Any*) = log(INFO, cause, message, items: _*)
  def ifWarn(message: String, items: Any*) = log(WARN, message, items: _*)
  def ifWarn(cause: Throwable, message: String, items: Any*) = log(WARN, cause, message, items: _*)
  def ifError(message: String, items: Any*) = log(ERROR, message, items: _*)
  def ifError(cause: Throwable, message: String, items: Any*) = log(ERROR, cause, message, items: _*)
  def ifFatal(message: String, items: Any*) = log(FATAL, message, items: _*)
  def ifFatal(cause: Throwable, message: String, items: Any*) = log(FATAL, cause, message, items: _*)

  private def logDirect(level: Level, message: String, items: Any*) = {
    wrapped.log(fqcn, level, message.format(items: _*), null)
  }

  private def logDirect(level: Level, cause: Throwable, message: String, items: Any*) = {
    wrapped.log(fqcn, level, message.format(items: _*), cause)
  }

  private def log(level: Level, message: String, items: Any*) = {
    if (wrapped.isEnabledFor(level)) wrapped.log(fqcn, level, message.format(items: _*), null)
  }

  private def log(level: Level, cause: Throwable, message: String, items: Any*) = {
    if (wrapped.isEnabledFor(level)) wrapped.log(fqcn, level, message.format(items: _*), cause)
  }
}

object Logger {
  def loggerNameForClass(className: String) = className.indexOf("$") match {
    case -1 => className
    case index => className.substring(0, index)
  }

  def apply(name: String): Logger = new Logger(l4jLogger.getLogger(name))

  def apply(ref: AnyRef): Logger = Logger(loggerNameForClass(ref.getClass.getName))

  def get: Logger = Logger(loggerNameForClass(new Throwable().getStackTrace()(1).getClassName))
}
