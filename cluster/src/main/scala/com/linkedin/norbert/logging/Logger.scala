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
package logging

import org.apache.log4j.{Level, Logger => l4jLogger}
import Level._

/**
 * A wrapper around a Log4j <code>Logger</code> which provides higher level methods to reduce boilerplate.
 */
class Logger(wrapped: l4jLogger) {
  private val fqcn = this.getClass.getName

  /**
   * Logs a message at trace level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def trace(msg: => String) = log(TRACE, msg)

  /**
   * Logs an exception and message at trace level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def trace(cause: Throwable, msg: => String) = log(TRACE, msg, cause)

  /**
   * Logs a message at debug level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def debug(msg: => String) = log(DEBUG, msg)

  /**
   * Logs an exception and message at debug level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def debug(cause: Throwable, msg: => String) = log(DEBUG, msg, cause)

  /**
   * Logs a message at info level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def info(msg: => String) = log(INFO, msg)

  /**
   * Logs an exception and message at info level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def info(cause: Throwable, msg: => String) = log(INFO, msg, cause)

  /**
   * Logs a message at warn level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def warn(msg: => String) = log(WARN, msg)

  /**
   * Logs an exception and message at warn level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def warn(cause: Throwable, msg: => String) = log(WARN, msg, cause)

  /**
   * Logs a message at error level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def error(msg: => String) = log(ERROR, msg)

  /**
   * Logs an exception and message at error level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def error(cause: Throwable, msg: => String) = log(ERROR, msg, cause)

  /**
   * Logs a message at fatal level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def fatal(msg: => String) = log(FATAL, msg)

  /**
   * Logs an exception and message at fatal level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def fatal(cause: Throwable, msg: => String) = log(FATAL, msg, cause)

  private def log(level: Level, msg: => String, cause: Throwable = null) = {
    if (wrapped.isEnabledFor(level)) wrapped.log(fqcn, level, msg, cause)
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
