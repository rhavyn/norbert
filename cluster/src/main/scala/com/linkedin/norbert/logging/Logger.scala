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
package com.linkedin.norbert.logging

import org.apache.log4j.{Level, Logger => l4jLogger}
import Level._

/**
 * A wrapper around a Log4j <code>Logger</code> which provides higher level methods to reduce boilerplate.
 */
class Logger(wrapped: l4jLogger) {
  val fqcn = this.getClass.getName

  /**
   * Logs a message at info level.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def info(message: String, items: Any*) = logDirect(INFO, message, items: _*)

  /**
   * Logs an exception and message at info level.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def info(cause: Throwable, message: String, items: Any*) = logDirect(INFO, cause, message, items: _*)

  /**
   * Logs a message at warn level.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def warn(message: String, items: Any*) = logDirect(WARN, message, items: _*)

  /**
   * Logs an exception and message at warn level.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def warn(cause: Throwable, message: String, items: Any*) = logDirect(WARN, cause, message, items: _*)

  /**
   * Logs a message at error level.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def error(message: String, items: Any*) = logDirect(ERROR, message, items: _*)

  /**
   * Logs an exception and message at error level.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def error(cause: Throwable, message: String, items: Any*) = logDirect(ERROR, cause, message, items: _*)

  /**
   * Logs a message at fatal level.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def fatal(message: String, items: Any*) = logDirect(FATAL, message, items: _*)

  /**
   * Logs an exception and message at fatal level.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def fatal(cause: Throwable, message: String, items: Any*) = logDirect(FATAL, cause, message, items: _*)

  /**
   * Logs a message at trace level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifTrace(message: String, items: Any*) = log(TRACE, message, items: _*)

  /**
   * Logs an exception and message at trace level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifTrace(cause: Throwable, message: String, items: Any*) = log(TRACE, cause, message, items: _*)

  /**
   * Logs a message at debug level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifDebug(message: String, items: Any*) = log(DEBUG, message, items: _*)

  /**
   * Logs an exception and message at debug level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifDebug(cause: Throwable, message: String, items: Any*) = log(DEBUG, cause, message, items: _*)

  /**
   * Logs a message at info level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifInfo(message: String, items: Any*) = log(INFO, message, items: _*)

  /**
   * Logs an exception and message at info level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifInfo(cause: Throwable, message: String, items: Any*) = log(INFO, cause, message, items: _*)

  /**
   * Logs a message at warn level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifWarn(message: String, items: Any*) = log(WARN, message, items: _*)

  /**
   * Logs an exception and message at warn level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifWarn(cause: Throwable, message: String, items: Any*) = log(WARN, cause, message, items: _*)

  /**
   * Logs a message at error level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifError(message: String, items: Any*) = log(ERROR, message, items: _*)

  /**
   * Logs an exception and message at error level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifError(cause: Throwable, message: String, items: Any*) = log(ERROR, cause, message, items: _*)

  /**
   * Logs a message at fatal level if that level is enabled.
   *
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
  def ifFatal(message: String, items: Any*) = log(FATAL, message, items: _*)

  /**
   * Logs an exception and message at fatal level if that level is enabled.
   *
   * @param exception the exception to log
   * @param message a printf style message to log
   * @param items the substitution values for the printf placeholders
   */
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
