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

import org.specs.SpecificationWithJUnit
import org.specs.mock.{ClassMocker, JMocker}
import org.apache.log4j
import log4j.Level._

class LoggerSpec extends SpecificationWithJUnit with JMocker with ClassMocker {
  "Logger" should {
    "directly log the message for info" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.info("the message: %d", 1)
    }

    "directly log the message and exception for info" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.info(ex, "the message: %d", 1)
    }

    "directly log the message for warn" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.warn("the message: %d", 1)
    }

    "directly log the message and exception for warn" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.warn(ex, "the message: %d", 1)
    }

    "directly log the message for error" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.error("the message: %d", 1)
    }

    "directly log the message and exception for error" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.error(ex, "the message: %d", 1)
    }

    "directly log the message for fatal" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.fatal("the message: %d", 1)
    }

    "directly log the message and exception for fatal" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.fatal(ex, "the message: %d", 1)
    }

    "log the message if debug enabled for ifDebug" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(DEBUG) willReturn true
        one(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.ifDebug("the message: %d", 1)
    }

    "log the message and exception if debug enabled for ifDebug" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).isEnabledFor(DEBUG) willReturn true
        one(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.ifDebug(ex, "the message: %d", 1)
    }

    "not log the message if debug is not enabled for ifDebug" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(DEBUG) willReturn false
      }

      val log = new Logger(wrapped)
      log.ifDebug("the message: %d", 1)
    }

    "log the message if info enabled for ifInfo" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(INFO) willReturn true
        one(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.ifInfo("the message: %d", 1)
    }

    "log the message and exception if info enabled for ifInfo" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).isEnabledFor(INFO) willReturn true
        one(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.ifInfo(ex, "the message: %d", 1)
    }

    "not log the message if info is not enabled for ifInfo" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(INFO) willReturn false
      }

      val log = new Logger(wrapped)
      log.ifInfo("the message: %d", 1)
    }

    "log the message if warn enabled for ifWarn" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(WARN) willReturn true
        one(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.ifWarn("the message: %d", 1)
    }

    "log the message and exception if warn enabled for ifWarn" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).isEnabledFor(WARN) willReturn true
        one(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.ifWarn(ex, "the message: %d", 1)
    }

    "not log the message if warn is not enabled for ifWarn" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(WARN) willReturn false
      }

      val log = new Logger(wrapped)
      log.ifWarn("the message: %d", 1)
    }

    "log the message if error enabled for ifError" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(ERROR) willReturn true
        one(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.ifError("the message: %d", 1)
    }

    "log the message and exception if error enabled for ifError" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).isEnabledFor(ERROR) willReturn true
        one(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.ifError(ex, "the message: %d", 1)
    }

    "not log the message if error is not enabled for ifError" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(ERROR) willReturn false
      }

      val log = new Logger(wrapped)
      log.ifError("the message: %d", 1)
    }

    "log the message if fatal enabled for ifFatal" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(FATAL) willReturn true
        one(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", null)
      }

      val log = new Logger(wrapped)
      log.ifFatal("the message: %d", 1)
    }

    "log the message and exception if fatal enabled for ifFatal" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      expect {
        one(wrapped).isEnabledFor(FATAL) willReturn true
        one(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", ex)
      }

      val log = new Logger(wrapped)
      log.ifFatal(ex, "the message: %d", 1)
    }

    "not log the message if fatal is not enabled for ifFatal" in {
      val wrapped = mock[log4j.Logger]
      expect {
        one(wrapped).isEnabledFor(FATAL) willReturn false
      }

      val log = new Logger(wrapped)
      log.ifFatal("the message: %d", 1)
    }
  }
}
