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

import org.specs.Specification
import org.apache.log4j
import log4j.Level._
import org.specs.mock.Mockito

class LoggerSpec extends Specification with Mockito {
  "Logger" should {
    "log the message if debug enabled for debug" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(DEBUG) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", null)

      val log = new Logger(wrapped)
      log.debug("the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(DEBUG)
        one(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", null)
      }
    }

    "log the message and exception if debug enabled for debug" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      wrapped.isEnabledFor(DEBUG) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", ex)

      val log = new Logger(wrapped)
      log.debug(ex, "the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(DEBUG)
        one(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", ex)
      }
    }

    "not log the message if debug is not enabled for debug" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(DEBUG) returns false

      val log = new Logger(wrapped)
      log.debug("the message: %d".format(1))

      there was one(wrapped).isEnabledFor(DEBUG)
      there was no(wrapped).log(classOf[Logger].getName, DEBUG, "the message: 1", null)
    }

    "log the message if info enabled for info" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(INFO) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", null)

      val log = new Logger(wrapped)
      log.info("the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(INFO)
        one(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", null)
      }
    }

    "log the message and exception if info enabled for info" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      wrapped.isEnabledFor(INFO) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", ex)

      val log = new Logger(wrapped)
      log.info(ex, "the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(INFO)
        one(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", ex)
      }
    }

    "not log the message if info is not enabled for info" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(INFO) returns false

      val log = new Logger(wrapped)
      log.info("the message: %d".format(1))

      there was one(wrapped).isEnabledFor(INFO)
      there was no(wrapped).log(classOf[Logger].getName, INFO, "the message: 1", null)
    }

    "log the message if warn enabled for warn" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(WARN) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", null)

      val log = new Logger(wrapped)
      log.warn("the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(WARN)
        one(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", null)
      }
    }

    "log the message and exception if warn enabled for warn" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      wrapped.isEnabledFor(WARN) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", ex)

      val log = new Logger(wrapped)
      log.warn(ex, "the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(WARN)
        one(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", ex)
      }
    }

    "not log the message if warn is not enabled for warn" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(WARN) returns false

      val log = new Logger(wrapped)
      log.warn("the message: %d".format(1))

      there was one(wrapped).isEnabledFor(WARN)
      there was no(wrapped).log(classOf[Logger].getName, WARN, "the message: 1", null)
    }

    "log the message if error enabled for error" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(ERROR) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", null)

      val log = new Logger(wrapped)
      log.error("the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(ERROR)
        one(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", null)
      }
    }

    "log the message and exception if error enabled for error" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      wrapped.isEnabledFor(ERROR) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", ex)

      val log = new Logger(wrapped)
      log.error(ex, "the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(ERROR)
        one(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", ex)
      }
    }

    "not log the message if error is not enabled for error" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(ERROR) returns false

      val log = new Logger(wrapped)
      log.error("the message: %d".format(1))

      there was one(wrapped).isEnabledFor(ERROR)
      there was no(wrapped).log(classOf[Logger].getName, ERROR, "the message: 1", null)
    }

    "log the message if fatal enabled for fatal" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(FATAL) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", null)

      val log = new Logger(wrapped)
      log.fatal("the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(FATAL)
        one(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", null)
      }
    }

    "log the message and exception if fatal enabled for fatal" in {
      val wrapped = mock[log4j.Logger]
      val ex = new Exception

      wrapped.isEnabledFor(FATAL) returns true
      doNothing.when(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", ex)

      val log = new Logger(wrapped)
      log.fatal(ex, "the message: %d".format(1))

      got {
        one(wrapped).isEnabledFor(FATAL)
        one(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", ex)
      }
    }

    "not log the message if fatal is not enabled for fatal" in {
      val wrapped = mock[log4j.Logger]

      wrapped.isEnabledFor(FATAL) returns false

      val log = new Logger(wrapped)
      log.fatal("the message: %d".format(1))

      there was one(wrapped).isEnabledFor(FATAL)
      there was no(wrapped).log(classOf[Logger].getName, FATAL, "the message: 1", null)
    }
  }
}
