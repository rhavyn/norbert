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
package util

import org.specs.Specification

class GuardChainSpec extends Specification {
  "GuardChain" should {
    "execute 'then' if the predicate is true" in {
      GuardChain(true, throw new Exception("Failed")) then { "then" } must be_==("then")
    }

    "execute 'otherwise' if the predicate is false" in {
      GuardChain(false, "otherwise") then { "then" } must be_==("otherwise")
    }

    "handle an exception throwing otherwise and a value returning then" in {
      GuardChain(true, throw new Exception) then { "then" } must be_==("then")
      GuardChain(true, throw new Exception) { "then" } must be_==("then")
    }

    "execute a chained guard if predicate is true" in {
      GuardChain(true, throw new Exception("failed")) and GuardChain(true, throw new Exception("Failed 2")) then { "then" } must be_==("then")
    }

    "not execute a chained guard if the predicate is false" in {
      GuardChain(false, "otherwise1") and GuardChain(true, "otherwise2") then { "then" } must be_==("otherwise1")
    }

    "handle throws" in {
      GuardChain(false, throw new Exception) and GuardChain(true, "otherwise") then { "then" } must throwA[Exception]
    }

    "handle throws 2" in {
      GuardChain(true, throw new Exception) and GuardChain(true, "otherwise") then { "then" } must be_==("then")
    }
  }
}
