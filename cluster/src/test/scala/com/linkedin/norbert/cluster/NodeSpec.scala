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
package cluster

import org.specs.Specification
import protos.NorbertProtos

class NodeSpec extends Specification {
  "Node" should {
    "serialize into the correct format" in {
      val builder = NorbertProtos.Node.newBuilder
      builder.setId(1)
      builder.setUrl("localhost:31313")
      builder.addPartition(0).addPartition(1)
      val bytes = builder.build.toByteArray

      List(Node(1, "localhost:31313", false, Set(0, 1)).toByteArray: _*) must containInOrder(List(bytes: _*))
    }

    "deserialize into the corrent Node" in {
      val builder = NorbertProtos.Node.newBuilder
      builder.setId(1)
      builder.setUrl("localhost:31313")
      builder.addPartition(0).addPartition(1)
      val bytes = builder.build.toByteArray

      Node(bytes, true) must be_==(Node(1, "localhost:31313", true, Set(0, 1)))
    }
  }
}
