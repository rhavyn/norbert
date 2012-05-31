package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.javacompat.cluster.Node;
import java.util.Set;

public interface RequestBuilder<PartitionedId, RequestMsg> {
  RequestMsg apply(Node n, Set<PartitionedId> ids);
}
