/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.kafka;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;
import java.util.Objects;

public class KafkaCatalogCapability implements Capability {
  @Override
  public CapabilityResult managedStorage(Scope scope) {
    if (Objects.requireNonNull(scope) == Scope.SCHEMA) {
      return CapabilityResult.SUPPORTED;
    }
    return CapabilityResult.unsupported("Kafka catalog does not support managed storage.");
  }
}