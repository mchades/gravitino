/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.bili.onemeta.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.bili.onemeta.Utils;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.responses.MetalakeListResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.MetalakeManager;
import com.datastrato.gravitino.metrics.MetricNames;
import java.util.Arrays;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/onemeta/metalakes")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MetalakeOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeOperations.class);

  private final MetalakeManager manager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeOperations(MetalakeManager manager) {
    this.manager = manager;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-metalake", absolute = true)
  public Response listMetalakes() {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            BaseMetalake[] metalakes = manager.listMetalakes();
            MetalakeDTO[] metalakeDTOS =
                Arrays.stream(metalakes).map(DTOConverters::toDTO).toArray(MetalakeDTO[]::new);
            return Utils.ok(new MetalakeListResponse(metalakeDTOS));
          });

    } catch (Exception e) {
      return null;
    }
  }
}
