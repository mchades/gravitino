/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.server.web.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class JsonProcessingExceptionFilter implements ExceptionMapper<JsonProcessingException> {
  @Override
  public Response toResponse(JsonProcessingException e) {
    String errorMsg = "Malformed request, error occurs when json processing";
    return Utils.illegalArguments(errorMsg, e);
  }
}
