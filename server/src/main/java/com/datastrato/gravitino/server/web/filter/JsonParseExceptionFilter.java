/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.server.web.Utils;
import com.fasterxml.jackson.core.JsonParseException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class JsonParseExceptionFilter implements ExceptionMapper<JsonParseException> {
  @Override
  public Response toResponse(JsonParseException e) {
    String errorMsg = "Malformed request, error occurs when json processing";
    return Utils.illegalArguments(errorMsg, e);
  }
}
