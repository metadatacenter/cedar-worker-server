package org.metadatacenter.cedar.worker.resources;

import org.metadatacenter.config.CedarConfig;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

public abstract class AbstractWorkerResource {

  protected
  @Context
  UriInfo uriInfo;

  protected
  @Context
  HttpServletRequest request;

  protected
  @Context
  HttpServletResponse response;

  protected final CedarConfig cedarConfig;

  public AbstractWorkerResource(CedarConfig cedarConfig) {
    this.cedarConfig = cedarConfig;
  }

}
