package org.metadatacenter.cedar.worker.resources;

import org.metadatacenter.cedar.util.dw.CedarMicroserviceResource;
import org.metadatacenter.config.CedarConfig;

public abstract class AbstractWorkerResource extends CedarMicroserviceResource {

  public AbstractWorkerResource(CedarConfig cedarConfig) {
    super(cedarConfig);
  }
}
