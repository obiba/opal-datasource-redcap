package org.obiba.datasource.opal;

import org.json.JSONObject;
import org.obiba.datasource.opal.support.REDCapDatasourceFactory;
import org.obiba.magma.DatasourceFactory;
import org.obiba.opal.spi.datasource.AbstractDatasourceService;
import org.obiba.opal.spi.datasource.DatasourceUsage;

public class REDCapDatasourceService extends AbstractDatasourceService {

  @Override
  public String getName() {
    return "opal-datasource-redcap";
  }

  @Override
  public DatasourceFactory createDatasourceFactory(DatasourceUsage usage, JSONObject parameters) {
    REDCapDatasourceFactory redcapDatasourceFactory = new REDCapDatasourceFactory();
    redcapDatasourceFactory.setUrl(parameters.optString("url"));
    redcapDatasourceFactory.setToken(parameters.optString("token"));
    redcapDatasourceFactory.setEntityType(parameters.optString("entity_type"));
    redcapDatasourceFactory.setIdentifierVariable(parameters.optString("id_variable"));
    redcapDatasourceFactory.setIdentifierPrefix(parameters.optString("id_prefix"));
    redcapDatasourceFactory.setIdentifierSuffix(parameters.optString("id_suffix"));

    return redcapDatasourceFactory;
  }

}