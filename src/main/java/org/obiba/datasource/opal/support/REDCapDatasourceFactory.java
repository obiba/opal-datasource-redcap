package org.obiba.datasource.opal.support;

import java.io.IOException;

import com.google.common.base.Strings;
import org.obiba.datasource.opal.REDCapDatasource;
import org.obiba.magma.AbstractDatasourceFactory;
import org.obiba.magma.Datasource;

public class REDCapDatasourceFactory extends AbstractDatasourceFactory {

  private static final String DEFAULT_ENTITY_TYPE = "Participant";

  private String url;

  private String token;

  private String projectName;

  private String entityType;

  private String identifierVariable;

  public REDCapDatasourceFactory() {
  }

  @Override
	protected Datasource internalCreate() {
    try {
      AbstractREDCapProject project = REDCapProjectFactory.create(getUrl(), getToken(), getIdentifierVariable());
      return new REDCapDatasource(getName(), project, getEntityType(), getIdentifierVariable());
    } catch(IOException e) {
      throw new REDCapDatasourceParsingException(e.getMessage(), "", new Object[] { null });
    }
  }

  private String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  private String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  private String getEntityType() {
    return Strings.isNullOrEmpty(entityType) ? DEFAULT_ENTITY_TYPE : entityType;
  }

  public void setEntityType(String entityType) {
    this.entityType = entityType;
  }

  private String getIdentifierVariable() {
    return identifierVariable;
  }

  public void setIdentifierVariable(String identifierVariable) {
    this.identifierVariable = identifierVariable;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }
}