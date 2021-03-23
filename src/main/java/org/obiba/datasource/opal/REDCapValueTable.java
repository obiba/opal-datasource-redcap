/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.datasource.opal;

import org.obiba.datasource.opal.support.AbstractREDCapProject;
import org.obiba.datasource.opal.support.REDCapDatasourceParsingException;
import org.obiba.datasource.opal.support.REDCapVariableValueSourceFactory;
import org.obiba.magma.*;
import org.obiba.magma.support.AbstractValueTable;
import org.obiba.magma.support.VariableEntityProvider;
import org.obiba.magma.type.DateTimeType;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class REDCapValueTable extends AbstractValueTable implements Disposable {

  private final AbstractREDCapProject project;

  private final Map<String, Map<String, String>> metadata;

  private final List<Map<String, String>> records = new CopyOnWriteArrayList<Map<String, String>>();

  private final String identifierVariable;

  private final String identifierPrefix;

  private final String identifierSuffix;

  REDCapValueTable(@NotNull Datasource datasource,
                   @NotNull String name,
                   @NotNull String entityType,
                   @NotNull AbstractREDCapProject project,
                   @NotNull String identifierVariable,
                   @Nullable String identifierPrefix,
                   @Nullable String identifierSuffix) {
    super(datasource, name);
    this.project = project;
    this.identifierVariable = identifierVariable;
    this.identifierPrefix = identifierPrefix;
    this.identifierSuffix = identifierSuffix;

    try {
      Map<String, Map<String, String>> metadata = project.getMetadata(name);

      if (metadata.containsKey(identifierVariable)) {
        metadata.remove(identifierVariable);
        this.metadata = metadata;
        setVariableEntityProvider(new REDCapVariableEntityProvider(entityType));
      } else {
        throw new REDCapDatasourceParsingException("ID Variable not found", "");
      }

    } catch (IOException e) {
      throw new REDCapDatasourceParsingException(e.getMessage(), "");
    }

  }

  @Override
  protected ValueSetBatch getValueSetsBatch(List<VariableEntity> entities) {
    try {
      List<String> recordIds = entities.stream()
          .map(entity -> (REDCapVariableEntityBean) entity)
          .map(REDCapVariableEntityBean::getOriginalIdentifier)
          .collect(Collectors.toList());
      records.addAll(project.getRecords(recordIds, name));
    } catch (IOException e) {
      throw new REDCapDatasourceParsingException(e.getMessage(), "");
    }
    return super.getValueSetsBatch(entities);
  }

  @Override
  public void initialise() {
    addVariableValueSources(new REDCapVariableValueSourceFactory(getEntityType(), metadata));
    super.initialise();
  }

  @Override
  public void dispose() {

  }

  @Override
  public ValueSet getValueSet(VariableEntity variableEntity) throws NoSuchValueSetException {
    return new REDCapValueSet(this, variableEntity, identifierVariable, records);
  }

  @NotNull
  @Override
  public Timestamps getTimestamps() {
    return new Timestamps() {
      @NotNull
      @Override
      public Value getLastUpdate() {
        Date lastModified = new Date();
        return DateTimeType.get().valueOf(lastModified);
      }

      @NotNull
      @Override
      public Value getCreated() {
        return DateTimeType.get().nullValue();
      }
    };
  }

  private class REDCapVariableEntityProvider implements VariableEntityProvider {

    private final String entityType;

    private List<VariableEntity> variableEntities;

    REDCapVariableEntityProvider(String entityType) {
      this.entityType = entityType;
    }

    @NotNull
    @Override
    public String getEntityType() {
      return entityType;
    }

    @Override
    public boolean isForEntityType(String anEntityType) {
      return getEntityType().equals(anEntityType);
    }

    @NotNull
    @Override
    public List<VariableEntity> getVariableEntities() {
      if (variableEntities == null) {
        variableEntities = getVariableEntitiesInternal();
      }

      return Collections.unmodifiableList(variableEntities);
    }

    private List<VariableEntity> getVariableEntitiesInternal() {
      try {
        return project.getIdentifiers(name)
            .stream()
            .map(identifier -> new REDCapVariableEntityBean(entityType, identifier, identifierPrefix, identifierSuffix))
            .distinct()
            .collect(Collectors.toList());
      } catch (IOException e) {
        throw new REDCapDatasourceParsingException(e.getMessage(), "");
      }
    }
  }
}
