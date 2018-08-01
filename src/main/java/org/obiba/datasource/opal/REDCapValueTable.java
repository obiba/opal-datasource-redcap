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

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.obiba.datasource.opal.support.REDCapClient;
import org.obiba.datasource.opal.support.REDCapDatasourceParsingException;
import org.obiba.datasource.opal.support.REDCapVariableValueSourceFactory;
import org.obiba.magma.Datasource;
import org.obiba.magma.Disposable;
import org.obiba.magma.NoSuchValueSetException;
import org.obiba.magma.Timestamps;
import org.obiba.magma.Value;
import org.obiba.magma.ValueSet;
import org.obiba.magma.VariableEntity;
import org.obiba.magma.support.AbstractValueTable;
import org.obiba.magma.support.VariableEntityBean;
import org.obiba.magma.support.VariableEntityProvider;
import org.obiba.magma.type.DateTimeType;

import com.google.common.collect.ImmutableSet;

public class REDCapValueTable extends AbstractValueTable implements Disposable {

  private final REDCapClient client;

  private final Map<String, Map<String, String>> metadata;

  private final List<Map<String, String>> records;

  private final String idVariable;

  REDCapValueTable(@NotNull REDCapClient client, @NotNull Datasource datasource, @NotNull String name,
      @NotNull String entityType, @NotNull String idVariable) {
    super(datasource, name);
    this.client = client;
    this.idVariable = idVariable;

    try {
      metadata = client.getMetadata();
      records = client.getRecords();

      if (metadata.containsKey(idVariable)) {
        setVariableEntityProvider(new REDCapVariableEntityProvider(entityType));
      } else {
         throw new REDCapDatasourceParsingException("ID Variable not found", "", null);
      }

    } catch(IOException e) {
      throw new REDCapDatasourceParsingException(e.getMessage(), "", null);
    }

  }

  @Override
  public void initialise() {
    addVariableValueSources(new REDCapVariableValueSourceFactory(getEntityType(), metadata, records, idVariable));
    super.initialise();
  }

  @Override
  public void dispose() {

  }

  @Override
  public ValueSet getValueSet(VariableEntity variableEntity) throws NoSuchValueSetException {
    return new REDCapValueSet(this, variableEntity, idVariable, records);
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
        // Not currently possible to read a file creation timestamp. Coming in JDK 7 NIO.
        return DateTimeType.get().nullValue();
      }
    };
  }

  private class REDCapVariableEntityProvider implements VariableEntityProvider {

    private final String entityType;

    private Set<VariableEntity> variableEntities;

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
    public Set<VariableEntity> getVariableEntities() {
      if(variableEntities == null) {
        variableEntities = getVariableEntitiesInternal();
      }

      return Collections.unmodifiableSet(variableEntities);
    }

    private Set<VariableEntity> getVariableEntitiesInternal() {
      ImmutableSet.Builder<VariableEntity> entitiesBuilder = ImmutableSet.builder();
      records.forEach(record -> entitiesBuilder.add(new VariableEntityBean(entityType, record.get(idVariable))));
      return entitiesBuilder.build();
    }
  }
}
