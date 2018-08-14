package org.obiba.datasource.opal;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.obiba.datasource.opal.support.AbstractREDCapProject;
import org.obiba.datasource.opal.support.REDCapClient;
import org.obiba.magma.Value;
import org.obiba.magma.ValueTable;
import org.obiba.magma.ValueTableWriter;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableEntity;
import org.obiba.magma.support.AbstractDatasource;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

public class REDCapDatasource extends AbstractDatasource {
  private static final Logger logger = getLogger(REDCapDatasource.class);

  private final String entityType;

  private final Map<String, REDCapValueTable> valueTablesMapOnInit = new LinkedHashMap<>();

  private final String identifierVariable;

  private final AbstractREDCapProject project;

  public REDCapDatasource(@NotNull String name, @NotNull AbstractREDCapProject project, @NotNull String entityType,
      @NotNull String identifierVariable) {
    super(name, "REDCap");
    this.project = project;
    this.entityType = entityType;
    this.identifierVariable = identifierVariable;
  }

  @Override
  protected void onInitialise() {

    try {
      project.getTables()
          .stream()
          .forEach(tabelName -> {
            if (!valueTablesMapOnInit.containsKey(tabelName)) {
              valueTablesMapOnInit.put(tabelName, new REDCapValueTable(this, tabelName, entityType, project, identifierVariable));
            }
          });
    } catch(IOException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  public void dispose() {
    super.dispose();

    try {
      project.close();
    } catch(IOException e) {
      logger.error(e.getMessage());
    }
  }

  @Override
  protected Set<String> getValueTableNames() {
    return valueTablesMapOnInit.keySet();
  }

  @Override
  protected ValueTable initialiseValueTable(String tableName) {
    return valueTablesMapOnInit.get(tableName);
  }

  @Override
  @NotNull
  public ValueTableWriter createWriter(@NotNull String name, @NotNull String entityType) {
    return new REDCapValueTableWriter();
  }

  private static class REDCapValueTableWriter implements ValueTableWriter {

    @Override
    public VariableWriter writeVariables() {
      return new VariableWriter() {

        @Override
        public void writeVariable(Variable variable) {

        }

        @Override
        public void removeVariable(Variable variable) {

        }

        @Override
        public void close() {

        }
      };
    }

    @Override
    public ValueSetWriter writeValueSet(VariableEntity entity) {
      return new ValueSetWriter() {

        @Override
        public void writeValue(Variable variable, Value value) {

        }

        @Override
        public void remove() {

        }

        @Override
        public void close() {

        }
      };
    }

    @Override
    public void close() {

    }

  }

}