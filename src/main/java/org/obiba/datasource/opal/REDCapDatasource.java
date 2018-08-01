package org.obiba.datasource.opal;

import java.util.Collections;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.obiba.magma.Value;
import org.obiba.magma.ValueTable;
import org.obiba.magma.ValueTableWriter;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableEntity;
import org.obiba.magma.support.AbstractDatasource;

public class REDCapDatasource extends AbstractDatasource {

  public REDCapDatasource(String name) {
    super(name, "REDCap");
  }

  @Override
  protected Set<String> getValueTableNames() {
    return Collections.emptySet();
  }

  @Override
  protected ValueTable initialiseValueTable(String tableName) {
    return null;
  }

  @Override
  @NotNull
  public ValueTableWriter createWriter(@NotNull String name, @NotNull String entityType) {
    return new ExampleValueTableWriter();
  }

  private static class ExampleValueTableWriter implements ValueTableWriter {

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