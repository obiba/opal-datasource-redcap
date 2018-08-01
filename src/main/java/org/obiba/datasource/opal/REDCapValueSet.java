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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.obiba.magma.Value;
import org.obiba.magma.ValueTable;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableEntity;
import org.obiba.magma.support.ValueSetBean;
import org.obiba.magma.type.TextType;

public class REDCapValueSet extends ValueSetBean {

  private final Map<String, Value> rows = new HashMap<>();

  public REDCapValueSet(@NotNull ValueTable table, @NotNull VariableEntity entity, @NotNull String idVariable,
      List<Map<String, String>> records) {
    super(table, entity);
    loadVariables(records, idVariable);
  }

  @NotNull
  @Override
  public REDCapValueTable getValueTable() {
    return (REDCapValueTable) super.getValueTable();
  }

  Value getValue(Variable variable) {
    Value value = rows.get(variable.getName());
    return value == null ? TextType.get().valueOf(String.format("No Value for %s", variable.getName())) : value;
  }

  private void loadVariables(List<Map<String, String>> records, String idVariable) {
    records.stream()
        .filter(record -> record.get(idVariable).equals(getVariableEntity().getIdentifier()))
        .forEach(record ->
            record.entrySet().stream()
              .filter(entry -> !entry.getKey().equals(idVariable))
              .forEach(entry -> rows.put(entry.getKey(), TextType.get().valueOf(entry.getValue())))
        );
  }
}
