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

import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.obiba.magma.AbstractVariableValueSource;
import org.obiba.magma.Value;
import org.obiba.magma.ValueSet;
import org.obiba.magma.ValueType;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableValueSource;
import org.obiba.magma.VectorSource;
import org.obiba.magma.VectorSourceNotSupportedException;

public class REDCapVariableValueSource extends AbstractVariableValueSource implements VariableValueSource {

  private final Variable variable;

  private final List<Map<String, String>> records;

  public REDCapVariableValueSource(Variable variable, List<Map<String, String>> records) {
    this.variable = variable;
    this.records = records;
  }

  @NotNull
  @Override
  public Variable getVariable() {
    return variable;
  }

  @NotNull
  @Override
  public ValueType getValueType() {
    return variable.getValueType();
  }

  @NotNull
  @Override
  public Value getValue(ValueSet valueSet) {
    REDCapValueSet redcapValueSet = (REDCapValueSet) valueSet;
    return redcapValueSet.getValue(variable);
  }


  @Override
  public boolean supportVectorSource() {
    return false;
  }

  @NotNull
  @Override
  public VectorSource asVectorSource() throws VectorSourceNotSupportedException {
    return null;
  }

}
