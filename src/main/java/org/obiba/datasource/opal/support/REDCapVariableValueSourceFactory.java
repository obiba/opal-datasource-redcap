/*
 * Copyright (c) 2017 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.datasource.opal.support;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.obiba.datasource.opal.REDCapVariableValueSource;
import org.obiba.magma.Attribute;
import org.obiba.magma.Category;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableValueSource;
import org.obiba.magma.VariableValueSourceFactory;
import org.obiba.magma.type.TextType;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class REDCapVariableValueSourceFactory implements VariableValueSourceFactory {

  private final String entityType;

  private final Map<String, Map<String, String>> metadata;

  private final String idVariable;

  private final List<Map<String, String>> records;

  public REDCapVariableValueSourceFactory(String entityType, Map<String, Map<String, String>> metadata,
      List<Map<String, String>> records, String idVariable) {

    this.entityType = entityType;
    this.metadata = metadata;
    this.records = records;
    this.idVariable = idVariable;
  }

  @Override
  public Set<VariableValueSource> createSources() {
    Set<VariableValueSource> sources = Sets.newLinkedHashSet();

    List<Map<String, String>> mdList = metadata.values()
        .stream()
        .filter(value -> !value.get("field_name").equals(idVariable))
        .collect(Collectors.toList());

    IntStream.range(0, mdList.size())
        .forEach(index -> sources.add(new REDCapVariableValueSource(createVariable(mdList.get(index), index), records)));

    return sources;
  }

  private Variable createVariable(Map<String, String> variableMetadata, int index) {
    Variable.Builder builder = Variable.Builder
        .newVariable(variableMetadata.get("field_name"), TextType.get(), entityType);
    builder.index(index);
    addAttribute(builder, null, "label", variableMetadata.get("field_label"));
    addAttributes(builder, variableMetadata);
    // TODO implement mapper (text_validation_type_or_show_slider_number)
    builder.type(TextType.get());
    addCategories(builder, variableMetadata);
    return builder.build();
  }

  private void addCategories(Variable.Builder builder, Map<String, String> variableMetadata) {
    String select_choices = variableMetadata.get("select_choices_or_calculations");
    if(!Strings.isNullOrEmpty(select_choices) && select_choices.contains("|")) {
      Stream.of(select_choices.split("\\|")).forEach(parts -> {
        String[] catParts = parts.split("\\s*,\\s*");
        builder.addCategory(
            Category.Builder
                .newCategory(catParts[0])
                .addAttribute(buildAttribute(null, "label", catParts[1]))
                .build());
      });
    }
  }

  private void addAttributes(Variable.Builder builder, Map<String, String> variableMetadata) {
    addAttribute(builder, "REDCap", "form name", variableMetadata.get("form_name"));
    String calculations = variableMetadata.get("select_choices_or_calculations");
    if(!Strings.isNullOrEmpty(calculations) && !calculations.contains("|")) {
      addAttribute(builder, "REDCap", "calculations", calculations);
    }
  }

  private void addAttribute(Variable.Builder builder, String namespace, String name, String value) {
    builder.addAttribute(buildAttribute(namespace, name, value));
  }

  private Attribute buildAttribute(String namespace, String name, String value) {
    Attribute.Builder builder = Attribute.Builder.newAttribute(name).withValue(value);
    if(!Strings.isNullOrEmpty(namespace)) {
      builder.withNamespace(namespace);
    }
    return builder.build();
  }
}
