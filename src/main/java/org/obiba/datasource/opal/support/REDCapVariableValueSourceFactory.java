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

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import org.obiba.datasource.opal.REDCapVariableValueSource;
import org.obiba.magma.Attribute;
import org.obiba.magma.Category;
import org.obiba.magma.Variable;
import org.obiba.magma.VariableValueSource;
import org.obiba.magma.VariableValueSourceFactory;
import org.obiba.magma.type.TextType;

public class REDCapVariableValueSourceFactory implements VariableValueSourceFactory {

  private final String entityType;

  private final Map<String, Map<String, String>> metadata;

  public REDCapVariableValueSourceFactory(String entityType, Map<String, Map<String, String>> metadata) {

    this.entityType = entityType;
    this.metadata = metadata;
  }

  @Override
  public Set<VariableValueSource> createSources() {
    Set<VariableValueSource> sources = Sets.newLinkedHashSet();
    int index = 0;

    for(Map<String,String> value : metadata.values()) {
      sources.add(new REDCapVariableValueSource(createVariable(value, index++)));
    }

    return sources;
  }

  private Variable createVariable(Map<String, String> variableMetadata, int index) {
    Variable.Builder builder = Variable.Builder
        .newVariable(variableMetadata.get("field_name"), TextType.get(), entityType);
    builder.index(index);
    addAttribute(builder, null, "label", variableMetadata.get("field_label"));
    addAttributes(builder, variableMetadata);
    builder.type(REDCapTypeMapper.getType(variableMetadata));
    addCategories(builder, variableMetadata);
    return builder.build();
  }

  private void addCategories(Variable.Builder builder, Map<String, String> variableMetadata) {
    String select_choices = variableMetadata.get("select_choices_or_calculations");
    if(!Strings.isNullOrEmpty(select_choices) && hasCategories(variableMetadata)) {
      Stream.of(select_choices.split("\\|")).forEach(parts -> {
        String[] catParts = parts.split("\\s*,\\s*", 2);
        Category.Builder catBuilder = Category.Builder.newCategory(catParts[0].trim());
        if (catParts.length>1)
          catBuilder.addAttribute(buildAttribute(null, "label", catParts[1].trim()));
        builder.addCategory(catBuilder.build());
      });
    }
  }

  private boolean hasCategories(Map<String, String> variableMetadata) {
    switch(variableMetadata.get("field_type")) {
      case "checkbox":
      case "radio":
      case "dropdown":
        return true;
    }

    return false;
  }

  private void addAttributes(Variable.Builder builder, Map<String, String> variableMetadata) {
    variableMetadata.forEach((key, value) -> addAttribute(builder, "REDCap", key, value));
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
