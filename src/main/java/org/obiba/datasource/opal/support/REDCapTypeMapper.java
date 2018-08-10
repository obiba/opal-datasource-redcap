package org.obiba.datasource.opal.support;

import java.util.Map;

import org.obiba.magma.ValueType;
import org.obiba.magma.type.DateTimeType;
import org.obiba.magma.type.DateType;
import org.obiba.magma.type.DecimalType;
import org.obiba.magma.type.IntegerType;
import org.obiba.magma.type.TextType;

public abstract class REDCapTypeMapper {

  private static final String VALIDATION_TYPE_DATE_PREFIX = "date";
  private static final String VALIDATION_TYPE_DATETIME_PREFIX = VALIDATION_TYPE_DATE_PREFIX + "time";
  private static final String VALIDATION_TYPE_INTEGER = "integer";
  private static final String VALIDATION_TYPE_NUMBER = "number";

  public static final String METADATA_FIELD_TYPE = "field_type";
  public static final String METADATA_TEXT_VALIDATION_TYPE = "text_validation_type_or_show_slider_number";

  public static ValueType getType(Map<String, String> variableMetadata) {
    if (variableMetadata == null) return TextType.get();
    return getType(variableMetadata.get(METADATA_TEXT_VALIDATION_TYPE));
  }

  private static ValueType getType(String validationType) {

    if (validationType != null) {
      if (validationType.contains(VALIDATION_TYPE_DATE_PREFIX)) {
        if (validationType.contains(VALIDATION_TYPE_DATETIME_PREFIX)) return DateTimeType.get();
        else return DateType.get();
      } else if (validationType.equals(VALIDATION_TYPE_INTEGER)) {
        return IntegerType.get();
      } else if (validationType.equals(VALIDATION_TYPE_NUMBER)) {
        return DecimalType.get();
      }
    }

    return TextType.get();
  }
}