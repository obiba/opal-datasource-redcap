package org.obiba.datasource.opal.support;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.obiba.magma.type.DateTimeType;
import org.obiba.magma.type.DateType;
import org.obiba.magma.type.DecimalType;
import org.obiba.magma.type.IntegerType;
import org.obiba.magma.type.TextType;

public class REDCapTypeMapperTest {

  private Map<String, String> metadataMapWithOneEntry(String key, String value) {
    return new HashMap<String, String>() {
      private static final long serialVersionUID = 1L;

      {
        put(key, value);
      }
    };
  }

  private Map<String, String> metadataMapWithOneTextValidationTypeEntry(String value) {
    return metadataMapWithOneEntry(REDCapTypeMapper.METADATA_TEXT_VALIDATION_TYPE, value);
  }

  @Test
  public void nullMetadata() {
    assertThat(REDCapTypeMapper.getType(null), instanceOf(TextType.class));
  }

  @Test
  public void emptyMetadata() {
    Map<String, String> metadataMap = new HashMap<>();
    assertThat(REDCapTypeMapper.getType(metadataMap), instanceOf(TextType.class));
  }

  @Test
  public void withoutvalidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneEntry(REDCapTypeMapper.METADATA_FIELD_TYPE, "value")),
        instanceOf(TextType.class));
  }

  @Test
  public void invalidValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("value")),
        instanceOf(TextType.class));
  }

  @Test
  public void dateValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("date")), instanceOf(DateType.class));
  }

  @Test
  public void dateFullCapsValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("DATE")), instanceOf(TextType.class));
  }

  @Test
  public void dateWeirdValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("dAtE")), instanceOf(TextType.class));
  }

  @Test
  public void dateishValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("dateish")),
        instanceOf(DateType.class));
  }

  @Test
  public void datetimeValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("datetime")),
        instanceOf(DateTimeType.class));
  }

  @Test
  public void datetimeFullCapsValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("DATETIME")),
        instanceOf(TextType.class));
  }

  @Test
  public void datetimeWeirdValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("dAteTiMe")),
        instanceOf(TextType.class));
  }

  @Test
  public void datetimeishValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("datetimeish")),
        instanceOf(DateTimeType.class));
  }

  @Test
  public void integerValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("integer")),
        instanceOf(IntegerType.class));
  }

  @Test
  public void notExactlyIntegerValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("intege")),
        instanceOf(TextType.class));
  }

  @Test
  public void numberValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("number")),
        instanceOf(DecimalType.class));
  }

  @Test
  public void notExactlyNumberValidationType() {
    assertThat(REDCapTypeMapper.getType(metadataMapWithOneTextValidationTypeEntry("numb")), instanceOf(TextType.class));
  }
}