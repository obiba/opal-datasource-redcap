package org.obiba.datasource.opal;

import com.google.common.base.Strings;
import org.obiba.magma.support.VariableEntityBean;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class REDCapVariableEntityBean extends VariableEntityBean {
  private final String suffix;
  private final String prefix;

  public REDCapVariableEntityBean(@NotNull String entityType,
                                  @NotNull String entityIdentifier,
                                  @Nullable String prefix, @Nullable String suffix) {
    super(entityType, entityIdentifier);
    this.prefix = Strings.nullToEmpty(prefix);
    this.suffix = Strings.nullToEmpty(suffix);
  }

  @Override
  public String getIdentifier() {
    return String.format("%s%s%s", prefix, super.getIdentifier(), suffix);
  }

  String getOriginalIdentifier() {
    return super.getIdentifier();
  }
}
