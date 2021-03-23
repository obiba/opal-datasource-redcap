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

import javax.annotation.Nullable;

import org.obiba.magma.support.DatasourceParsingException;

public class REDCapDatasourceParsingException extends DatasourceParsingException {
  private static final long serialVersionUID = 6146935409095135852L;

  public REDCapDatasourceParsingException(String message, String messageKey, Object... parameters) {
    super(message, messageKey, parameters);
  }
}
