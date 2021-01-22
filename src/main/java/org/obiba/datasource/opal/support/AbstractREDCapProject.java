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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractREDCapProject {

  protected final REDCapClient client;

  protected final Map<String, String> projectInfo;

  protected final String identifierVariable;

  AbstractREDCapProject(REDCapClient client, Map<String, String> projectInfo, String identifierVariable) {
    this.client = client;
    this.projectInfo = projectInfo;
    this.identifierVariable = identifierVariable;
  }

  public abstract Set<String> getIdentifiers(String table) throws IOException;

  public abstract Set<String> getTables() throws IOException;

  public abstract List<Map<String, String>> getRecords(List<String> recordIds, String table) throws IOException;

  public void close() throws IOException {
    client.close();
  }

  public abstract Map<String, Map<String, String>> getMetadata(String table) throws IOException;

}
