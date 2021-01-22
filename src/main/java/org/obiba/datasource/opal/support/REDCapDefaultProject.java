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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class REDCapDefaultProject extends AbstractREDCapProject {

  REDCapDefaultProject(REDCapClient client, Map<String, String> projectInfo, String identifierVariable) {
    super(client, projectInfo, identifierVariable);
  }

  @Override
  public Set<String> getTables() throws IOException {
    return client.getInstruments();
  }

  @Override
  public List<Map<String, String>> getRecords(List<String> recordIds, String table) throws IOException {
    return client.getRecords(recordIds, Arrays.asList(identifierVariable), Arrays.asList(table), null);
  }

  @Override
  public Map<String, Map<String, String>> getMetadata(String table) throws IOException {
    return client.getMetadata(Arrays.asList(table), Arrays.asList(identifierVariable));
  }

  @Override
  public Set<String> getIdentifiers(String table) throws IOException {
    return client.getRecords(null, Arrays.asList(identifierVariable), Arrays.asList(table), null)
        .stream()
        .map(result -> result.get(identifierVariable))
        .collect(Collectors.toSet());
  }
}
