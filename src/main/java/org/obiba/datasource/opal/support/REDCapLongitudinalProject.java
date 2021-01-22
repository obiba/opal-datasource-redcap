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

import com.google.common.collect.Maps;

public class REDCapLongitudinalProject extends AbstractREDCapProject {

  private Map<String, Map<String, String>> tableToFormEventMap = Maps.newHashMap();

  REDCapLongitudinalProject(REDCapClient client, Map<String, String> projectInfo, String identifierVariable) {
    super(client, projectInfo, identifierVariable);
  }

  @Override
  public Set<String> getTables() throws IOException {
    if(tableToFormEventMap.isEmpty()) {
      getTablesInternal();
    }

    return tableToFormEventMap.keySet();
  }

  @Override
  public List<Map<String, String>> getRecords(List<String> recordIds, String table) throws IOException {
    Map<String, String> formEventMap = tableToFormEventMap.get(table);
    return client.getRecords(recordIds, Arrays.asList(identifierVariable), Arrays.asList(formEventMap.get("form")),
        Arrays.asList(formEventMap.get("unique_event_name")));
  }

  @Override
  public Map<String, Map<String, String>> getMetadata(String table) throws IOException {
    Map<String, String> formEventMap = tableToFormEventMap.get(table);
    return client.getMetadata(Arrays.asList(formEventMap.get("form")), Arrays.asList(identifierVariable));
  }

  @Override
  public Set<String> getIdentifiers(String table) throws IOException {
    Map<String, String> formEventMap = tableToFormEventMap.get(table);
    return client.getRecords(null, Arrays.asList(identifierVariable), Arrays.asList(formEventMap.get("form")),
            Arrays.asList(formEventMap.get("unique_event_name")))
        .stream()
        .map(result -> result.get(identifierVariable))
        .collect(Collectors.toSet());
  }

  private void getTablesInternal() throws IOException {
    boolean hasRepeating = "1".equals(projectInfo.get("has_repeating_instruments_or_events"));
    List<Map<String, String>> formEventMapping = client.getFormEventMapping();

    if(hasRepeating) {
      List<Map<String, String>> repeatingFormEvents = client.getRepeatingFormEvents();
      List<String> repeatingEventNames = repeatingFormEvents.stream().map(repeating -> repeating.get("event_name"))
          .collect(Collectors.toList());

      formEventMapping = formEventMapping.stream()
          .filter(mapping -> !repeatingEventNames.contains(mapping.get("unique_event_name")))
          .collect(Collectors.toList());
    }

    formEventMapping.stream().map(mapping -> {
      String tableName = mapping.get("form") + "_" + mapping.get("unique_event_name");
      tableToFormEventMap.put(tableName, mapping);
      return tableName;
    }).collect(Collectors.toSet());
  }
}
