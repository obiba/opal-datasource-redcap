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
import java.util.Map;

public class REDCapProjectFactory {

  public static AbstractREDCapProject create(String url, String token, String identifierVariable) throws IOException {
    REDCapClient client = new REDCapClient(url, token);
    client.connect();
    Map<String, String> projectInfo = client.getProjectInfo();

    if ("1".equals(projectInfo.get("is_longitudinal"))) {
      return new REDCapLongitudinalProject(client, projectInfo, identifierVariable);
    }

    return new REDCapDefaultProject(client, projectInfo, identifierVariable);
  }
}
