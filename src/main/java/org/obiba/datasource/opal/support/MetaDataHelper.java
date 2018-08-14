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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

final class MetaDataHelper {

  public static void splitChoicesMetaData(List<Map<String, String>> list) {
    Map<Integer, Map<String, String>> indexToMetadata = IntStream.range(0, list.size())
        .filter(index -> "checkbox".equals(list.get(index).get("field_type"))).boxed()
        .collect(Collectors.toMap(index -> index, index -> list.get(index)));

    indexToMetadata.entrySet().forEach(entry -> createMetadataPerChoice(list, entry.getKey(), entry.getValue()));
  }

  private static void createMetadataPerChoice(List<Map<String, String>> list, int index, Map<String, String> metadata) {
    String select_choices_or_calculations = metadata.get("select_choices_or_calculations");
    Pattern compile = Pattern.compile("\\|\\s*([^,]*)", Pattern.DOTALL);
    Matcher m = compile.matcher("|" + select_choices_or_calculations);
    List<Map<String, String>> metadataPerChoice = Lists.newArrayList();
    String fieldName = metadata.get("field_name");

    if (m.find()) {
      metadata.put("field_name", fieldName + "__" + m.group(1));
      metadata.put("field_type", "text");
      metadata.put("select_choices_or_calculations", "");
    }

    while (m.find()) {
      Map<String, String> clone = Maps.newLinkedHashMap();
      clone.putAll(metadata);
      clone.put("field_name", fieldName + "__" + m.group(1));
      clone.put("field_type", "text");
      clone.put("select_choices_or_calculations", "");
      metadataPerChoice.add(clone);
      list.add(index + metadataPerChoice.size(), clone);
    }
  }

}