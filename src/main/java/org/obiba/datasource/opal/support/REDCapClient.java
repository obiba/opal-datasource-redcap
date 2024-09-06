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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class REDCapClient {

  private static final Logger log = getLogger(REDCapClient.class);

  private static final String XML_FORMAT = "xml";

  private static final String JSON_FORMAT = "json";

  private final String url;

  private final String token;

  private String format = XML_FORMAT;

  private String returnFormat = JSON_FORMAT;

  private CloseableHttpClient client = null;

  REDCapClient(String url, String token) {
    this.url = url;
    this.token = token;
  }

  void connect() throws IOException {
    close();
    client = HttpClients.createDefault();
  }

  void close() throws IOException {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  /**
   * Format used for export (meta-data, records, etc)
   *
   * @param value
   * @return
   */
  public REDCapClient withFormat(String value) {
    format = value;
    return this;
  }

  /**
   * Format used for an error response
   * @param value
   * @return
   */
  public REDCapClient withReturnFormat(String value) {
    returnFormat = value;
    return this;
  }

  /**
   * Returns list of project instruments
   *
   * @return
   * @throws IOException
   */
  Set<String> getInstruments() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "instrument"));
    return postAndGetAsList(params, XML_FORMAT).stream()
        .map(instrument -> instrument.get("instrument_name"))
        .collect(Collectors.toSet());
  }

  /**
   * Returns the variable metadata in its original order
   *
   * @return
   * @throws IOException
   */
  Map<String, Map<String, String>> getMetadata(List<String> forms, List<String> fields) throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "metadata"));

    if (forms != null) {
      forms.forEach(form -> params.add(new BasicNameValuePair("forms[]", form)));
    }

    if (fields != null) {
      fields.forEach(field -> params.add(new BasicNameValuePair("fields[]", field)));
    }

    List<Map<String, String>> result = postAndGetAsList(params, XML_FORMAT);
      MetaDataHelper.splitChoicesMetaData(result);

    LinkedHashMap<String, Map<String, String>> metaDataMap = new LinkedHashMap<>();
    result.forEach(data -> metaDataMap.put(data.get("field_name"), data));
    return metaDataMap;
  }

  /**
   * Returns the records for the given IDs
   *
   * @param recordIds
   * @return
   * @throws IOException
   */
  List<Map<String, String>> getRecords(
      List<String> recordIds,
      List<String> fields,
      List<String> forms,
      List<String> events) throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "record"));

    if (recordIds != null) {
      recordIds.forEach(recordId -> params.add(new BasicNameValuePair("records[]", recordId)));
    }

    if (fields != null) {
      fields.forEach(field -> params.add(new BasicNameValuePair("fields[]", field)));
    }

    if (forms != null) {
      forms.forEach(form -> params.add(new BasicNameValuePair("forms[]", form)));
    }

    if (events != null) {
      events.forEach(event -> params.add(new BasicNameValuePair("events[]", event)));
    }

    return postAndGetAsList(params, JSON_FORMAT);
  }

  Map<String, String> getProjectInfo() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "project"));
    return postAndGetAsMap(params, XML_FORMAT);
  }

  List<Map<String, String>> getFormEventMapping() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "formEventMapping"));
    return postAndGetAsList(params, XML_FORMAT);
  }

  List<Map<String, String>> getRepeatingFormEvents() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "repeatingFormsEvents"));
    return postAndGetAsList(params, XML_FORMAT);
  }

  private List<Map<String, String>> postAndGetAsList(List<NameValuePair> params, String requiredFormat)
      throws IOException {
    String theFormat = Strings.isNullOrEmpty(requiredFormat) ? format : requiredFormat;
    ObjectMapper mapper = getObjectMapperForFormat(theFormat);
    JavaType javaType = mapper.getTypeFactory().constructCollectionType(List.class, Map.class);
    return post(params, theFormat, mapper, javaType);
  }

  private Map<String, String> postAndGetAsMap(List<NameValuePair> params, String requiredFormat)
      throws IOException {
    String theFormat = Strings.isNullOrEmpty(requiredFormat) ? format : requiredFormat;
    ObjectMapper mapper = getObjectMapperForFormat(theFormat);
    JavaType javaType = mapper.getTypeFactory().constructMapType(Map.class, String.class, String.class);
    return post(params, theFormat, mapper, javaType);
  }

  /**
   * Sends request to REDCap API server
   *
   * @param params
   * @param requiredFormat
   * @return
   * @throws IOException
   */
  private <T> T post(List<NameValuePair> params, String requiredFormat, ObjectMapper mapper, JavaType javaType) throws IOException {
    HttpPost httpPost = new HttpPost(url);

    params.add(new BasicNameValuePair("token", token));
    params.add(new BasicNameValuePair("format", requiredFormat));
    params.add(new BasicNameValuePair("returnFormat", returnFormat));
    httpPost.setEntity(new UrlEncodedFormEntity(params));

    log.debug("Sending REDCap request: {}", httpPost.toString());
    CloseableHttpResponse response = client.execute(httpPost);
    StatusLine statusLine = response.getStatusLine();
    log.debug("Sending REDCap response status: {}", statusLine);

    if (statusLine.getStatusCode() == 200) {
      try(InputStream inputStream = response.getEntity().getContent()) {
        return mapper.readValue(inputStream, javaType);
      } catch(Exception e) {
        throw new REDCapDatasourceParsingException(e.getMessage(), "");
      }
    }

    throw new REDCapDatasourceParsingException(statusLine.getReasonPhrase() + statusLine.getStatusCode(), "", new Object[] { null });
  }

  private ObjectMapper getObjectMapperForFormat(String targetFormat) {
    if (XML_FORMAT.equals(targetFormat)) return new XmlMapper();
    return new ObjectMapper();
  }
}
