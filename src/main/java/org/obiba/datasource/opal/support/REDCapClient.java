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

import org.apache.http.NameValuePair;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Strings;

public class REDCapClient {

  private static final String DEFAULT_FORMAT = "xml";

  private final String url;

  private final String token;

  private String format = DEFAULT_FORMAT;

  private String returnFormat = "json";

  private CloseableHttpClient client = null;

  public REDCapClient(String url, String token) {
    this.url = url;
    this.token = token;
  }

  public void connect() throws IOException {
    close();
    client = HttpClients.createDefault();
  }

  public void close() throws IOException {
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
  public Set<String> getInstruments() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "instrument"));
    return post(params).stream()
        .map(instrument -> instrument.get("instrument_name"))
        .collect(Collectors.toSet());
  }

  public Set<String> getIdentifiers(String identifierVariable) throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "record"));
    params.add(new BasicNameValuePair("fields[0]", "identifierVariable"));
    return post(params).stream().map(result -> result.get(identifierVariable)).collect(Collectors.toSet());
  }

  /**
   * Returns the variable metadata in its original order
   *
   * @return
   * @throws IOException
   */
  public Map<String, Map<String, String>> getMetadata() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "metadata"));
    List<Map<String, String>> result = post(params);
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
  public List<Map<String, String>> getRecords(List<String> recordIds) throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "record"));

    if (recordIds != null) {
      recordIds.forEach(recordId -> params.add(new BasicNameValuePair("records[]", recordId)));
    }

    return post(params);
  }

  public List<Map<String, String>> getAllRecords() throws IOException {
    return getRecords(null);
  }

  public List<Map<String, String>> getProjectInfo() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "project"));
    return post(params);
  }

  private List<Map<String, String>> post(List<NameValuePair> params) throws IOException {
    return post(params, DEFAULT_FORMAT);
  }

  /**
   * Sends request to REDCap API server
   *
   * @param params
   * @param requiredFormat
   * @return
   * @throws IOException
   */
  private List<Map<String, String>> post(List<NameValuePair> params, String requiredFormat) throws IOException {
    HttpPost httpPost = new HttpPost(url);
    String theFormat = Strings.isNullOrEmpty(requiredFormat) ? format : requiredFormat;

    params.add(new BasicNameValuePair("token", token));
    params.add(new BasicNameValuePair("format", theFormat));
    params.add(new BasicNameValuePair("returnFormat", returnFormat));
    httpPost.setEntity(new UrlEncodedFormEntity(params));

    CloseableHttpResponse response = client.execute(httpPost);
    StatusLine statusLine = response.getStatusLine();

    if (statusLine.getStatusCode() == 200) {
      try(InputStream inputStream = response.getEntity().getContent()) {
        ObjectMapper mapper = getObjectMapperForFormat(theFormat);
        return mapper.readValue(inputStream, mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
      } catch(Exception e) {
        throw new REDCapDatasourceParsingException(e.getMessage(), "", new Object[] { null });
      }
    }

    throw new REDCapDatasourceParsingException(statusLine.getReasonPhrase() + statusLine.getStatusCode(), "", new Object[] { null });
  }

  private ObjectMapper getObjectMapperForFormat(String targetFormat) {
    if ("xml".equals(targetFormat)) return new XmlMapper();
    return new ObjectMapper();
  }
}
