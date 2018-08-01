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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

public class REDCapClient {

  public static final String DEFAULT_FORMAT = "json";

  private final String url;

  private final String token;

  private String format = DEFAULT_FORMAT;

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

  public REDCapClient withFormat(String value) {
    format = value;
    return this;
  }

  public Set<String> getInstruments() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "instrument"));
    return post(params).stream()
        .map(instrument -> instrument.get("instrument_name"))
        .collect(Collectors.toSet());
  }

  public Map<String, Map<String, String>> getMetadata() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "metadata"));
    return post(params).stream().collect(Collectors.toMap(md -> md.get("field_name"), md -> md));
  }

  public List<Map<String, String>> getRecords() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "record"));
    return post(params);
  }

  public List<Map<String, String>> getProjectInfo() throws IOException {
    List<NameValuePair> params = new ArrayList<>();
    params.add(new BasicNameValuePair("content", "project"));
    return post(params);
  }

  private List<Map<String, String>> post(List<NameValuePair> params) throws IOException {
    HttpPost httpPost = new HttpPost(url);
    params.add(new BasicNameValuePair("token", token));
    params.add(new BasicNameValuePair("format", format));
    httpPost.setEntity(new UrlEncodedFormEntity(params));

    CloseableHttpResponse response = client.execute(httpPost);
    StatusLine statusLine = response.getStatusLine();

    if (statusLine.getStatusCode() == 200) {
      try(InputStream inputStream = response.getEntity().getContent()) {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(inputStream, mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
      } catch(Exception e) {
        throw new REDCapDatasourceParsingException(e.getMessage(), "", null);
      }
    }

    throw new REDCapDatasourceParsingException(statusLine.getReasonPhrase() + statusLine.getStatusCode(), "", null);
  }
}
