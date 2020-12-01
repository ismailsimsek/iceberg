/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.JsonUtil;

public final class JdbcUtil {

  private JdbcUtil() {
  }

  public static Namespace stringToNamespace(String string) throws JsonProcessingException {
    if (string == null) {
      return null;
    }
    TypeReference<String[]> typeRef = new TypeReference<String[]>() {
    };
    String[] levels = JsonUtil.mapper().readValue(string, typeRef);
    return Namespace.of(levels);
  }

  public static String namespaceToString(Namespace namespace) throws JsonProcessingException {
    return JsonUtil.mapper().writer().writeValueAsString(namespace.levels());
  }

  public static Map<String, String> stringToNamespaceMetadata(String namespaceMetadata) throws JsonProcessingException {
    if (namespaceMetadata == null) {
      return null;
    }
    TypeReference<ConcurrentHashMap<String, String>> typeRef = new TypeReference<ConcurrentHashMap<String, String>>() {
    };
    return JsonUtil.mapper().readValue(namespaceMetadata, typeRef);
  }

  public static String namespaceMetadataToString(Map<String, String> metadata) throws JsonProcessingException {
    if (metadata == null) {
      return JsonUtil.mapper().writer().writeValueAsString(ImmutableMap.of());
    }
    return JsonUtil.mapper().writer().writeValueAsString(metadata);
  }

  public static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName)
          throws JsonProcessingException {
    return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
  }

}
