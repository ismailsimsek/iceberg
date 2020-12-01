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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcNamespace {
  public static final String SQL_TABLE_NAME = "iceberg_namespaces";
  public static final String SQL_TABLE_DDL = "CREATE TABLE IF NOT EXISTS " +
          JdbcNamespace.SQL_TABLE_NAME + " ( " +
          "catalog_name VARCHAR(1255) NOT NULL," +
          "namespace VARCHAR(1255) NOT NULL," +
          "namespace_metadata VARCHAR(32768)" +
          ")";
  public static final String SQL_SELECT = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND namespace = ? ";
  public static final String SQL_SELECT_ALL = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? ";
  public static final String SQL_SELECT_CHILD = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? ";
  public static final String SQL_UPDATE = "UPDATE " + SQL_TABLE_NAME + " SET namespace_metadata = ? " +
          " WHERE catalog_name = ? AND namespace = ? ";
  public static final String SQL_INSERT = "INSERT INTO " + SQL_TABLE_NAME +
          " (catalog_name, namespace, namespace_metadata) VALUES (?,?,?)";
  public static final String SQL_DELETE = "DELETE FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND namespace = ? ";

  private static final Logger LOG = LoggerFactory.getLogger(JdbcNamespace.class);
  private final String catalogName;
  private Connection dbConn;

  public JdbcNamespace(Connection dbConn, String catalogName) {
    this.dbConn = dbConn;
    this.catalogName = catalogName;
  }

  public Map<String, String> get(Namespace namespace) throws SQLException, JsonProcessingException {
    Map<String, String> ns = Maps.newHashMap();
    PreparedStatement sql = dbConn.prepareStatement(SQL_SELECT);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(namespace));
    ResultSet rs = sql.executeQuery();
    if (rs.next()) {
      ns.put("catalog_name", rs.getString("catalog_name"));
      ns.put("namespace", rs.getString("namespace"));
      ns.put("namespace_metadata", rs.getString("namespace_metadata"));
    }
    rs.close();
    return ns;
  }

  public boolean isExists(Namespace namespace) {
    try {
      if (!this.get(namespace).isEmpty()) {
        return true;
      }

      boolean isSubNs = false;
//      // it might be parent of another namespace
//      List<Namespace> nsList = getAll();
//      for (Namespace subNs : nsList) {
//        if (isParentOf(namespace, subNs)) {
//          isSubNs = true;
//          break;
//        }
//      }
      return isSubNs;
    } catch (SQLException | JsonProcessingException throwables) {
      return false;
    }
  }

  private Namespace getParent(Namespace namespace) {
    if (namespace.levels().length == 0) {
      return null;
    }
    Stream<String> parentNodes = Arrays.stream(namespace.levels()).limit(namespace.levels().length - 1);
    Namespace parentNs = Namespace.of(parentNodes.toArray(String[]::new));
    return parentNs;
  }


//  private Namespace getChildNamespace(Namespace parent, Namespace subNamespace) {
//    // parent Namespace should have less levels than its children
//    if (parent.levels().length >= subNamespace.levels().length) {
//      return null;
//    }
//
//    Stream<String> childNodes = Arrays.stream(subNamespace.levels()).limit(parent.levels().length + 1);
//    Namespace childNs = Namespace.of(childNodes.toArray(String[]::new));
//    Stream<String> childParentNodes = Arrays.stream(subNamespace.levels()).limit(parent.levels().length);
//    Namespace childParent = Namespace.of(childParentNodes.toArray(String[]::new));
//    if (childParent.equals(parent)) {
//      return childNs;
//    } else {
//      return null;
//    }
//  }

  public List<Namespace> getAll() throws SQLException, JsonProcessingException {
    List<Namespace> namespaces = Lists.newArrayList();
    PreparedStatement sql = dbConn.prepareStatement(SQL_SELECT_ALL);
    sql.setString(1, catalogName);
    ResultSet rs = sql.executeQuery();
    while (rs.next()) {
      namespaces.add(JdbcUtil.stringToNamespace(rs.getString("namespace")));
    }
    rs.close();
    return namespaces;
  }

  public List<Namespace> getChildren(Namespace namespace) throws SQLException, JsonProcessingException {
    List<Namespace> namespaces = Lists.newArrayList();
    List<Map<String, String>> results = Lists.newArrayList();
    PreparedStatement sql = dbConn.prepareStatement(SQL_SELECT_CHILD);
    sql.setString(1, catalogName);
    ResultSet rs = sql.executeQuery();
    while (rs.next()) {
      Map<String, String> ns = Maps.newHashMap();
      ns.put("catalog_name", rs.getString("catalog_name"));
      ns.put("namespace", rs.getString("namespace"));
      ns.put("namespace_metadata", rs.getString("namespace_metadata"));
      results.add(ns);
    }
    rs.close();

    // when given namespace is empty Only retunr root namespaces
    if (namespace.isEmpty()) {
      for (Map<String, String> nsResult : results) {
        if (JdbcUtil.stringToNamespace(nsResult.get("namespace")).levels().length == 1) { // if its root
          namespaces.add(JdbcUtil.stringToNamespace(nsResult.get("namespace")));
        }
      }
    } else {
      // Only return child (-1 level) namespaces of given namespace!
      for (Map<String, String> nsResult : results) {
        Namespace parentNs = getParent(JdbcUtil.stringToNamespace(nsResult.get("namespace")));
        if (parentNs != null && parentNs.equals(namespace)) {
          namespaces.add(JdbcUtil.stringToNamespace(nsResult.get("namespace")));
        }
      }
    }

    return namespaces;
  }

//  public List<Namespace> getAllChildsV2(Namespace namespace) throws SQLException, JsonProcessingException {
//    Map<String, Namespace> namespaces = Maps.newHashMap();
//    List<JdbcNamespace> results = queryRunner.query("SELECT * FROM " + NAMESPACES_TABLE_NAME + " WHERE " +
//                    "catalog_name = ? ",
//            new BeanListHandler<>(JdbcNamespace.class), catalog_name);
//
//    // collect root namespaces
//    if (namespace.isEmpty()) {
//      for (JdbcNamespace nsResult : results) {
//        if (!nsResult.toNamespace().isEmpty()) {
//          String rootNs = nsResult.toNamespace().level(0);
//          namespaces.putIfAbsent(rootNs, Namespace.of(rootNs));
//        }
//      }
//    } else {
//
//      for (JdbcNamespace nsResult : results) {
//        Namespace child = getChildNamespace(namespace, nsResult.toNamespace());
//        if (child != null) {
//          namespaces.putIfAbsent(nsResult.getNamespace(), child);
//        }
//      }
//    }
//
//    return Lists.newArrayList(namespaces.values());
//  }

  public int update(Namespace namespace, Map<String, String> metadata) throws
          SQLException, JsonProcessingException {
    PreparedStatement sql = dbConn.prepareStatement(SQL_UPDATE);
    sql.setString(1, JdbcUtil.namespaceMetadataToString(metadata));
    sql.setString(2, catalogName);
    sql.setString(3, JdbcUtil.namespaceToString(namespace));
    return sql.executeUpdate();
  }

  public int save(Namespace namespace, Map<String, String> metadata) throws SQLException, JsonProcessingException {
    // it might be better option to create namespace.levels separately! ex: db.t1.t2.t3 as 4 separate namespace.
    // {db, db.t1, db.t1.t2, db.t1.t2.t3}
    Namespace parentNs = getParent(namespace);
    if (parentNs != null && !isExists(parentNs)) {
      this.save(parentNs, null);
    }
    PreparedStatement sql = dbConn.prepareStatement(SQL_INSERT);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(namespace));
    sql.setString(3, JdbcUtil.namespaceMetadataToString(metadata));
    return sql.executeUpdate();

  }

  public int delete(Namespace namespace) throws SQLException, JsonProcessingException {

    PreparedStatement sql = dbConn.prepareStatement(SQL_DELETE);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(namespace));
    int deletedRecords = sql.executeUpdate();

    if (deletedRecords > 0) {
      LOG.debug("Successfully dropped namespace {}", namespace);
    } else {
      LOG.info("Cannot drop namespace {}! namespace not found in the catalog.", namespace);
    }
    return deletedRecords;
  }

}
