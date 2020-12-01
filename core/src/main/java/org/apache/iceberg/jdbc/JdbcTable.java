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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JdbcTable {
  public static final String SQL_TABLE_NAME = "iceberg_tables";
  public static final String SQL_TABLE_DDL =
          "CREATE TABLE IF NOT EXISTS " + JdbcTable.SQL_TABLE_NAME +
                  "(catalog_name VARCHAR(1255) NOT NULL," +
                  "table_namespace VARCHAR(1255) NOT NULL," +
                  "table_name VARCHAR(1255) NOT NULL," +
                  "metadata_location VARCHAR(32768)," +
                  "previous_metadata_location VARCHAR(32768)" +
                  ")";
  public static final String SQL_SELECT = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_SELECT_ALL = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ?";
  public static final String SQL_UPDATE_METADATA_LOCATION = "UPDATE " + SQL_TABLE_NAME +
          " SET metadata_location = ? , previous_metadata_location = ? " +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?";
  public static final String SQL_INSERT = "INSERT INTO " + SQL_TABLE_NAME +
          " (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) " +
          " VALUES (?,?,?,?,?)";
  public static final String SQL_UPDATE_TABLE_NAME = "UPDATE " + SQL_TABLE_NAME +
          " SET table_namespace = ? , table_name = ? " +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_DELETE = "DELETE FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";

  private static final Logger LOG = LoggerFactory.getLogger(JdbcTable.class);
  private final String catalogName;
  private Connection dbConn;


  public JdbcTable(Connection dbConn, String catalogName) {
    this.dbConn = dbConn;
    this.catalogName = catalogName;
  }

  public boolean isExists(TableIdentifier tableIdentifier) {
    try {
      return !this.get(tableIdentifier).isEmpty();
    } catch (SQLException | JsonProcessingException throwables) {
      return false;
    }
  }

  public Map<String, String> get(TableIdentifier tableIdentifier) throws SQLException, JsonProcessingException {
    Map<String, String> table = Maps.newHashMap();
    PreparedStatement sql = dbConn.prepareStatement(SQL_SELECT);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
    sql.setString(3, tableIdentifier.name());
    ResultSet rs = sql.executeQuery();
    if (rs.next()) {
      table.put("catalog_name", rs.getString("catalog_name"));
      table.put("table_namespace", rs.getString("table_namespace"));
      table.put("table_name", rs.getString("table_name"));
      table.put("metadata_location", rs.getString("metadata_location"));
      table.put("previous_metadata_location", rs.getString("previous_metadata_location"));
    }
    rs.close();
    return table;
  }

  public List<TableIdentifier> getAll(Namespace namespace) throws SQLException, JsonProcessingException {
    List<TableIdentifier> results = Lists.newArrayList();

    PreparedStatement sql = dbConn.prepareStatement(SQL_SELECT_ALL);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(namespace));
    ResultSet rs = sql.executeQuery();

    while (rs.next()) {
      final TableIdentifier table = JdbcUtil.stringToTableIdentifier(
              rs.getString("table_namespace"), rs.getString("table_name"));
      results.add(table);
    }
    rs.close();
    return results;
  }

  public int updateMetadataLocation(TableIdentifier tableIdentifier, String oldMetadataLocation,
                                    String newMetadataLocation) throws SQLException, JsonProcessingException {
    PreparedStatement sql = dbConn.prepareStatement(SQL_UPDATE_METADATA_LOCATION);
    // UPDATE
    sql.setString(1, newMetadataLocation);
    sql.setString(2, oldMetadataLocation);
    // WHERE
    sql.setString(3, catalogName);
    sql.setString(4, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
    sql.setString(5, tableIdentifier.name());
    sql.setString(6, oldMetadataLocation);
    return sql.executeUpdate();
  }

  public int save(TableIdentifier tableIdentifier, String oldMetadataLocation,
                  String newMetadataLocation) throws SQLException, JsonProcessingException {

    PreparedStatement sql = dbConn.prepareStatement(SQL_INSERT);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(tableIdentifier.namespace()));
    sql.setString(3, tableIdentifier.name());
    sql.setString(4, newMetadataLocation);
    sql.setString(5, oldMetadataLocation);
    return sql.executeUpdate();

  }

  public void updateTableName(TableIdentifier from, TableIdentifier to) throws SQLException, JsonProcessingException {

    PreparedStatement sql = dbConn.prepareStatement(SQL_UPDATE_TABLE_NAME);
    sql.setString(1, JdbcUtil.namespaceToString(to.namespace()));
    sql.setString(2, to.name());
    sql.setString(3, catalogName);
    sql.setString(4, JdbcUtil.namespaceToString(from.namespace()));
    sql.setString(5, from.name());
    int updatedRecords = sql.executeUpdate();

    if (updatedRecords > 0) {
      LOG.debug("Successfully renamed table from {} to {}!", from, to);
    } else {
      throw new NoSuchTableException("Failed to rename table! Table '%s' not found in the catalog!", from);
    }

  }

  public int delete(TableIdentifier identifier) throws SQLException, JsonProcessingException {

    PreparedStatement sql = dbConn.prepareStatement(SQL_DELETE);
    sql.setString(1, catalogName);
    sql.setString(2, JdbcUtil.namespaceToString(identifier.namespace()));
    sql.setString(3, identifier.name());
    int deletedRecords = sql.executeUpdate();

    if (deletedRecords > 0) {
      LOG.debug("Successfully dropped table {}.", identifier);
    } else {
      LOG.info("Cannot drop table {}! table not found in the Catalog.", identifier);
    }

    return deletedRecords;
  }

}
