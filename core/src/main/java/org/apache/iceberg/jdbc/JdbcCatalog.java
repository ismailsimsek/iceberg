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
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCatalog extends BaseMetastoreCatalog implements Configurable, SupportsNamespaces, Closeable {

  public static final String JDBC_CATALOG_DEFAULT_NAME = "jdbc";

  public static final String JDBC_PARAM_PREFIX = "jdbccatalog.property.";
  private static final Joiner SLASH = Joiner.on("/");
  private static final String TABLE_METADATA_FILE_EXTENSION = ".metadata.json";
  private static final PathFilter TABLE_FILTER = path -> path.getName().endsWith(TABLE_METADATA_FILE_EXTENSION);


  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private FileIO fileIO;
  private FileSystem fs;
  private String catalogName = JDBC_CATALOG_DEFAULT_NAME;
  private String warehouseLocation;
  private Configuration hadoopConf;
  private Connection dbConn;

  public JdbcCatalog() {
  }

  private void initializeCatalog(Map<String, String> properties) throws UncheckedIOException {
    try {

      LOG.debug("Connecting to Jdbc database {}.", properties.get(CatalogProperties.HIVE_URI));
      Properties connectionProps = new Properties();
      for (Map.Entry<String, String> prop : properties.entrySet()) {
        if (prop.getKey().startsWith(JDBC_PARAM_PREFIX)) {
          connectionProps.put(prop.getKey().substring(JDBC_PARAM_PREFIX.length()), prop.getValue());
        }
      }

      dbConn = DriverManager.getConnection(properties.get(CatalogProperties.HIVE_URI), connectionProps);
      // create tables if not exits
      ResultSet tables = dbConn.getMetaData().getTables(null, null, JdbcNamespace.SQL_TABLE_NAME, null);
      if (!tables.next()) {
        dbConn.prepareStatement(JdbcNamespace.SQL_TABLE_DDL).execute();
        LOG.debug("Created database table {} to store iceberg catalog!", JdbcNamespace.SQL_TABLE_DDL);
      }
      tables.close();
      tables = dbConn.getMetaData().getTables(null, null, JdbcNamespace.SQL_TABLE_NAME, null);
      if (!tables.next()) {
        dbConn.prepareStatement(JdbcTable.SQL_TABLE_DDL).execute();
        LOG.debug("Created database table {} to store iceberg catalog!", JdbcTable.SQL_TABLE_DDL);
      }
      tables.close();

    } catch (SQLException throwables) {
      throw new UncheckedIOException("Failed to initialize Jdbc Catalog!", new IOException(throwables));
    }
  }

  @Override
  public void initialize(String name, Map<String, String> properties) throws UncheckedIOException {
    Preconditions.checkArgument(
            !properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "").equals(""),
            "no location provided for warehouse!");
    Preconditions.checkArgument(!properties.getOrDefault(CatalogProperties.HIVE_URI, "").equals(""),
            "no connection url provided for jdbc catalog!");

    this.catalogName = name;
    this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION).replaceAll("/*$", "");
    this.fs = Util.getFs(new Path(warehouseLocation), hadoopConf);

    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl == null ?
            new HadoopFileIO(hadoopConf) :
            CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);

    initializeCatalog(properties);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(dbConn, fileIO, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String tableName = tableIdentifier.name();
    StringBuilder sb = new StringBuilder();

    sb.append(warehouseLocation).append('/');
    for (String level : tableIdentifier.namespace().levels()) {
      sb.append(level).append('/');
    }
    sb.append(tableName);

    return sb.toString();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata = ops.current();
    try {
      new JdbcTable(dbConn, catalogName).delete(identifier);
      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
        fs.delete(new Path(lastMetadata.location()), true /* recursive */);
        LOG.info("Table {} data purged!", identifier);
      }
      return true;
    } catch (SQLException | IOException throwables) {
      LOG.error("Cannot complete drop table operation for {} due to unexpected exception {}!", identifier,
              throwables.getMessage(), throwables);
      throw new UncheckedIOException("Failed to drop table!", new IOException(throwables));
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {

    JdbcNamespace nsDao = new JdbcNamespace(dbConn, catalogName);
    if (!nsDao.isExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace %s does not exist!", namespace);
    }
    JdbcTable tableDao = new JdbcTable(dbConn, catalogName);
    try {
      return tableDao.getAll(namespace);
    } catch (SQLException | JsonProcessingException throwables) {
      LOG.error("Failed to list tables!", throwables);
      return null;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    JdbcTable tableDao = new JdbcTable(dbConn, catalogName);
    try {
      if (tableDao.get(from).isEmpty()) {
        throw new NoSuchTableException("Failed to rename table! Table '%s' not found in the catalog!", from);
      }
      JdbcNamespace nsDao = new JdbcNamespace(dbConn, catalogName);
      if (!nsDao.isExists(to.namespace())) {
        nsDao.save(to.namespace(), null);
      }
      tableDao.updateTableName(from, to);
      if (!tableDao.isExists(to)) {
        throw new NoSuchTableException("Rename Operation Failed! Table '%s' not found after the rename!", to);
      }
    } catch (SQLException | JsonProcessingException throwables) {
      LOG.debug("Failed to rename table from {} to {}", from, to, throwables);
      throw new UncheckedIOException("Failed to rename table!", new IOException(throwables));
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Configuration getConf() {
    return this.hadoopConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Preconditions.checkArgument(!namespace.isEmpty(),
            "Cannot create namespace with invalid name: %s", namespace);
    if (!metadata.isEmpty()) {
      throw new UnsupportedOperationException("Cannot create namespace " + namespace + ": metadata is not supported");
    }
    JdbcNamespace nsDao = new JdbcNamespace(dbConn, catalogName);
    if (nsDao.isExists(namespace)) {
      throw new AlreadyExistsException("Cannot create namespace %s because it already exists in the catalog",
              namespace);
    }

    Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));

    if (isNamespace(nsPath)) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    try {
      fs.mkdirs(nsPath);
      nsDao.save(namespace, metadata);
      LOG.info("Created new namespace: {}", namespace);
    } catch (SQLException | IOException throwables) {
      throw new UncheckedIOException("Failed to create namespace", new IOException(throwables));
    }
  }

  private boolean isNamespace(Path path) {
    Path metadataPath = new Path(path, "metadata");
    try {
      return fs.isDirectory(path) && !(fs.exists(metadataPath) && fs.isDirectory(metadataPath) &&
              (fs.listStatus(metadataPath, TABLE_FILTER).length >= 1));

    } catch (IOException throwables) {
      throw new UncheckedIOException("Failed to list namespace info ", throwables);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    JdbcNamespace dao = new JdbcNamespace(dbConn, catalogName);
    // @NOTE cannot throw NoSuchNamespaceException it might be parent of one of the existing namespaces
    if (!namespace.isEmpty() && !dao.isExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist %s", namespace);
    }
    try {
      List<Namespace> nsList = dao.getChildren(namespace);
      LOG.debug("From the namespace '{}' found: {}", namespace, nsList);
      return nsList;
    } catch (SQLException | JsonProcessingException throwables) {
      LOG.error("Failed to list namespace!", throwables);
      return null;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    JdbcNamespace dao = new JdbcNamespace(dbConn, catalogName);
    if (!dao.isExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace not found: %s", namespace);
    }
    try {
      Map<String, String> ns = dao.get(namespace);
      return JdbcUtil.stringToNamespaceMetadata(ns.get("namespace_metadata"));
    } catch (SQLException | JsonProcessingException throwables) {
      LOG.warn("Failed to load namespace metadata!", throwables);
      throw new NoSuchNamespaceException(new IOException(throwables), "Failed to load namespace metadata!");
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    JdbcNamespace dao = new JdbcNamespace(dbConn, catalogName);
    if (!dao.isExists(namespace)) {
      throw new NoSuchNamespaceException("Cannot drop namespace %s because it is not found!", namespace);
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException("Cannot drop namespace %s because it is not empty. " +
              "The following tables still exist under the namespace: %s", namespace, tableIdentifiers);
    }

    Path nsPath = new Path(warehouseLocation, SLASH.join(namespace.levels()));
    if (!isNamespace(nsPath) || namespace.isEmpty()) {
      return false;
    }

    try {
      if (fs.listStatusIterator(nsPath).hasNext()) {
        throw new NamespaceNotEmptyException("Namespace %s is not empty.", namespace);
      }

      fs.delete(nsPath, false /* recursive */);
      dao.delete(namespace);
      return true;
    } catch (IOException | SQLException throwables) {
      throw new UncheckedIOException("Namespace delete failed!", new IOException(throwables));
    }

  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws
          NoSuchNamespaceException {

    throw new UnsupportedOperationException(
            "Cannot set namespace properties " + namespace + " : setProperties is not supported");
// @TODO is this needed for Jdbc Catalog?
//    JdbcNamespaceDao dao = new JdbcNamespaceDao(dbConn, catalogName);
//    if (!dao.isExists(namespace)) {
//      throw new NoSuchNamespaceException("Namespace not found: %s", namespace);
//    }
//    try {
//      dao.update(namespace, properties);
//      LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
//      return true;
//    } catch (SQLException | JsonProcessingException throwables) {
//      return false;
//    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    Map<String, String> metadata = Maps.newHashMap(this.loadNamespaceMetadata(namespace));
    for (String property : properties) {
      metadata.remove(property);
    }
    this.setProperties(namespace, metadata);
    return true;
  }

  @Override
  public void close() throws IOException {
    try {
      this.dbConn.close();
    } catch (SQLException throwables) {
      throw new IOException(throwables);
    }
  }
}
