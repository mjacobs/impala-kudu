// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.Pair;
import com.cloudera.impala.thrift.TCatalogObjectType;
import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TKuduTable;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.cloudera.impala.thrift.TResultSet;
import com.cloudera.impala.thrift.TResultSetMetadata;
import com.cloudera.impala.thrift.TTable;
import com.cloudera.impala.thrift.TTableDescriptor;
import com.cloudera.impala.thrift.TTableType;
import com.cloudera.impala.util.TResultRowBuilder;
import com.google.common.base.Preconditions;

/**
 * Impala representation of Kudu table metadata,
 * as loaded from Hive's metastore.
 * This implies that we inherit the metastore's limitations related to Kudu,
 * for example the lack of support for composite Kudu row keys.
 * We sort the Kudu columns (cols) by family/qualifier
 * to simplify the retrieval logic in the backend, since
 * Kudu returns data ordered by family/qualifier.
 * This implies that a "select *"-query on an Kudu table
 * will not have the columns ordered as they were declared in the DDL.
 * They will be ordered by family/qualifier.
 *
 */
public class KuduTable extends Table {
  private static final Logger LOG = Logger.getLogger(KuduTable.class);
  // Copied from Hive's KuduStorageHandler.java.
  public static final String DEFAULT_PREFIX = "default.";

  // Name of table in Kudu.
  // 'this.name' is the alias of the Kudu table in Hive.
  protected String kuduTableName_;

  // Address of kudu master.
  protected String kuduMasterAddress_;

  // Input format class for Kudu tables read by Hive.
  // This doesn't actually exist - just a placeholder.
  private static final String KUDU_INPUT_FORMAT =
      "com.cloudera.kudu.hive.HiveKuduTableInputFormat";
  // Storage handler class for Kudu tables read by Hive.
  // This doesn't actually exist - just a placeholder.
  private static final String KUDU_STORAGE_HANDLER =
      "com.cloudera.kudu.hive.KuduStorageHandler";

  // The hive table property which describes the Kudu table name.
  private static final String KUDU_TABLE_NAME_PROPERTY =
    "kudu.table_name";

  private static final String KUDU_MASTER_ADDRESS_PROPERTY =
    "kudu.master_address";

  protected KuduTable(TableId id, org.apache.hadoop.hive.metastore.api.Table msTbl,
      Db db, String name, String owner) {
    super(id, msTbl, db, name, owner);
    LOG.info("Creating KuduTable", new Exception());
  }

  /**
   * Create columns corresponding to fieldSchemas.
   * Throws a TableLoadingException if the metadata is incompatible with what we
   * support.
   */
  private void loadColumns(List<FieldSchema> fieldSchemas, HiveMetaStoreClient client)
      throws TableLoadingException {
    int pos = 0;
    for (FieldSchema s: fieldSchemas) {
      ColumnType type = parseColumnType(s);
      Column col = new Column(s.getName(), type, s.getComment(), pos);
      colsByPos_.add(col);
      colsByName_.put(s.getName(), col);
      ++pos;
      LOG.info("adding a column: " + s.getName());
      // TODO: column statistics
    }
  }

  @Override
  public void load(Table oldValue, HiveMetaStoreClient client,
      org.apache.hadoop.hive.metastore.api.Table msTbl) throws TableLoadingException {
    LOG.info("table load", new Exception());
    Preconditions.checkNotNull(getMetaStoreTable());
    try {
      kuduTableName_ = getKuduTableName(getMetaStoreTable());
      kuduMasterAddress_ = getKuduMasterAddress(getMetaStoreTable());
      Map<String, String> serdeParams =
          getMetaStoreTable().getSd().getSerdeInfo().getParameters();

      // Create column objects.
      List<FieldSchema> fieldSchemas = getMetaStoreTable().getSd().getCols();
      loadColumns(fieldSchemas, client);

      // Set table stats.
      numRows_ = getRowCount(super.getMetaStoreTable().getParameters());

      // since we don't support composite kudu rowkeys yet, all kudu tables have a
      // single clustering col
      numClusteringCols_ = 1;

    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for Kudu table: " +
          name_, e);
    }
  }

  @Override
  public void loadFromThrift(TTable table) throws TableLoadingException {
    super.loadFromThrift(table);
    try {
      kuduTableName_ = getKuduTableName(getMetaStoreTable());
      kuduMasterAddress_ = getKuduMasterAddress(getMetaStoreTable());
    } catch (Exception e) {
      throw new TableLoadingException("Failed to load metadata for Kudu table from " +
          "thrift table: " + name_, e);
    }
  }

  // This method is completely copied from Hive's KuduStorageHandler.java.
  private String getKuduTableName(org.apache.hadoop.hive.metastore.api.Table tbl) {
    // Give preference to TBLPROPERTIES over SERDEPROPERTIES
    // (really we should only use TBLPROPERTIES, so this is just
    // for backwards compatibility with the original specs).
    String tableName = tbl.getParameters().get(KUDU_TABLE_NAME_PROPERTY);
    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
          KUDU_TABLE_NAME_PROPERTY);
    }
    if (tableName == null) {
      tableName = tbl.getDbName() + "." + tbl.getTableName();
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    return tableName;
  }

  private String getKuduMasterAddress(org.apache.hadoop.hive.metastore.api.Table tbl) {
    String addr = tbl.getParameters().get(KUDU_MASTER_ADDRESS_PROPERTY);
    Preconditions.checkNotNull(addr, "No master address specified. Please set "
                               + KUDU_MASTER_ADDRESS_PROPERTY + " in serde properties.");
    return addr;
  }

  /**
   * Hive returns the columns in order of their declaration for Kudu tables.
   */
  @Override
  public ArrayList<Column> getColumnsInHiveOrder() { return colsByPos_; }

  @Override
  public TTableDescriptor toThriftDescriptor() {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(id_.asInt(), TTableType.KUDU_TABLE, colsByPos_.size(),
                             numClusteringCols_, kuduTableName_, db_.getName());
    tableDescriptor.setKuduTable(getTKuduTable());
    return tableDescriptor;
  }

  public String getKuduTableName() { return kuduTableName_; }

  public String getKuduMasterAddress() { return kuduMasterAddress_; }

  @Override
  public int getNumNodes() {
    // TODO: implement
    return 100;
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  @Override
  public TTable toThrift() {
    TTable table = super.toThrift();
    table.setTable_type(TTableType.KUDU_TABLE);
    table.setKudu_table(getTKuduTable());
    return table;
  }

  private TKuduTable getTKuduTable() {
    TKuduTable tKuduTable = new TKuduTable();
    tKuduTable.setTableName(kuduTableName_);
    tKuduTable.setMasterAddress(kuduMasterAddress_);

    List<String> colNames = new ArrayList<String>();
    for (int i = 0; i < colsByPos_.size(); ++i) {
      colNames.add(colsByPos_.get(i).getName());
    }
    tKuduTable.setColNames(colNames);

    return tKuduTable;
  }

  /**
   * Returns the input-format class string for Kudu tables read by Hive.
   */
  public static String getInputFormat() { return KUDU_INPUT_FORMAT; }

  /**
   * Returns the storage handler class for Kudu tables read by Hive.
   */
  @Override
  public String getStorageHandlerClassName() { return KUDU_STORAGE_HANDLER; }

  public static boolean isKuduTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return KUDU_STORAGE_HANDLER.equals(msTbl.getParameters().get("storage_handler"));
  }
}
