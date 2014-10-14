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

package com.cloudera.impala.planner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Analyzer;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.StringLiteral;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.KuduTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.Pair;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TKuduKeyRange;
import com.cloudera.impala.thrift.TKuduScanNode;
import com.cloudera.impala.thrift.TPlanNode;
import com.cloudera.impala.thrift.TPlanNodeType;
import com.cloudera.impala.thrift.TQueryOptions;
import com.cloudera.impala.thrift.TScanRange;
import com.cloudera.impala.thrift.TScanRangeLocation;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Deferred;

import kudu.Common;
import kudu.rpc.KuduClient;


/**
 * Full scan of an Kudu table.
 * Only families/qualifiers specified in TupleDescriptor will be retrieved in the backend.
 */
public class KuduScanNode extends ScanNode {
  private final static Logger LOG = LoggerFactory.getLogger(KuduScanNode.class);
  private final TupleDescriptor desc_;

  private static final long OPEN_TABLE_TIMEOUT_MS = 10000;

  public KuduScanNode(PlanNodeId id, TupleDescriptor desc) {
    super(id, desc, "SCAN KUDU");
    desc_ = desc;
    LOG.info("kudu scan node create", new Exception());
  }


  @Override
  public void init(Analyzer analyzer) throws InternalException {
    LOG.info("kudu scan node init", new Exception());
    assignConjuncts(analyzer);
//      analyzer.enforceSlotEquivalences(tupleIds_.get(0), conjuncts_);
    computeStats(analyzer);

    // materialize slots in remaining conjuncts_
    analyzer.materializeSlots(conjuncts_);

    computeMemLayout(analyzer);
  }

  /**
   * Also sets suggestedCaching_.
   */
  @Override
  public void computeStats(Analyzer analyzer) {
    super.computeStats(analyzer);

    // TODO: estimate stats
    cardinality_ = 0;
    LOG.debug("computeStats KuduScan: cardinality=" + Long.toString(cardinality_));

    // TODO: take actual regions into account
    numNodes_ = desc_.getTable().getNumNodes();
    LOG.debug("computeStats KuduScan: #nodes=" + Integer.toString(numNodes_));
  }

  @Override
  protected String debugString() {
    KuduTable tbl = (KuduTable) desc_.getTable();
    return Objects.toStringHelper(this)
        .add("tid", desc_.getId().asInt())
        .add("hiveTblName", tbl.getFullName())
        .add("kuduTblName", tbl.getKuduTableName())
        .addValue(super.debugString())
        .toString();
  }

  @Override
  protected void toThrift(TPlanNode msg) {
    msg.node_type = TPlanNodeType.KUDU_SCAN_NODE;
    msg.kudu_scan_node = new TKuduScanNode(desc_.getId().asInt());
  }

  // TODO: this should be in kudu client
  private static kudu.rpc.KuduTable syncOpenTable(KuduClient client, String tableName) throws IOException {
    Deferred<Object> d = client.openTable(tableName);
    try {
      return (kudu.rpc.KuduTable)d.join(OPEN_TABLE_TIMEOUT_MS);
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }

  private NavigableMap<KuduClient.RemoteTablet, List<Common.HostPortPB>> getLocations() {
    KuduTable table = (KuduTable)desc_.getTable();
    String tableName = table.getKuduTableName();
    HostAndPort hp = HostAndPort.fromString(table.getKuduMasterAddress());
    KuduClient client = new KuduClient(hp.getHostText(), hp.getPort());
    try {
      kudu.rpc.KuduTable tableClient = syncOpenTable(client, tableName);
      return tableClient.getTabletsLocations(OPEN_TABLE_TIMEOUT_MS);
    } catch (Exception e) {
      throw new RuntimeException("Could not get the tablets locations", e);
    } finally {
      client.shutdown();
    }
  }

  /**
   * We create a TScanRange for each region server that contains at least one
   * relevant region, and the created TScanRange will contain all the relevant regions
   * of that region server.
   */
  @Override
  public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
    KuduTable table = (KuduTable)desc_.getTable();
    String tableName = table.getKuduTableName();

    NavigableMap<KuduClient.RemoteTablet, List<Common.HostPortPB>> locations =
      getLocations();

    List<TScanRangeLocations> result = Lists.newArrayList();
    for (Map.Entry<KuduClient.RemoteTablet, List<Common.HostPortPB>> entry : locations.entrySet()) {
      KuduClient.RemoteTablet tablet = entry.getKey();
      List<Common.HostPortPB> hostPorts = entry.getValue();

      TScanRange scanRange = new TScanRange();
      TKuduKeyRange keyRange = new TKuduKeyRange();
      keyRange.setStartKey(tablet.getStartKey());
      keyRange.setStopKey(tablet.getEndKey());
      scanRange.setKudu_key_range(keyRange);

      TScanRangeLocations loc = new TScanRangeLocations();
      loc.setScan_range(scanRange);

      for (Common.HostPortPB hostPort : hostPorts) {
        String s = hostPort.getHost() + ":" + hostPort.getPort();
        loc.addToLocations(
          new TScanRangeLocation(addressToTNetworkAddress(s)));
      }
      result.add(loc);
    }

    LOG.info("Scan ranges:");
    for(TScanRangeLocations loc : result) {
      LOG.info("  " + loc.toString());
    }
    LOG.info("--------");

    return result;
  }

  @Override
  protected String getNodeExplainString(String prefix, String detailPrefix,
      TExplainLevel detailLevel) {
    KuduTable tbl = (KuduTable) desc_.getTable();
    StringBuilder output = new StringBuilder()
        .append(prefix + "table:" + tbl.getName() + "\n");
    if (!conjuncts_.isEmpty()) {
      output.append(prefix + "predicates: " + getExplainString(conjuncts_) + "\n");
    }
    // Add table and column stats in verbose mode.
    if (detailLevel == TExplainLevel.VERBOSE) {
      output.append(getStatsExplainString(prefix, detailLevel));
      output.append("\n");
    }
    return output.toString();
  }

  @Override
  public void computeCosts(TQueryOptions queryOptions) {
    // TODO: What's a good estimate of memory consumption?
    perHostMemCost_ = 1024L * 1024L * 1024L;
  }

  /**
   * Returns the per-host upper bound of memory that any number of concurrent scan nodes
   * will use. Used for estimating the per-host memory requirement of queries.
   */
  public static long getPerHostMemUpperBound() {
    // TODO: What's a good estimate of memory consumption?
    return 1024L * 1024L * 1024L;
  }
}
