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


#ifndef IMPALA_EXEC_KUDU_SCAN_NODE_H_
#define IMPALA_EXEC_KUDU_SCAN_NODE_H_

#include <boost/scoped_ptr.hpp>

#include <kudu/client/client.h>

#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"

namespace impala {

class Tuple;

class KuduScanNode : public ScanNode {
 public:
  KuduScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~KuduScanNode();

  // Prepare conjuncts, create Kudu columns to slots mapping,
  // initialize hbase_scanner_, and create text_converter_.
  virtual Status Prepare(RuntimeState* state);

  // Start Kudu scan using hbase_scanner_.
  virtual Status Open(RuntimeState* state);

  // Fill the next row batch by calling Next() on the hbase_scanner_,
  // converting text data in Kudu cells to binary data.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // Close the hbase_scanner_, and report errors.
  virtual void Close(RuntimeState* state);

 protected:
  // Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  void CacheMaterializedSlots();
  Status BuildKuduSchema(kudu::client::KuduSchema* schema);

/*
  Status SetupScanRangePredicate(const TKuduKeyRange& key_range,
                                 kudu::client::KuduScanner* scanner);
  Status AddScanRangePredicate(kudu_predicate_field_t bound_type,
                               const std::string& encoded_key,
                               kudu_range_predicate_t* pred);
*/

  Status CloseCurrentScanner();
  bool HasMoreScanners();
  Status OpenNextScanner();


  Status FetchNextBlockIfEmpty(bool* end_of_scanner);
  Status DecodeRowsIntoRowBatch(RowBatch* batch, Tuple** tuple, bool* batch_done);
  void KuduRowToImpalaRow(const kudu::client::KuduRowResult& row,
                          RowBatch* row_batch,
                          Tuple* tuple);
  void AdvanceOneTuple(Tuple** tuple) const;



  // Tuple id resolved in Prepare() to set tuple_desc_;
  TupleId tuple_id_;

  // Descriptor of tuples read from Kudu table.
  const TupleDescriptor* tuple_desc_;

  // Tuple index in tuple row.
  int tuple_idx_;

  // The schema of the materialized slots (i.e projection)
  kudu::client::KuduSchema schema_;
  // The schema of the whole table
  kudu::client::KuduSchema table_schema_;
  std::tr1::shared_ptr<kudu::client::KuduClient> client_;
  scoped_refptr<kudu::client::KuduTable> table_;
  kudu::client::KuduScanner* scanner_;

  std::vector<TKuduKeyRange> scan_ranges_;
  // The index into scan_range_params_ for the range currently being
  // serviced.
  int cur_scan_range_idx_;

  // Column range predicates which have been pushed down.
  // This vector has one entry per slot, even if the slot is not
  // materialized. NULLs will be present where no predicate was pushed.
  //std::vector<RangePredicate*> range_predicates_;

  struct MaterializedSlotInfo {
    // the index of the slot in the overall tuple
    int slot_idx;
    int tuple_offset;
    SlotDescriptor* slot_desc;
  };
  std::vector<MaterializedSlotInfo> materialized_slots_;

  std::vector<kudu::client::KuduRowResult> cur_rows_;
  size_t num_rows_current_block_;
  size_t rows_scanned_current_block_;

  RuntimeProfile::Counter* kudu_read_timer_;
  RuntimeProfile::Counter* kudu_round_trips_;
  static const std::string KUDU_READ_TIMER;
  static const std::string KUDU_ROUND_TRIPS;

  // Counts the total number of conversion errors for this table.
  int num_errors_;

  // Size of tuple buffer determined by size of tuples and capacity of row batches.
  int tuple_buffer_size_;

  // Buffer into which fixed-length portion of tuple data is written in current
  // GetNext() call.
  void* tuple_buffer_;
};

}

#endif
