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

#include "exec/kudu-scan-node.h"

#include <boost/foreach.hpp>
#include <thrift/protocol/TDebugProtocol.h>
#include <vector>
#include <kudu/client/row_result.h>

#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "gutil/gscoped_ptr.h"
#include "gutil/strings/substitute.h"
#include "util/jni-util.h"
#include "util/periodic-counter-updater.h"
#include "util/runtime-profile.h"

using namespace std;
using apache::thrift::ThriftDebugString;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduClient;
using kudu::client::KuduRowResult;
using kudu::client::KuduTable;
using kudu::client::KuduScanner;

#define IMPALA_RETURN_NOT_OK(expr, prepend) \
  do { \
    kudu::Status _s = (expr); \
    if (PREDICT_FALSE(!_s.ok())) {                                      \
      return Status(strings::Substitute("$0: $1", prepend, _s.ToString())); \
    } \
  } while (0)

namespace impala {

const string KuduScanNode::KUDU_READ_TIMER = "TotalKuduReadTime";
const string KuduScanNode::KUDU_ROUND_TRIPS = "TotalKuduScanRoundTrips";

KuduScanNode::KuduScanNode(ObjectPool* pool, const TPlanNode& tnode,
                             const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      tuple_id_(tnode.kudu_scan_node.tuple_id),
      tuple_idx_(0),
      table_(NULL),
      scanner_(NULL),
      cur_scan_range_idx_(0),
      cur_rows_(NULL),
      rows_scanned_current_block_(0) {
}

KuduScanNode::~KuduScanNode() {
}

Status KuduScanNode::BuildKuduSchema(KuduSchema* schema) {
  const KuduTableDescriptor* table_desc = static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());
  LOG(INFO) << "Table desc for schema: " << table_desc->DebugString();

  vector<KuduColumnSchema> kudu_cols;
  const std::vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (int i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
    const string& col_name = table_desc->col_names()[col_idx];
    KuduColumnSchema::DataType kt = KuduColumnSchema::INT8;
    RETURN_IF_ERROR(ImpalaToKuduType(slots[i]->type(), &kt));
    // TODO: apparently all Impala columns are nullable?
    kudu_cols.push_back(KuduColumnSchema(col_name, kt, /* is_nullable = */ false));
  }
  schema->Reset(kudu_cols, /* key_cols = */ 0);
  return Status::OK;
}

Status KuduScanNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  kudu_read_timer_ = ADD_CHILD_TIMER(runtime_profile(), KUDU_READ_TIMER,
      SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);
  kudu_round_trips_ = ADD_COUNTER(runtime_profile(), KUDU_ROUND_TRIPS,
                                  TCounterType::UNIT);

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  if (tuple_desc_ == NULL) {
    return Status("Failed to get tuple descriptor.");
  }

  //ExtractRangePredicates(conjuncts_, tuple_desc_, &range_predicates_, pool_);

  RETURN_IF_ERROR(BuildKuduSchema(&schema_));

  // Convert TScanRangeParams to ScanRanges
  CHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";
  BOOST_FOREACH(const TScanRangeParams& params, *scan_range_params_) {
    LOG(INFO) << "==> scan range: " << ThriftDebugString(params);
    const TKuduKeyRange& key_range = params.scan_range.kudu_key_range;

    scan_ranges_.push_back(key_range);
  }

  return Status::OK;
}

#if 0
static Status AddBound(kudu_range_predicate_t* pred, kudu_predicate_field_t field,
                       Expr* expr) {
  void* val_ptr = CHECK_NOTNULL(expr->GetValue(NULL));
  size_t val_size;
  if (expr->type() == TYPE_STRING) {
    StringValue sv;
    memcpy(&sv, val_ptr, sizeof(sv));
    val_ptr = sv.ptr;
    val_size = sv.len;
  } else {
    val_size = expr->type().GetByteSize();
  }

  /*
  char* err = NULL;
  int rc;
  if ((rc = kudu_range_predicate_set(pred, field, val_ptr, val_size, &err)) != KUDU_OK) {
    return Status(string("Couldn't push predicate with expr ") + expr->DebugString() + ": " + err);
  }
  */
  return Status::OK;
}
#endif

Status KuduScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  const KuduTableDescriptor* table_desc = static_cast<const KuduTableDescriptor*>(tuple_desc_->table_desc());

  IMPALA_RETURN_NOT_OK(kudu::client::KuduClientBuilder()
                       .add_master_server_addr(table_desc->kudu_master_address())
                       .Build(&client_),
                       "Unable to create Kudu client");

  IMPALA_RETURN_NOT_OK(client_->OpenTable(table_desc->table_name(), &table_),
                       "Unable to open Kudu table");

  table_schema_ = table_->schema();

  CacheMaterializedSlots();

    /*
  for (int i = 0; i < slots.size(); i++) {
    if (!slots[i]->is_materialized()) continue;
    RangePredicate* range_pred = range_predicates_[i];
    if (range_pred != NULL) {
      DCHECK_EQ(range_pred->slot->slot_id(), i);
      DCHECK(range_pred->lower_bound || range_pred->upper_bound);

      bool pushable_lower = range_pred->lower_bound && range_pred->lower_bound_inclusive;
      bool pushable_upper = range_pred->upper_bound && range_pred->upper_bound_inclusive;
      if (!pushable_lower && !pushable_upper) {
        continue;
      }

      kudu_range_predicate_t* pred;
      if ((rc = kudu_create_range_predicate(schema_, kudu_projection_idx, &pred, &err)) != KUDU_OK) {
        return MakeStatusAndFree(err);
      }

      if (pushable_lower) {
        RETURN_IF_ERROR(AddBound(pred, KUDU_LOWER_BOUND, range_pred->lower_bound));
        VLOG(1) << "Pushed " << conjuncts_[range_pred->lower_conjunct_idx]->DebugString();
        conjuncts_[range_pred->lower_conjunct_idx] = NULL;
      }
      if (pushable_upper) {
        RETURN_IF_ERROR(AddBound(pred, KUDU_UPPER_BOUND, range_pred->upper_bound));
        VLOG(1) << "Pushed " << conjuncts_[range_pred->upper_conjunct_idx]->DebugString();
        conjuncts_[range_pred->upper_conjunct_idx] = NULL;
      }


      if ((rc = kudu_scanner_add_range_predicate(scanner_, pred, &err)) != KUDU_OK) {
        return MakeStatusAndFree(err);
      }
    }
  }
  */

  // Remove any conjuncts from our list which we've successfully pushed.
  /*
  std::vector<Expr*> remaining_conjuncts;
  BOOST_FOREACH(Expr* c, conjuncts_) {
    if (c != NULL) {
      remaining_conjuncts.push_back(c);
    }
  }
  conjuncts_.swap(remaining_conjuncts);
  */

  RETURN_IF_ERROR(OpenNextScanner());
  return Status::OK;
}

/*
Status KuduScanNode::AddScanRangePredicate(kudu_predicate_field_t bound_type,
                                           const string& encoded_key,
                                           kudu_range_predicate_t* pred) {
  char* err = NULL;
  kudu_err_t rc;
  kudu_decoded_key_t* key;
  if ((rc = kudu_decode_encoded_key(table_schema_,
                                    encoded_key.c_str(),
                                    encoded_key.size(),
                                    &key)) != KUDU_OK) {
    return MakeStatusAndFree(err);
  }

  const void* val = kudu_decoded_key_ptr(key);
  int val_size = kudu_schema_get_column_size(table_schema_, 0);
  LOG(INFO) << "Decoded key: " << string((char*)val, val_size) << "(" << val_size << ")";

  if ((rc = kudu_range_predicate_set(pred, bound_type, val, val_size, &err)) != KUDU_OK) {
    kudu_free_decoded_key(key);
    return MakeStatusAndFree(err);
  }

  kudu_free_decoded_key(key);

  return Status::OK;
}
*/

namespace {

// Mutate the string 's' into the string which lexicographically supports
// just before it. i.e the largest string which is less than 's'.
// Returns an bad Status if there is no such string (ie s is empty)
//
// This is the inverse of gutil's ImmediateSuccessor() function
Status ImmediatePredecessor(string* s) {
  if (s->empty()) {
    return Status("No predecessor to empty string");
  }
  char* last = &(*s)[s->length() - 1];
  if (*last == '\0') {
    s->resize(s->size() - 1);
  } else {
    (*last)--;
  }
  return Status::OK;
}

} // anonymous namespace

Status KuduScanNode::SetupScanRangePredicate(const TKuduKeyRange& key_range,
                                             KuduScanner* scanner) {
  if (key_range.startKey.empty() && key_range.stopKey.empty()) {
    return Status::OK;
  }

  if (!key_range.startKey.empty()) {
    IMPALA_RETURN_NOT_OK(scanner->AddLowerBound(key_range.startKey),
                         "adding scan range lower bound");
  }
  if (!key_range.stopKey.empty()) {
    IMPALA_RETURN_NOT_OK(scanner->AddUpperBound(key_range.stopKey),
                         "adding scan range upper bound");
  }

  return Status::OK;
}

Status KuduScanNode::CloseCurrentScanner() {
  if (scanner_) {
    delete scanner_;
    scanner_ = NULL;
    cur_scan_range_idx_++;
  }
  return Status::OK;
}

bool KuduScanNode::HasMoreScanners() {
  return cur_scan_range_idx_ < scan_ranges_.size();
}

Status KuduScanNode::OpenNextScanner()  {
  CHECK(scanner_ == NULL);
  CHECK_LT(cur_scan_range_idx_, scan_ranges_.size());
  const TKuduKeyRange& key_range = scan_ranges_[cur_scan_range_idx_];

  VLOG(1) << "Starting scanner " << (cur_scan_range_idx_ + 1)
          << "/" << scan_ranges_.size();

  kudu::Status s;
  scanner_ = new KuduScanner(table_.get());
  IMPALA_RETURN_NOT_OK(scanner_->SetProjection(&schema_),
                       "Unable to set projection");

  RETURN_IF_ERROR(SetupScanRangePredicate(key_range, scanner_));

  IMPALA_RETURN_NOT_OK(scanner_->Open(),
                       "Unable to open scanner");

  return Status::OK;
}

void KuduScanNode::CacheMaterializedSlots() {
  DCHECK(materialized_slots_.empty());

  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  int kudu_projection_idx = 0;
  for (int i = 0; i < slots.size(); i++) {
    if (!slots[i]->is_materialized()) continue;

    MaterializedSlotInfo info;
    info.slot_idx = i;
    info.slot_desc = slots[i];

    materialized_slots_.push_back(info);
    kudu_projection_idx++;
  }
}

void KuduScanNode::KuduRowToImpalaRow(const KuduRowResult& row,
                                      RowBatch* row_batch,
                                      Tuple* tuple) {
  for (int i = 0; i < materialized_slots_.size(); ++i) {
    const MaterializedSlotInfo& info = materialized_slots_[i];

    void* slot = tuple->GetSlot(info.slot_desc->tuple_offset());

    switch (info.slot_desc->type().type) {
      case TYPE_STRING: {
        kudu::Slice slice;
        KUDU_CHECK_OK(row.GetString(i, &slice));
        char* buffer = (char*)row_batch->tuple_data_pool()->Allocate(slice.size());
        memcpy(buffer, slice.data(), slice.size());
        reinterpret_cast<StringValue*>(slot)->ptr = buffer;
        reinterpret_cast<StringValue*>(slot)->len = slice.size();
        break;
      }
      case TYPE_TINYINT:
        KUDU_CHECK_OK(row.GetUInt8(i, reinterpret_cast<uint8_t*>(slot)));
        break;
      case TYPE_SMALLINT:
        KUDU_CHECK_OK(row.GetUInt16(i, reinterpret_cast<uint16_t*>(slot)));
        break;
      case TYPE_INT:
        KUDU_CHECK_OK(row.GetUInt32(i, reinterpret_cast<uint32_t*>(slot)));
        break;
      case TYPE_BIGINT:
        KUDU_CHECK_OK(row.GetUInt64(i, reinterpret_cast<uint64_t*>(slot)));
        break;
      default:
        DCHECK(false);
    }
  }
}

Status KuduScanNode::FetchNextBlockIfEmpty(bool* end_of_scanner) {
  if (rows_scanned_current_block_ < cur_rows_.size()) {
    // More remaining in this block which we haven't processed yet
    VLOG(1) << "Already have enough in current block";
    return Status::OK;
  }

  if (!scanner_->HasMoreRows()) {
    VLOG(1) << "Scanner ended";
    *end_of_scanner = true;
    return Status::OK;
  }

  SCOPED_TIMER(kudu_read_timer_);

  VLOG(1) << "Fetching next block from kudu";
  cur_rows_.clear();
  IMPALA_RETURN_NOT_OK(scanner_->NextBatch(&cur_rows_),
                       "Unable to advance iterator");
  kudu_round_trips_->Add(1);
  rows_scanned_current_block_ = 0;
  return Status::OK;
}

void KuduScanNode::AdvanceOneTuple(Tuple** tuple) const {
  char* new_tuple = reinterpret_cast<char*>(*tuple);
  new_tuple += tuple_desc_->byte_size();
  *tuple = reinterpret_cast<Tuple*>(new_tuple);
}

Status KuduScanNode::DecodeRowsIntoRowBatch(RowBatch* row_batch, Tuple** tuple, bool* batch_done) {
  // Get pointer to the next row. We'll march this pointer through as we collect rows.
  int idx = row_batch->AddRow();
  TupleRow* row = row_batch->GetRow(idx);
  row->SetTuple(tuple_idx_, *tuple);
  (*tuple)->Init(tuple_desc_->num_null_bytes());

  VLOG(1) << "Decoding " << (cur_rows_.size() - rows_scanned_current_block_)
          << " rows from current batch";

  for (int krow_idx = rows_scanned_current_block_; krow_idx < cur_rows_.size(); ++krow_idx) {
    const KuduRowResult& krow = cur_rows_[krow_idx];
    KuduRowToImpalaRow(krow, row_batch, *tuple);
    ++rows_scanned_current_block_;

    if (conjunct_ctxs_.empty() ||
        EvalConjuncts(&conjunct_ctxs_[0], conjunct_ctxs_.size(), row)) {
      row_batch->CommitLastRow();
      ++num_rows_returned_;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      AdvanceOneTuple(tuple);
      row = row_batch->AdvanceRow(row);
      if (row_batch->AtCapacity()) {
        *batch_done = true;
        break;
      }
      row->SetTuple(tuple_idx_, *tuple);
      (*tuple)->Init(tuple_desc_->num_null_bytes());
    }

    if (row_batch->tuple_data_pool()->total_allocated_bytes() > RowBatch::AT_CAPACITY_MEM_USAGE) {
      *batch_done = true;
      break;
    }
  }
  return Status::OK;
}

Status KuduScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  SCOPED_TIMER(materialize_tuple_timer());
  *eos = false;
  if (ReachedLimit()) {
    *eos = true;
    return Status::OK;
  }

  // create new tuple buffer for row_batch
  tuple_buffer_size_ = row_batch->capacity() * tuple_desc_->byte_size();
  VLOG(1) << "Allocating tuple buffer size: " << tuple_buffer_size_;
  tuple_buffer_ = row_batch->tuple_data_pool()->Allocate(tuple_buffer_size_);
  // Current tuple.
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer_);

  bool batch_done = false;
  while (!batch_done) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(QueryMaintenance(state));

    bool end_of_scanner = false;
    RETURN_IF_ERROR(FetchNextBlockIfEmpty(&end_of_scanner));
    if (UNLIKELY(end_of_scanner)) {
      VLOG(1) << "End of scanner";
      RETURN_IF_ERROR(CloseCurrentScanner());
      if (HasMoreScanners()) {
        VLOG(1) << "Opening next scanner";
        RETURN_IF_ERROR(OpenNextScanner());
        continue;
      } else {
        VLOG(1) << "No more scanners";
        *eos = true;
        return Status::OK;
      }
    }

    if (*eos) {
      VLOG(1) << "End of stream";
      return Status::OK;
    }

    RETURN_IF_ERROR(DecodeRowsIntoRowBatch(row_batch, &tuple, &batch_done));
  }
  *eos = ReachedLimit();
  VLOG(1) << "Batch done. eos: " << *eos;
  return Status::OK;
}

void KuduScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  cur_rows_.clear();
  if (scanner_ != NULL) delete scanner_;
  ExecNode::Close(state);
}

void KuduScanNode::DebugString(int indentation_level, stringstream* out) const {
  string indent(indentation_level * 2, ' ');
  *out << indent << "KuduScanNode(tupleid=" << tuple_id_ << ", range predicates=[" << endl;
  /*
  BOOST_FOREACH(const RangePredicate* pred, range_predicates_) {
    if (pred != NULL) {
      *out << indent << indent << pred->DebugString() << endl;
    }
  }
  */
  *out << indent << "])";
}

}  // namespace impala
