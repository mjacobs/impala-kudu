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

#include "exec/kudu-table-sink.h"

#include <vector>

#include "exec/kudu-util.h"
#include "util/logging.h"
#include "exprs/expr.h"

using namespace std;

namespace impala {

KuduTableSink::KuduTableSink(const RowDescriptor& row_desc,
                               const vector<TExpr>& select_list_texprs,
                               const TDataSink& tsink)
    : row_desc_(row_desc),
      select_list_texprs_(select_list_texprs),
      schema_(NULL),
      client_(NULL),
      row_builder_(NULL) {
}

Status KuduTableSink::PrepareExprs(RuntimeState* state) {
  // From the thrift expressions create the real exprs.
  RETURN_IF_ERROR(Expr::CreateExprTrees(state->obj_pool(), select_list_texprs_,
      &output_exprs_));
  // Prepare the exprs to run.
  RETURN_IF_ERROR(Expr::Prepare(output_exprs_, state, row_desc_));


  kudu_schema_builder_t* sb = kudu_new_schema_builder();
  if (sb == NULL) return Status("Could not create schema builder.");
  for (int i = 0; i < output_exprs_.size(); ++i) {
    stringstream col_name;
    col_name << "col " << i;
    kudu_type_t kt;
    RETURN_IF_ERROR(ImpalaToKuduType(output_exprs_[i]->type(), &kt));
    kudu_schema_builder_add(sb, col_name.str().c_str(), kt, false /* TODO nullable? */);
  }
  kudu_schema_builder_set_num_key_columns(sb, 1);

  char* err = NULL;
  int rc;
  if ((rc = kudu_schema_builder_build(sb, &schema_, &err)) != KUDU_OK) {
    return Status(string("Could not create schema: ") + err);
  }
  CHECK(schema_ != NULL);

  return Status::OK;
}

Status KuduTableSink::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(PrepareExprs(state));
  return Status::OK;
}

Status KuduTableSink::Open(RuntimeState* state) {
  char* err = NULL;
  kudu_err_t rc;
  if ((rc = kudu_client_open(NULL, &client_, &err)) != KUDU_OK) {
    return Status(err);
  }
  if ((rc = kudu_client_open_table(client_, "twitter", &table_, &err)) != KUDU_OK) {
    return Status(err);
  }
  row_builder_ = kudu_new_rowbuilder(schema_);
  DCHECK(row_builder_ != NULL);
  runtime_profile_ = state->obj_pool()->Add(
      new RuntimeProfile(state->obj_pool(), "KuduTableSink"));
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  (*state->num_appended_rows())[""] = 0L;
  return Status::OK;
}

Status KuduTableSink::Send(RuntimeState* state, RowBatch* batch, bool eos) {
  char* err = NULL;
  kudu_err_t rc;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  int rows_added = 0;
  // Since everything is set up just forward everything to the writer.
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* current_row = batch->GetRow(i);
    kudu_rowbuilder_reset(row_builder_);
    bool skip_row = false;

    stringstream ss;
    string str;
    kudu_slice_t slice;
    for (int j = 0; j < output_exprs_.size(); ++j) {
      void* value = output_exprs_[j]->GetValue(current_row);
      if (value == NULL) {
        skip_row = true;
        break;
      }

      switch (output_exprs_[j]->type().type) {
        case TYPE_STRING: {
          StringValue* sv = reinterpret_cast<StringValue*>(value);
          slice.data = reinterpret_cast<uint8_t*>(sv->ptr);
          slice.size = sv->len;
          kudu_rowbuilder_add_string(row_builder_, &slice);
          break;
        }
        case TYPE_FLOAT:
          ss.str(string());
          ss << *reinterpret_cast<float*>(value);
          str = ss.str();
          slice.data = (uint8_t*)str.c_str();
          slice.size = str.size();
          kudu_rowbuilder_add_string(row_builder_, &slice);
          break;
        case TYPE_DOUBLE:
          ss.str(string());
          ss << *reinterpret_cast<double*>(value);
          str = ss.str();
          slice.data = (uint8_t*)str.c_str();
          slice.size = str.size();
          kudu_rowbuilder_add_string(row_builder_, &slice);
          break;
        case TYPE_BOOLEAN:
          kudu_rowbuilder_add_uint32(row_builder_, *reinterpret_cast<bool*>(value));
          break;
        case TYPE_TINYINT:
          kudu_rowbuilder_add_uint32(row_builder_, *reinterpret_cast<uint8_t*>(value));
          break;
        case TYPE_SMALLINT:
          kudu_rowbuilder_add_uint32(row_builder_, *reinterpret_cast<uint16_t*>(value));
          break;
        case TYPE_INT:
          kudu_rowbuilder_add_uint32(row_builder_, *reinterpret_cast<uint32_t*>(value));
          break;
        case TYPE_BIGINT:
          kudu_rowbuilder_add_uint32(row_builder_, *reinterpret_cast<uint64_t*>(value));
          break;
        default:
          DCHECK(false);
      }
    }
    if (skip_row) continue;

    rc = kudu_table_insert(table_, row_builder_, &err);
    if (rc != KUDU_OK && rc != KUDU_ALREADY_PRESENT) {
      return Status(err);
    }
    ++rows_added;
  }

  (*state->num_appended_rows())[""] += rows_added;
  return Status::OK;
}

void KuduTableSink::Close(RuntimeState* state) {
  /*
  char* err = NULL;
  if (kudu_table_flush(table_, &err) != KUDU_OK) {
    return Status(err);
  }
TODO: flush? maybe if 'eos' is true in ::Send()?
  */

  if (row_builder_ != NULL) kudu_rowbuilder_free(row_builder_);
  if (table_ != NULL) kudu_table_free(table_);
  if (client_ != NULL) kudu_client_free(client_);
  if (schema_ != NULL) kudu_schema_free(schema_);
}

}  // namespace impala
