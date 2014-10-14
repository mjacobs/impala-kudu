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

#ifndef IMPALA_EXEC_KUDU_TABLE_SINK_H
#define IMPALA_EXEC_KUDU_TABLE_SINK_H

#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/descriptors.h"
#include "exec/data-sink.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Exprs_types.h"
#include <libkudu/libkudu.h>

namespace impala {

// Class to take row batches and send them to kudu.
class KuduTableSink : public DataSink {
 public:
  KuduTableSink(const RowDescriptor& row_desc,
                 const std::vector<TExpr>& select_list_texprs,
                 const TDataSink& tsink);
  Status Prepare(RuntimeState* state);
  Status Open(RuntimeState* state);
  Status Send(RuntimeState* state, RowBatch* batch, bool eos);
  void Close(RuntimeState* state);
  RuntimeProfile* profile() { return runtime_profile_; }

 private:
  // Turn thrift TExpr into Expr and prepare them to run
  Status PrepareExprs(RuntimeState* state);

  // Owned by the RuntimeState.
  const RowDescriptor& row_desc_;

  // Owned by the RuntimeState.
  const std::vector<TExpr>& select_list_texprs_;
  std::vector<Expr*> output_exprs_;

  kudu_schema_t* schema_;
  kudu_client_t* client_;
  kudu_table_t* table_;
  kudu_rowbuilder_t* row_builder_;

  // Allocated from runtime state's pool.
  RuntimeProfile* runtime_profile_;
};

}  // namespace impala

#endif
