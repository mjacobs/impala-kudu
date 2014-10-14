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

#ifndef IMPALA_EXEC_KUDU_UTIL_H_
#define IMPALA_EXEC_KUDU_UTIL_H_

#include "runtime/descriptors.h"

namespace impala {

static Status ImpalaToKuduType(const ColumnType& impala_type, kudu_type_t* kudu_type) {
  switch (impala_type.type) {
    case TYPE_STRING:
      *kudu_type = KUDU_STRING;
      break;
    case TYPE_TINYINT:
      *kudu_type = KUDU_UINT8;
      break;
    case TYPE_SMALLINT:
      *kudu_type = KUDU_UINT16;
      break;
    case TYPE_INT:
      *kudu_type = KUDU_UINT32;
      break;
    case TYPE_BIGINT:
      *kudu_type = KUDU_UINT64;
      break;
    default:
      return Status("Unsupported column type.");
  }
  return Status::OK;
}


} // namespace impala
#endif
