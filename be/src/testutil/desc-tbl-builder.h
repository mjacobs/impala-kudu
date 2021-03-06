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

#ifndef IMPALA_TESTUTIL_ROW_DESC_BUILDER_H_
#define IMPALA_TESTUTIL_ROW_DESC_BUILDER_H_

#include "runtime/runtime-state.h"

namespace impala {

class ObjectPool;
class TupleDescBuilder;

/// Aids in the construction of a DescriptorTbl by declaring tuples and slots
/// associated with those tuples.
/// TupleIds are monotonically increasing from 0 for each DeclareTuple, and
/// SlotIds increase similarly, but are always greater than all TupleIds.
/// Unlike FE, slots are not reordered based on size, and padding is not addded.
//
/// Example usage:
/// DescriptorTblBuilder builder;
/// builder.DeclareTuple() << TYPE_TINYINT << TYPE_TIMESTAMP; // gets TupleId 0
/// builder.DeclareTuple() << TYPE_FLOAT; // gets TupleId 1
/// DescriptorTbl desc_tbl = builder.Build();
class DescriptorTblBuilder {
 public:
  DescriptorTblBuilder(ObjectPool* object_pool);

  TupleDescBuilder& DeclareTuple();

  // Allows to set a TableDescriptor on TDescriptorTable.
  // Only one can be set.
  void SetTableDescriptor(const TTableDescriptor& table_desc);

  DescriptorTbl* Build();

 private:
  /// Owned by caller.
  ObjectPool* obj_pool_;

  std::vector<TupleDescBuilder*> tuples_descs_;
  TDescriptorTable thrift_desc_tbl_;
};

class TupleDescBuilder {
 public:
  struct Slot {
    Slot(ColumnType type, bool mat = true)
      : slot_type(type), materialized(mat) {}
    ColumnType slot_type;
    bool materialized;
  };

  TupleDescBuilder& operator<< (ColumnType slot_type) {
    slots_.push_back(Slot(slot_type));
    return *this;
  }

  TupleDescBuilder& AddSlot(ColumnType slot_type, bool materialized) {
    slots_.push_back(Slot(slot_type, materialized));
    return *this;
  }

  std::vector<Slot> slots() const { return slots_; }

 private:
  std::vector<Slot> slots_;
};

}

#endif
