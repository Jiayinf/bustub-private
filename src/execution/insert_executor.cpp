//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (used_) {
    return false;
  }
  int size = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_temp = catalog->GetTable(plan_->GetTableOid());
  auto table_heap = table_temp->table_.get();
  auto indices = catalog->GetTableIndexes(table_temp->name_);

  while (child_executor_->Next(tuple, rid)) {
    auto tuple_meta = TupleMeta();
    tuple_meta.is_deleted_ = false;
    auto new_rid = table_heap->InsertTuple(tuple_meta, *tuple);
    for (auto index_info : indices) {
      auto key = (*tuple).KeyFromTuple(table_temp->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, new_rid.value(), exec_ctx_->GetTransaction());
    }
    size++;
  }
  std::vector<Value> val;
  //   std::vector<Column> col;
  val.emplace_back(INTEGER, size);
  //   col.emplace_back("Inserted_Rows", INTEGER);
  //   auto schema = Schema(col);

  //   *tuple = Tuple(val, &schema);

  *tuple = Tuple(val, &GetOutputSchema());

  used_ = true;
  return true;
}

}  // namespace bustub