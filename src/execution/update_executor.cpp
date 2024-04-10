//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (used_) {
    return false;
  }
  int size = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_heap = table_info_->table_.get();
  auto indices = catalog->GetTableIndexes(table_info_->name_);

  while (child_executor_->Next(tuple, rid)) {
    // add new
    auto tuple_meta = TupleMeta();
    tuple_meta.is_deleted_ = false;
    std::vector<Value> values{};

    // delete odd
    auto old_tuple_meta = TupleMeta();
    old_tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(old_tuple_meta, *rid);

    values.reserve(table_info_->schema_.GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }
    auto new_tuple = Tuple{values, &table_info_->schema_};
    auto new_rid = table_heap->InsertTuple(tuple_meta, new_tuple);

    // update
    for (auto index_info : indices) {
      auto newkey =
          new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      auto oldkey =
          (*tuple).KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

      index_info->index_->InsertEntry(newkey, new_rid.value(), exec_ctx_->GetTransaction());
      index_info->index_->DeleteEntry(oldkey, *rid, exec_ctx_->GetTransaction());
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
