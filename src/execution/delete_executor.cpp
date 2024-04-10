//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  // auto table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (used_) {
    return false;
  }
  int32_t size = 0;
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  auto indices = catalog->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(tuple, rid)) {
    // delete odd
    auto old_tuple_meta = TupleMeta();
    old_tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(old_tuple_meta, *rid);

    for (auto index_info : indices) {
      auto newkey =
          (*tuple).KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(newkey, *rid, exec_ctx_->GetTransaction());
    }
    size++;
  }
  std::vector<Value> val;

  val.emplace_back(INTEGER, size);

  *tuple = Tuple(val, &GetOutputSchema());

  used_ = true;
  return true;
}

}  // namespace bustub
