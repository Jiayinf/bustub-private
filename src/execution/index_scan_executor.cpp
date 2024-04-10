//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    index_info_ = (exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid()));

    if (index_info_ == nullptr) {
       throw ExecutionException("error");
    }

    table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);

    if (table_info_ == nullptr) {
        throw ExecutionException("error");
    }

    tree_it_ = (dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get()));
    if (tree_it_ == nullptr) {
        throw ExecutionException("error");
    }
    it_ = (tree_it_->GetBeginIterator());
    end_it_ = (tree_it_->GetEndIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    
    while (true) {
        if (it_ == end_it_) {
        return false;
        }

        *rid = (*it_).second;
        std::pair<TupleMeta, Tuple> res_tuple = table_info_->table_->GetTuple(*rid);
        // ++it_;

        if (res_tuple.first.is_deleted_) {
            *tuple = res_tuple.second;

            if (plan_->filter_predicate_ != nullptr) {
                auto value = plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema());
                if (value.IsNull() || !value.GetAs<bool>()) {
                    ++it_;
                    continue;
                }
            }

            ++it_;
            continue;
        } 
       
        *tuple = res_tuple.second;

        if (plan_->filter_predicate_ != nullptr) {
            auto value = plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema());
            if (value.IsNull() || !value.GetAs<bool>()) {
                ++it_;
                continue;
            }
        }
        ++it_;
        return true;   
    }
}

}  // namespace bustub