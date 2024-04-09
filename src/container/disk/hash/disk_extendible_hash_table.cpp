//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard page = bpm_->FetchPageRead(header_page_id_);

  auto header = page.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  ReadPageGuard d_page = bpm_->FetchPageRead(directory_page_id);
  page.Drop();

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto directory = d_page.As<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  d_page.Drop();

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard b_page = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = b_page.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  if (bucket->Lookup(key, value, cmp_)) {
    result->push_back(value);
    return true;
  }
  b_page.Drop();
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard page = bpm_->FetchPageWrite(header_page_id_);
  auto header = page.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);

  if (directory_page_id == INVALID_PAGE_ID) {
    BasicPageGuard d_page_basic = bpm_->NewPageGuarded(&directory_page_id);

    auto directory = d_page_basic.AsMut<ExtendibleHTableDirectoryPage>();
    directory->Init(directory_max_depth_);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
  }
  WritePageGuard d_page = bpm_->FetchPageWrite(directory_page_id);
  auto directory = d_page.AsMut<ExtendibleHTableDirectoryPage>();

  page.Drop();

  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    BasicPageGuard b_page_basic = bpm_->NewPageGuarded(&bucket_page_id);

    auto bucket = b_page_basic.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    directory->SetLocalDepth(bucket_idx, 0);
  }
  WritePageGuard b_page = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = b_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (bucket->Insert(key, value, cmp_)) {
    return true;
  }

  V v;
  if (bucket->Lookup(key, v, cmp_)) {
    return false;
  }

  if (bucket->IsFull()) {
    if (directory->GetLocalDepth(bucket_idx) == directory->GetGlobalDepth()) {
      if (directory->GetGlobalDepth() >= directory->GetMaxDepth()) {
        return false;
      }
      directory->IncrGlobalDepth();
    }

    directory->IncrLocalDepth(bucket_idx);

    if (!SplitBucket(directory, bucket, bucket_idx)) {
      return false;
    }
    d_page.Drop();
    b_page.Drop();
    return Insert(key, value, transaction);
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard page = bpm_->FetchPageWrite(header_page_id_);
  auto header = page.AsMut<ExtendibleHTableHeaderPage>();

  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  page.Drop();

  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard d_page = bpm_->FetchPageWrite(directory_page_id);
  auto directory = d_page.AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard b_page = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = b_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket->Remove(key, cmp_)) {
    return false;
  }
  if (bucket->IsEmpty()) {
    MergeBucket(directory, bucket, bucket_idx);
  }

  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  b_page.Drop();
  d_page.Drop();

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub