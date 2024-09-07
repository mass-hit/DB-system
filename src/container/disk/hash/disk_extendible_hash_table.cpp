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
  index_name_ = name;
  auto header_guard=bpm_->NewPageGuarded(&header_page_id_).UpgradeWrite();
  auto header_page=header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash=Hash(key);
  auto header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page=header_guard.As<ExtendibleHTableHeaderPage>();
  page_id_t directory_page_id= header_page->GetDirectoryPageId(header_page->HashToDirectoryIndex(hash));
  header_guard.Drop();
  if(directory_page_id==INVALID_PAGE_ID) {
    return false;
  }
  auto directory_guard=bpm_->FetchPageRead(directory_page_id);
  auto directory_page=directory_guard.As<ExtendibleHTableDirectoryPage>();
  auto bucket_page_id=directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash));
  directory_guard.Drop();
  if(bucket_page_id==INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard=bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page=bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  bool found=bucket_page->Lookup(key, value,cmp_);
  bucket_guard.Drop();
  if(!found) {
    return false;
  }
  result->push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  auto header_guard=bpm_->FetchPageWrite(header_page_id_);
  auto header_page=header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto directory_idx=header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id= header_page->GetDirectoryPageId(directory_idx);
  if(directory_page_id==INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page,directory_idx,hash,key,value);
  }
  header_guard.Drop();
  auto directory_guard=bpm_->FetchPageWrite(directory_page_id);
  auto directory_page=directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx=directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id=directory_page->GetBucketPageId(bucket_idx);
  if(bucket_page_id==INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page,bucket_idx,key,value);
  }
  auto bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  V old_value;
  if(bucket_page->Lookup(key,old_value,cmp_)) {
    return false;
  }
  while(bucket_page->IsFull()) {
    auto global_depth=directory_page->GetGlobalDepth();
    if(global_depth==directory_page->GetLocalDepth(bucket_idx)) {
      if(global_depth==directory_page->GetMaxDepth()) {
        return false;
      }
      directory_page->IncrGlobalDepth();
    }
    page_id_t new_bucket_page_id;
    auto new_bucket_page=bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite().AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    new_bucket_page->Init(bucket_max_size_);
    directory_page->IncrLocalDepth(bucket_idx);
    auto new_bucket_idx=directory_page->GetSplitImageIndex(bucket_idx);
    UpdateDirectoryMapping(directory_page,new_bucket_idx,new_bucket_page_id,directory_page->GetLocalDepth(bucket_idx),directory_page->GetLocalDepthMask(bucket_idx));
    for(uint32_t i=0;i<bucket_page->Size();i++) {
      auto tmp_key=bucket_page->KeyAt(i);
      auto tmp_value=bucket_page->ValueAt(i);
      if(directory_page->HashToBucketIndex(Hash(tmp_key))==new_bucket_idx) {
        new_bucket_page->Insert(tmp_key,tmp_value,cmp_);
        bucket_page->Remove(tmp_key,cmp_);
        i--;
      }
    }
    bucket_guard.Drop();
    bucket_idx=directory_page->HashToBucketIndex(hash);
    bucket_page_id=directory_page->GetBucketPageId(bucket_idx);
    bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
    bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  return bucket_page->Insert(key,value,cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  auto directory_guard=bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  auto directory_page=directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx,directory_page_id);
  return InsertToNewBucket(directory_page,directory_page->HashToBucketIndex(hash),key,value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto bucket_guard=bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx,bucket_page_id);
  directory->SetLocalDepth(bucket_idx,0);
  return bucket_page->Insert(key,value,cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  uint32_t size=directory->Size();
  for(uint32_t i=0;i<size;i++) {
    if ((i & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      directory->SetBucketPageId(i,new_bucket_page_id);
      directory->SetLocalDepth(i,new_local_depth);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  auto header_guard=bpm_->FetchPageRead(header_page_id_);
  auto header_page=header_guard.As<ExtendibleHTableHeaderPage>();
  auto directory_idx=header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id= header_page->GetDirectoryPageId(directory_idx);
  if(directory_page_id==INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();
  auto directory_guard=bpm_->FetchPageWrite(directory_page_id);
  auto directory_page=directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx=directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id=directory_page->GetBucketPageId(bucket_idx);
  if(bucket_page_id==INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if(bucket_page->Remove(key,cmp_)) {
    while(bucket_page->IsEmpty()&&directory_page->GetLocalDepth(bucket_idx)!=0) {
      bucket_guard.Drop();
      auto image_idx=directory_page->GetSplitImageIndex(bucket_idx);
      auto image_page_id=directory_page->GetBucketPageId(image_idx);
      if(directory_page->GetLocalDepth(bucket_idx)==directory_page->GetLocalDepth(image_idx)) {
        directory_page->DecrLocalDepth(image_idx);
        directory_page->DecrLocalDepth(bucket_idx);
        bpm_->DeletePage(bucket_page_id);
        if(directory_page->CanShrink()) {
          directory_page->DecrGlobalDepth();
        }
        auto new_local_depth=directory_page->GetLocalDepth(image_idx);
        UpdateDirectoryMapping(directory_page,image_idx,image_page_id,new_local_depth,directory_page->GetLocalDepthMask(image_idx));
        if(new_local_depth==0) {
          break;
        }
        bucket_idx=directory_page->GetSplitImageIndex(directory_page->HashToBucketIndex(hash));
        bucket_page_id=directory_page->GetBucketPageId(bucket_idx);
        bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
        bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      }
      else {
        break;
      }
    }
    return true;
  }
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
