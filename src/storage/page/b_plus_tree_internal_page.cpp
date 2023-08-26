//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  SetRootPage(false);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;  // Not found
}

/**
 * Perform binary search to find the correct position to insert the given key.
 * @param key the key to be inserted
 * @param comparator the comparator to compare keys
 * @return index where the key should be inserted
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindInsertPosition(const KeyType &key, const KeyComparator &comparator) const
    -> int {
  int low = 1;  // start from 1 since 0 is reserved for the first child pointer
  int high = GetSize() - 1;

  while (low <= high) {
    int mid = (low + high) / 2;
    int comp_result = comparator(KeyAt(mid), key);

    if (comp_result == 0) {
      // This is optional: if duplicate keys are not allowed, you might handle this differently or report an error.
      return mid;
    }
    if (comp_result < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return low;
}

/**
 * Perform binary search to find the correct child pointer index for the given key.
 * @param key the key to search for
 * @param comparator the comparator to compare keys
 * @return index of the child pointer to follow
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindChildIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  if (comparator(key, KeyAt(1)) < 0) {
    return 0;  // return the index of the first child pointer if the key is less than the first key
  }

  int low = 1;
  int high = GetSize() - 1;

  while (low < high) {
    int mid = (low + high) / 2;
    int comp_result = comparator(KeyAt(mid), key);

    if (comp_result == 0) {
      return mid;
    }
    if (comp_result < 0) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return low;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int position, const KeyType &key, page_id_t child_id) {
  if (position == 0) {
    SetValueAt(position, child_id);
    IncreaseSize(1);
    return;
  }
  for (int i = GetSize(); i > position; i--) {
    array_[i] = array_[i - 1];
  }
  SetKeyAt(position, key);
  SetValueAt(position, child_id);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
