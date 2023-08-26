#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafNode(const KeyType &key) -> page_id_t {
  page_id_t current_page_id = GetRootPageId();

  while (current_page_id != INVALID_PAGE_ID) {
    auto page_guard = bpm_->FetchPageRead(current_page_id);
    auto node_page = page_guard.template As<InternalPage>();  // NOLINT

    if (node_page->IsLeafPage()) {
      return current_page_id;
    }
    auto child_index = node_page->FindChildIndex(key, comparator_);

    // Get the child's page_id and continue the traversal
    current_page_id = node_page->ValueAt(child_index);
  }

  // This should not happen if the tree is properly constructed.
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(const KeyType &key, const ValueType &value, LeafPage *page) -> bool {
  if (page->GetMaxSize() - 1 == page->GetSize()) {
    return false;
  }
  int position = page->FindPosition(key, comparator_);
  page->ShiftAt(position);
  page->SetAt(position, key, value);
  page->IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *old_node, const KeyType &middle_key, BPlusTreePage *new_node) {
  // If old_node is the root, create a new root
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    auto new_root_guard = bpm_->NewPageGuarded(&new_root_id);
    InternalPage *new_root = new_root_guard.AsMut<InternalPage>();  // NOLINT
    new_root->Init();
    new_root->SetRootPage(true);
    new_root->SetPage(new_root_id);

    new_root->SetKeyAt(1, middle_key);
    new_root->SetValueAt(1, new_node->GetPage());
    new_root->SetValueAt(0, old_node->GetPage());
    new_root->SetSize(2);
    new_root->SetMaxSize(internal_max_size_);

    new_node->SetParent(new_root_id);
    old_node->SetParent(new_root_id);

    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header = header_guard.AsMut<BPlusTreeHeaderPage>();
    header->root_page_id_ = new_root_id;

    return;
  }

  // Get the parent of old_node
  auto parent_guard = bpm_->FetchPageWrite(old_node->GetParent());
  InternalPage *parent = parent_guard.AsMut<InternalPage>();  // NOLINT

  // If parent has space, just insert the key
  if (parent->GetSize() < parent->GetMaxSize()) {
    int position = parent->FindInsertPosition(middle_key, comparator_);
    parent->InsertAt(position, middle_key, new_node->GetPage());
    new_node->SetParent(old_node->GetParent());
    return;
  }

  // Otherwise, split the parent
  SplitInternalNode(parent, middle_key, new_node->GetPage());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternalNode(InternalPage *page, const KeyType &key, page_id_t child_id) {
  // Create the temporary storage
  std::vector<std::pair<KeyType, page_id_t>> tmp;
  int position = 0;
  tmp.reserve(page->GetSize() + 1);
  for (int i = 0; i < page->GetSize(); i++) {
    tmp.push_back(std::make_pair(page->KeyAt(i), page->ValueAt(i)));
  }
  position = page->FindChildIndex(key, comparator_);
  tmp.insert(tmp.begin() + position + 1, std::make_pair(key, child_id));

  page_id_t new_internal_id;
  auto new_internal_guard = bpm_->NewPageGuarded(&new_internal_id);
  InternalPage *new_internal = new_internal_guard.template AsMut<InternalPage>();  // NOLINT
  new_internal->Init();
  new_internal->SetMaxSize(internal_max_size_);
  new_internal->SetPage(new_internal_id);

  size_t i = 0;
  size_t middle_index = (page->GetMaxSize() + 1) / 2;
  for (; i < middle_index; i++) {
    page->SetKeyAt(i, tmp[i].first);
    page->SetValueAt(i, tmp[i].second);
  }
  KeyType middle_key = tmp[i].first;
  size_t j = 0;
  for (; i < tmp.size(); i++, j++) {
    new_internal->SetKeyAt(j, tmp[i].first);
    new_internal->SetValueAt(j, tmp[i].second);
    page->IncreaseSize(-1);
    new_internal->IncreaseSize(1);
  }

  page->IncreaseSize(1);

  auto child_page_guard = bpm_->FetchPageWrite(child_id);
  auto child_page = child_page_guard.template AsMut<BPlusTreePage>();  // NOLINT
  child_page->SetParent(new_internal_id);

  InsertInParent(page, middle_key, new_internal);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeafNode(LeafPage *page, const KeyType &key, const ValueType &value) {
  // Create the temporary storage T
  std::vector<std::pair<KeyType, ValueType>> tmp;
  int position = 0;
  tmp.reserve(page->GetSize() + 1);
  for (int i = 0; i < page->GetSize(); i++) {
    tmp.push_back(std::make_pair(page->KeyAt(i), page->ValueAt(i)));
  }

  position = page->FindPosition(key, comparator_);

  tmp.insert(tmp.begin() + position, std::make_pair(key, value));

  // Create the new leaf node
  page_id_t new_leaf_id;
  auto new_page_guard = bpm_->NewPageGuarded(&new_leaf_id);
  auto new_leaf = new_page_guard.AsMut<LeafPage>();
  new_leaf->Init();
  new_leaf->SetMaxSize(leaf_max_size_);
  new_leaf->SetPage(new_leaf_id);

  size_t i = 0;
  size_t middle_index = (page->GetMaxSize()) / 2;
  for (; i < middle_index; i++) {
    page->SetAt(i, tmp[i].first, tmp[i].second);
  }

  KeyType middle_key = tmp[i].first;  // which is the key we need insert it to the parent
  size_t j = 0;
  for (; i < tmp.size(); i++, j++) {
    new_leaf->SetAt(j, tmp[i].first, tmp[i].second);
    page->IncreaseSize(-1);
    new_leaf->IncreaseSize(1);
  }

  page->IncreaseSize(1);

  // update the sibling pointer
  new_leaf->SetNextPageId(page->GetNextPageId());
  page->SetNextPageId(new_leaf_id);

  InsertInParent(page, middle_key, new_leaf);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  // if tree is empty create an empty leaf node , which is also the root
  if (IsEmpty()) {
    page_id_t root_id;
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto new_page_guard = bpm_->NewPageGuarded(&root_id);
    auto header = header_guard.AsMut<BPlusTreeHeaderPage>();
    LeafPage *root_page = new_page_guard.AsMut<LeafPage>();  // NOLINT
    header->root_page_id_ = root_id;
    root_page->Init();
    root_page->SetRootPage(true);
    root_page->SetPage(root_id);
    root_page->SetMaxSize(leaf_max_size_);
    InsertInLeaf(key, value, root_page);
    return true;
  }

  auto leaf_node_id = FindLeafNode(key);
  auto leaf_guard = bpm_->FetchPageWrite(leaf_node_id);
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();

  if (InsertInLeaf(key, value, leaf_page)) {
    return true;
  }
  SplitLeafNode(leaf_page, key, value);
  // insert in leaf false, split it

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input >> key) {
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

/*
 * This method is used for test only
 * Read data from file and insert/remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
