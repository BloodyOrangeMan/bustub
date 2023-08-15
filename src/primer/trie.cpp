#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  auto curr = this->root_;

  for (char c : key) {
    if (!curr) {
      return nullptr;
    }

    auto iter = curr->children_.find(c);

    if (iter == curr->children_.end()) {
      return nullptr;
    }
    curr = iter->second;
  }

  if (curr->is_value_node_ && curr) {
    auto result = dynamic_cast<const TrieNodeWithValue<T> *>(curr.get());
    if (result && result->value_) {
      return result->value_.get();
    }
  }

  return nullptr;

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  auto new_root = root_ ? root_->Clone() : std::make_shared<TrieNode>();

  if (key.empty()) {
    new_root = std::shared_ptr<TrieNodeWithValue<T>>(
        new TrieNodeWithValue<T>(root_->children_, std::make_shared<T>(std::move(value))));
    return Trie(new_root);
  }

  std::shared_ptr<TrieNode> current_node = new_root;
  for (size_t i = 0; i < key.length() - 1; i++) {
    char c = key.at(i);
    if (current_node->children_.find(c) == current_node->children_.end()) {
      current_node->children_[c] = std::make_shared<TrieNode>();
    } else {
      current_node->children_[c] = current_node->children_[c]->Clone();
    }
    current_node = std::const_pointer_cast<TrieNode>(current_node->children_[c]);
  }

  // At the end of the key, set the node's value
  if (current_node->children_[key.back()]) {
    current_node->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(
        current_node->children_[key.back()]->children_, std::make_shared<T>(std::move(value)));
  } else {
    auto value_node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    value_node->is_value_node_ = true;
    current_node->children_[key.back()] = value_node;
  }

  return Trie(new_root);

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto RemoveHelper(std::unique_ptr<TrieNode> node, std::string_view key, size_t index) -> std::unique_ptr<TrieNode> {
  if (index == key.size()) {
    if (node->children_.empty()) {
      return nullptr;
    }
    return std::make_unique<TrieNode>(node->children_);
  }

  std::unique_ptr<TrieNode> child;
  auto c = key.at(index);

  auto search = node->children_.find(c);
  if (search != node->children_.end()) {
    child = search->second->Clone();
    child = RemoveHelper(std::move(child), key, index + 1);
  } else {
    return node;
  }

  if (child == nullptr) {
    node->children_.erase(search);
    if (node->children_.empty() && !(node->is_value_node_)) {
      return nullptr;
    }
    return node;
  }

  node->children_[c] = std::move(child);
  return node;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (root_ == nullptr) {
    return Trie(nullptr);
  }

  auto root = root_->Clone();
  root = RemoveHelper(std::move(root), key, 0);
  return Trie(std::move(root));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;
// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;
}  // namespace bustub
