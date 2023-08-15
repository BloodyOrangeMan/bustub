#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  // (1) Take the root lock, get the root, and release the root lock.
  Trie root;
  {
      std::lock_guard<std::mutex> lock(root_lock_);
      root = root_;
  }

  // (2) Lookup the value in the trie.
  auto value = root.Get<T>(key);
  if (!value) {
      // not found
      return std::nullopt;
  }

  // (3) If the value is found, return a ValueGuard object.
  return std::make_optional(ValueGuard(std::move(root), *value));
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
    std::unique_lock write_guard(write_lock_);

    // get the old root
    Trie root;
    {
        std::lock_guard<std::mutex> lock(root_lock_);
        root = root_;
    }

    // do the operation
    root = root.Put(key, std::move(value));

    // swap in old root
    {
        std::lock_guard<std::mutex> lock(root_lock_);
        root_ = std::move(root);
    }
}

void TrieStore::Remove(std::string_view key) {
    std::unique_lock write_guard(write_lock_);

    // get the old root
    Trie root;
    {
        std::lock_guard<std::mutex> lock(root_lock_);
        root = root_;
    }

    // do the operation
    root = root.Remove(key);

    // swap in old root
    {
        std::lock_guard<std::mutex> lock(root_lock_);
        root_ = std::move(root);
    }
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub
