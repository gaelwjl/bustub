#include "primer/trie.h"
#include <sys/types.h>
#include <cstddef>
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

// some helper functions
namespace {

auto Search(const TrieNode *node, std::string_view key, uint64_t index) -> const TrieNode * {
  if (index == key.size()) {
    if (node->is_value_node_) {
      return node;
    }
    return nullptr;
  }
  auto next_node = node->children_.find(key[index]);
  if (next_node == node->children_.end()) {
    return nullptr;
  }
  return Search(next_node->second.get(), key, index + 1);
}

// old_node can be nullptr
template <class T>
auto CopyCreateNewNode(const std::shared_ptr<const TrieNode> &old_node, bool has_value, std::shared_ptr<T> value)
    -> std::shared_ptr<TrieNode> {
  if (old_node == nullptr && has_value) {
    return std::make_shared<TrieNodeWithValue<T>>(value);
  }
  if (old_node == nullptr && !has_value) {
    return std::make_shared<TrieNode>();
  }
  if (old_node != nullptr && has_value) {
    return std::make_shared<TrieNodeWithValue<T>>(old_node->children_, value);
  }
  return old_node->Clone();
}

// return'ed result won't be empty
template <class T>
auto Insert(const std::shared_ptr<const TrieNode> &node, std::string_view key, uint64_t index, std::shared_ptr<T> value)
    -> std::shared_ptr<TrieNode> {
  if (index == key.size()) {
    return CopyCreateNewNode(node, true, value);
  }
  auto child = Insert(node == nullptr || !node->children_.count(key[index]) ? nullptr : node->children_.at(key[index]),
                      key, index + 1, value);
  auto new_node = CopyCreateNewNode(node, false, value);
  new_node->children_[key[index]] = child;
  return new_node;
}

// return'ed result won't be empty
auto RemoveRecursive(const std::shared_ptr<const TrieNode> &node, std::string_view key, uint64_t index)
    -> std::shared_ptr<TrieNode> {
  if (node == nullptr) {
    return nullptr;
  }
  if (index == key.size()) {
    if (node->children_.empty()) {
      return nullptr;
    }
    return std::make_shared<TrieNode>(node->children_);
  }
  auto child = RemoveRecursive(node->children_.count(key[index]) == 0 ? nullptr : node->children_.at(key[index]), key,
                               index + 1);
  std::shared_ptr<TrieNode> new_node = node->Clone();
  if (child != nullptr) {
    new_node->children_[key[index]] = child;
  } else {
    new_node->children_.erase(key[index]);
  }
  return new_node;
}

}  // namespace

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (root_ == nullptr) {
    return nullptr;
  }
  auto node = Search(root_.get(), key, 0);
  if (!node || !node->is_value_node_) {
    return nullptr;
  }
  auto casted = dynamic_cast<const TrieNodeWithValue<T> *>(node);
  if (!casted) {
    return nullptr;
  }
  return casted->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  auto value_ptr = std::make_shared<T>(std::move(value));
  auto node = Insert(root_, key, 0, value_ptr);
  return Trie(node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto node = RemoveRecursive(root_, key, 0);
  return Trie(std::move(node));
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
