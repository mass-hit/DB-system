#include "primer/trie.h"

#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  std::shared_ptr<const TrieNode> now = root_;
  for (char ch : key) {
    auto to = now->children_.find(ch);
    if (to == now->children_.end()) {
      return nullptr;
    }
    now = to->second;
  }
  if (now == nullptr) {
    return nullptr;
  }
  auto node_with_value = dynamic_cast<const TrieNodeWithValue<T> *>(now.get());
  if (node_with_value == nullptr) {
    return nullptr;
  }
  return node_with_value->value_.get();
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  std::shared_ptr<const TrieNode> new_root;
  std::shared_ptr<const TrieNode> now = root_;
  std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));
  if (key.empty()) {
    new_root = std::make_shared<const TrieNodeWithValue<T>>(root_->children_, value_ptr);
  } else {
    std::stack<std::pair<char, std::unique_ptr<TrieNode>>> st;
    for (char ch : key) {
      if (now != nullptr) {
        st.emplace(ch, now->Clone());
        if (auto to = now->children_.find(ch); to == now->children_.end()) {
          now = nullptr;
        } else {
          now = to->second;
        }
      } else {
        st.emplace(ch, nullptr);
      }
    }
    std::shared_ptr<const TrieNode> son;
    if (now != nullptr) {
      son = std::make_shared<const TrieNodeWithValue<T>>(now->children_, value_ptr);
    } else {
      son = std::make_shared<const TrieNodeWithValue<T>>(std::map<char, std::shared_ptr<const TrieNode>>(), value_ptr);
    }
    while (!st.empty()) {
      char ch = st.top().first;
      if (st.top().second == nullptr) {
        st.top().second = std::make_unique<TrieNode>();
      }
      st.top().second->children_[ch] = son;
      son = std::shared_ptr<const TrieNode>(std::move(st.top().second));
      st.pop();
    }
    new_root = son;
  }
  return Trie(new_root);
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (root_ == nullptr) {
    return Trie(root_);
  }
  std::shared_ptr<const TrieNode> now = root_;
  std::shared_ptr<const TrieNode> new_root;
  if (key.empty()) {
    new_root = std::make_shared<const TrieNode>(root_->children_);
  } else {
    std::stack<std::pair<char, std::unique_ptr<TrieNode>>> st;
    for (char ch : key) {
      st.emplace(ch, now->Clone());
      auto to = now->children_.find(ch);
      if (to == now->children_.end()) {
        return Trie(root_);
      }
      now = to->second;
    }
    std::shared_ptr<const TrieNode> son;
    if (now->children_.empty()) {
      son = nullptr;
    } else {
      son = std::make_shared<const TrieNode>(now->children_);
    }
    while (!st.empty()) {
      char ch = st.top().first;
      if (son == nullptr) {
        st.top().second->children_.erase(ch);
        if (st.top().second->children_.empty() && !st.top().second->is_value_node_) {
          st.top().second = nullptr;
        }
      } else {
        st.top().second->children_[ch] = son;
      }
      son = std::shared_ptr<const TrieNode>(std::move(st.top().second));
      st.pop();
    }
    new_root = son;
  }
  return Trie(new_root);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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