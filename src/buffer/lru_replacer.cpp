/**
 * LRU implementation
 */
#include <cassert>
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template<typename T> LRUReplacer<T>::LRUReplacer() {
}

template<typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template<typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(_mtx);
  auto it = _id2node.find(value);
  if (it!=_id2node.end()) {
    _list.erase(it->second);
  }
  _list.insert(_list.begin(), value);
  _id2node[value] = _list.begin();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template<typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> guard(_mtx);
  if (_id2node.empty()) {
    return false;
  }
  assert(!_list.empty());
  value = _list.back();
  _list.pop_back();
  _id2node.erase(value);

  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template<typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> guard(_mtx);

  auto it = _id2node.find(value);
  if (it==_id2node.end()) {
    return false;
  }

  _list.erase(it->second);
  _id2node.erase(value);

  return true;
}

template<typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> guard(_mtx);
  return _id2node.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
