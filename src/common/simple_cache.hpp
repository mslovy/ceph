// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_SIMPLECACHE_H
#define CEPH_SIMPLECACHE_H

#include <map>
#include <list>
#include <memory>
#include "common/Mutex.h"
#include "common/Cond.h"

template <class K, class V>
class SimpleLRU {
  Mutex lock;
  size_t max_size;
  map<K, typename list<pair<K, V> >::iterator> contents;
  list<pair<K, V> > lru;
  map<K, V> pinned;

  void trim_cache() {
    while (lru.size() > max_size) {
      contents.erase(lru.back().first);
      lru.pop_back();
    }
  }

  void _add(K key, V value) {
    lru.push_front(make_pair(key, value));
    contents[key] = lru.begin();
    trim_cache();
  }

public:
  SimpleLRU(size_t max_size) : lock("SimpleLRU::lock"), max_size(max_size) {}

  void pin(K key, V val) {
    Mutex::Locker l(lock);
    pinned.insert(make_pair(key, val));
  }

  void clear_pinned(K e) {
    Mutex::Locker l(lock);
    for (typename map<K, V>::iterator i = pinned.begin();
	 i != pinned.end() && i->first <= e;
	 pinned.erase(i++)) {
      if (!contents.count(i->first))
	_add(i->first, i->second);
      else
	lru.splice(lru.begin(), lru, contents[i->first]);
    }
  }

  void clear(K key) {
    Mutex::Locker l(lock);
    typename map<K, typename list<pair<K, V> >::iterator>::iterator i =
      contents.find(key);
    if (i == contents.end())
      return;
    lru.erase(i->second);
    contents.erase(i);
  }

  K last_key() {
    Mutex::Locker l(lock);
    return lru.back().first;
  }
  
  list<K> last_N_keys(int n) {
    Mutex::Locker l(lock);
    list<K> keys;
    if (n <= 0)
      return keys;
    for (typename list<pair<K, V> >::reverse_iterator p = lru.rbegin();
         p != lru.rend();
         ++p) {
      keys.push_back(p->first);
      if (keys.size() >= n)
        break;
    }
    return keys;
  }

  list<K> get_range_keys(int offset, int len) {
    Mutex::Locker l(lock);
    list<K> keys;
    if (len <= 0)
      return keys;
    if (offset < 0)
      return last_N_keys(len);

    typename list<pair<K, V> >::reverse_iterator p;
    for (p = lru.rbegin(); p != lru.rend(); ++p) {
      if (offset-- == 0)
        break;
    }
    for (; p != lru.rend(); ++p) {
      keys.push_back(p->first);
      if (keys.size() >= len)
        break;
    }
    return keys;
  }

  void clear() {
    Mutex::Locker l(lock);
    lru.clear();
    contents.clear();
  }

  uint32_t size() {
    Mutex::Locker l(lock);
    return lru.size();
  }

  void set_size(size_t new_size) {
    Mutex::Locker l(lock);
    max_size = new_size;
    trim_cache();
  }

  bool lookup(K key, V *out, bool reorder = true) {
    Mutex::Locker l(lock);
    typename list<pair<K, V> >::iterator loc = contents.count(key) ?
      contents[key] : lru.end();
    if (loc != lru.end()) {
      if (out)
        *out = loc->second;
      if (reorder)
        lru.splice(lru.begin(), lru, loc);
      return true;
    }
    if (pinned.count(key)) {
      if (out)
        *out = pinned[key];
      return true;
    }
    return false;
  }

  void add(K key, V value) {
    Mutex::Locker l(lock);
    _add(key, value);
  }
};

#endif
