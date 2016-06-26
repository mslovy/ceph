// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015-2016 Ning Yao <yaoning@unitedstack.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_BOUNDED_LOSSY_INTERVAL_SET_H
#define CEPH_BOUNDED_LOSSY_INTERVAL_SET_H

#include <iterator>
#include <map>
#include <ostream>

#include "encoding.h"
#include "interval_set.h"

#ifndef MIN
# define MIN(a,b)  ((a)<=(b) ? (a):(b))
#endif
#ifndef MAX
# define MAX(a,b)  ((a)>=(b) ? (a):(b))
#endif

#define MAX_NUM_INTERVALS 10

template<typename T>
class bounded_lossy_interval_set {
 public:

  class const_iterator;

  class iterator : public std::iterator <std::forward_iterator_tag, T>
  {
    public:
        explicit iterator(typename interval_set<T>::iterator iter)
          : _iter(iter)
        { }

        // For the copy constructor and assignment operator, the compiler-generated functions, which
        // perform simple bitwise copying, should be fine.

        bool operator==(const iterator& rhs) const {
          return (_iter == rhs._iter);
        }

        bool operator!=(const iterator& rhs) const {
          return (_iter != rhs._iter);
        }

        // Dereference this iterator to get a pair.
        std::pair < T, T > &operator*() {
                return *_iter;
        }

        // Return the interval start.
        T get_start() const {
                return _iter->get_start();
        }

        // Return the interval length.
        T get_len() const {
                return _iter->get_len();
        }

        // Set the interval length.
        void set_len(T len) {
                _iter->set_len(len);
        }

        // Preincrement
        iterator &operator++()
        {
                ++_iter;
                return *this;
        }

        // Postincrement
        iterator operator++(int)
        {
                iterator prev(_iter);
                ++_iter;
                return prev;
        }

    friend class bounded_lossy_interval_set<T>::const_iterator;

    protected:
        typename interval_set<T>::iterator _iter;
    friend class bounded_lossy_interval_set<T>;
  };

  class const_iterator : public std::iterator <std::forward_iterator_tag, T>
  {
    public:
        explicit const_iterator(typename interval_set<T>::const_iterator iter)
          : _iter(iter)
        { }

        const_iterator(const iterator &i)
	  : _iter(i._iter)
        { }

        // For the copy constructor and assignment operator, the compiler-generated functions, which
        // perform simple bitwise copying, should be fine.

        bool operator==(const const_iterator& rhs) const {
          return (_iter == rhs._iter);
        }

        bool operator!=(const const_iterator& rhs) const {
          return (_iter != rhs._iter);
        }

        // Dereference this iterator to get a pair.
        std::pair < T, T > operator*() const {
                return *_iter;
        }

        // Return the interval start.
        T get_start() const {
                return _iter->get_start();
        }

        // Return the interval length.
        T get_len() const {
                return _iter->get_len();
        }

        // Preincrement
        const_iterator &operator++()
        {
                ++_iter;
                return *this;
        }

        // Postincrement
        const_iterator operator++(int)
        {
                const_iterator prev(_iter);
                ++_iter;
                return prev;
        }

    protected:
        typename interval_set<T>::const_iterator _iter;
  };

  bounded_lossy_interval_set(uint32_t max = MAX_NUM_INTERVALS) : max_num_intervals(max) {}
  bounded_lossy_interval_set(const interval_set<T>& other) {
    max_num_intervals = MAX_NUM_INTERVALS;
    m = other;
  }
  bounded_lossy_interval_set<T>& operator=(const interval_set<T>& other) {
    max_num_intervals = MAX_NUM_INTERVALS;
    m = other;
    return this;
  }

  const interval_set<T>& get_intervals() const {
    return m;
  }

  int num_intervals() const
  {
    return m.num_intervals();
  }

  typename bounded_lossy_interval_set<T>::iterator begin() {
    return typename bounded_lossy_interval_set<T>::iterator(m.begin());
  }

  typename bounded_lossy_interval_set<T>::iterator lower_bound(T start) {
    return typename bounded_lossy_interval_set<T>::iterator(m.lower_bound(start));
  }

  typename bounded_lossy_interval_set<T>::iterator end() {
    return typename bounded_lossy_interval_set<T>::iterator(m.end());
  }

  typename bounded_lossy_interval_set<T>::const_iterator begin() const {
    return typename bounded_lossy_interval_set<T>::const_iterator(m.begin());
  }

  typename bounded_lossy_interval_set<T>::const_iterator lower_bound(T start) const {
    return typename bounded_lossy_interval_set<T>::const_iterator(m.lower_bound(start));
  }

  typename bounded_lossy_interval_set<T>::const_iterator end() const {
    return typename bounded_lossy_interval_set<T>::const_iterator(m.end());
  }

 private:
  void trim() {
    while (m.num_intervals() > max_num_intervals) {
      typename interval_set<T>::iterator smallest = m.begin();
      if (smallest == m.end())
	break;
      for (typename interval_set<T>::iterator it = m.begin(); it != m.end(); ++it) {
	if (it.get_len() < smallest.get_len())
	  smallest = it;
      }
      m.erase(smallest);
    }
  }

 public:
  bool operator==(const bounded_lossy_interval_set& other) const {
    return m == other.m;
  }

  int size() const {
    return m.size();
  }

  void encode(bufferlist& bl) const {
    ::encode(m, bl);
  }
  void encode_nohead(bufferlist& bl) const {
    ::encode_nohead(m, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(m, bl);
  }
  void decode_nohead(int n, bufferlist::iterator& bl) {
    ::decode_nohead(n, m, bl);
  }

  void clear() {
    m.clear();
  }

  bool contains(T i, T *pstart=0, T *plen=0) const {
    return m.contains(i, pstart, plen);
  }
  bool contains(T start, T len) const {
    return m.contains(start, len);
  }
  bool intersects(T start, T len) const {
    return m.intersects(start, len);
  }

  // outer range of set
  bool empty() const {
    return m.empty();
  }
  T range_start() const {
    return m.range_start();
  }
  T range_end() const {
    return m.range_end();
  }

  // interval start after p (where p not in set)
  bool starts_after(T i) const {
    return m.starts_after(i);
  }
  T start_after(T i) const {
    return m.start_after(i);
  }

  // interval end that contains start
  T end_after(T start) const {
    return m.end_after(start);
  }
  
  void insert(T val) {
    insert(val, 1);
  }

  void insert(T start, T len, T *pstart=0, T *plen=0) {
    m.insert(start, len, pstart, plen);
    trim();
  }

  void swap(bounded_lossy_interval_set<T>& other) {
    m.swap(other.m);
  }    
  
  void erase(iterator &i) {
    m.erase(i._iter);
  }

  void erase(T val) {
    erase(val, 1);
  }

  void erase(T start, T len) {
    m.erase(start, len);
    trim();
  }

  void subtract(const bounded_lossy_interval_set &a) {
    m.subtract(a.m);
    trim();
  }

  void insert(const bounded_lossy_interval_set &a) {
    m.insert(a.m);
    trim();
  }


  void intersection_of(const bounded_lossy_interval_set &a, const bounded_lossy_interval_set &b) {
    m.intersection_of(a.m, b.m);
    trim();
  }
  void intersection_of(const bounded_lossy_interval_set& b) {
    bounded_lossy_interval_set a;
    swap(a);
    intersection_of(a, b);
  }

  void union_of(const bounded_lossy_interval_set &a, const bounded_lossy_interval_set &b) {
    m.union_of(a.m, b.m);
    trim();
  }
  void union_of(const bounded_lossy_interval_set &b) {
    bounded_lossy_interval_set a;
    swap(a);    
    union_of(a, b);
  }

  bool subset_of(const bounded_lossy_interval_set &big) const {
    return m.subset_of(big.m);
  }  

  /*
   * build a subset of @other, starting at or after @start, and including
   * @len worth of values, skipping holes.  e.g.,
   *  span_of([5~10,20~5], 8, 5) -> [8~2,20~3]
   */
  void span_of(const bounded_lossy_interval_set &other, T start, T len) {
    m.span_of(other.m, start, len);
    trim();
  }

  template<class U>
  friend std::ostream& operator<<(std::ostream& out, const bounded_lossy_interval_set<U> &s);
private:
  // data
  uint32_t max_num_intervals;
  interval_set<T> m;
};


template<class T>
inline std::ostream& operator<<(std::ostream& out, const bounded_lossy_interval_set<T> &s) {
  out << "max_num_intervals " << s.max_num_intervals << " " << s.m;
  return out;
}

template<class T>
inline void encode(const bounded_lossy_interval_set<T>& s, bufferlist& bl)
{
  s.encode(bl);
}
template<class T>
inline void decode(bounded_lossy_interval_set<T>& s, bufferlist::iterator& p)
{
  s.decode(p);
}

#endif
