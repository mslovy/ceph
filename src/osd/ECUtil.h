// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef ECUTIL_H
#define ECUTIL_H

#include <map>
#include <set>

#include "include/memory.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer.h"
#include "include/assert.h"
#include "include/types.h"
#include "include/encoding.h"
#include "common/Formatter.h"
#include "common/debug.h"

namespace ECUtil {

const uint64_t CHUNK_ALIGNMENT = 64;
const uint64_t CHUNK_INFO = 8;
const uint64_t CHUNK_PADDING = 8;
const uint64_t CHUNK_OVERHEAD = 16; // INFO + PADDING

class stripe_info_t {
  const uint64_t stripe_size;
  const uint64_t stripe_width;
  const uint64_t chunk_size;
public:
  stripe_info_t(uint64_t stripe_size, uint64_t stripe_width)
    : stripe_size(stripe_size), stripe_width(stripe_width),
      chunk_size(stripe_width / stripe_size) {
    assert(stripe_width % stripe_size == 0);
  }
  uint64_t get_stripe_width() const {
    return stripe_width;
  }
  uint64_t get_chunk_size() const {
    return chunk_size;
  }
  uint64_t logical_to_prev_chunk_offset(uint64_t offset) const {
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t logical_to_next_chunk_offset(uint64_t offset) const {
    return ((offset + stripe_width - 1)/ stripe_width) * chunk_size;
  }
  uint64_t logical_to_prev_stripe_offset(uint64_t offset) const {
    return offset - (offset % stripe_width);
  }
  uint64_t logical_to_next_stripe_offset(uint64_t offset) const {
    return ((offset % stripe_width) ?
      (offset - (offset % stripe_width) + stripe_width) :
      offset);
  }
  uint64_t aligned_logical_offset_to_chunk_offset(uint64_t offset) const {
    assert(offset % stripe_width == 0);
    return (offset / stripe_width) * chunk_size;
  }
  uint64_t aligned_chunk_offset_to_logical_offset(uint64_t offset) const {
    assert(offset % chunk_size == 0);
    return (offset / chunk_size) * stripe_width;
  }
  pair<uint64_t, uint64_t> aligned_offset_len_to_chunk(
    pair<uint64_t, uint64_t> in) const {
    return make_pair(
      aligned_logical_offset_to_chunk_offset(in.first),
      aligned_logical_offset_to_chunk_offset(in.second));
  }
  pair<uint64_t, uint64_t> offset_len_to_stripe_bounds(
    pair<uint64_t, uint64_t> in) const {
    uint64_t off = logical_to_prev_stripe_offset(in.first);
    uint64_t len = logical_to_next_stripe_offset(
      (in.first - off) + in.second);
    return make_pair(off, len);
  }
};

int decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out);

int decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out);

int encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out);

class HashInfo {
  uint64_t total_chunk_size;
  vector<uint32_t> cumulative_shard_hashes;
public:
  HashInfo() : total_chunk_size(0) {}
  HashInfo(unsigned num_chunks)
  : total_chunk_size(0),
    cumulative_shard_hashes(num_chunks, -1) {}
  void append(uint64_t old_size, map<int, bufferlist> &to_append) {
    assert(to_append.size() == cumulative_shard_hashes.size());
    assert(old_size == total_chunk_size);
    uint64_t size_to_append = to_append.begin()->second.length();
    for (map<int, bufferlist>::iterator i = to_append.begin();
	 i != to_append.end();
	 ++i) {
      assert(size_to_append == i->second.length());
      assert((unsigned)i->first < cumulative_shard_hashes.size());
      uint32_t new_hash = i->second.crc32c(cumulative_shard_hashes[i->first]);
      cumulative_shard_hashes[i->first] = new_hash;
    }
    total_chunk_size += size_to_append;
  }
  void clear() {
    total_chunk_size = 0;
    cumulative_shard_hashes = vector<uint32_t>(
      cumulative_shard_hashes.size(),
      -1);
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<HashInfo*>& o);
  uint32_t get_chunk_hash(int shard) const {
    assert((unsigned)shard < cumulative_shard_hashes.size());
    return cumulative_shard_hashes[shard];
  }
  uint64_t get_total_chunk_size() const {
    return total_chunk_size;
  }
};
typedef ceph::shared_ptr<HashInfo> HashInfoRef;

bool is_hinfo_key_string(const string &key);
const string &get_hinfo_key();

class CompactInfo {
  uint64_t total_origin_chunk_size;
  uint32_t stripe_width;
  uint32_t chunk_size;
  map<string, uint32_t> attrs;
  map<uint8_t, vector<uint32_t> > stripe_compact_range;
public:
  CompactInfo() : total_origin_chunk_size(0) {}
  CompactInfo(uint8_t num_chunks, uint32_t stripe_width, uint32_t chunk_size)
  : total_origin_chunk_size(0), stripe_width(stripe_width), chunk_size(chunk_size) {
    for (uint8_t i = 0; i < num_chunks; i++) {
      stripe_compact_range[(shard_id_t)i];
    }
  }
  uint32_t get_stripe_width() const {
    return stripe_width;
  }
  uint32_t get_chunk_size() const {
    return chunk_size;
  }
  void append(uint64_t old_size, map<uint8_t, vector<uint32_t> > &to_append,
    uint64_t append_size) {
    assert(to_append.size() == stripe_compact_range.size());
    assert(old_size == total_origin_chunk_size);
    uint64_t size_to_append = to_append.begin()->second.size();
    for (map<uint8_t, vector<uint32_t> >::iterator i = to_append.begin();
	 i != to_append.end();
	 ++i) {
      assert(size_to_append == i->second.size());
      assert(i->first < stripe_compact_range.size());
      stripe_compact_range[i->first].insert(stripe_compact_range[i->first].end(),
        i->second.begin(), i->second.end());
    }
    total_origin_chunk_size += append_size;
  }
  void clear() {
    total_origin_chunk_size = 0;
    unsigned num_chunks = stripe_compact_range.size();
    for (uint8_t i = 0; i < num_chunks; i++) {
      stripe_compact_range[i].clear();
    }
  }
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  void dump(Formatter *f) const;
  static void generate_test_instances(list<CompactInfo*>& o);

  const vector<uint32_t>& get_chunk_compact_range(uint8_t shard) const {
    map<uint8_t, vector<uint32_t> >::const_iterator it = stripe_compact_range.find(shard);
    assert(it != stripe_compact_range.end());
    return it->second;
  }

  pair<uint32_t, uint32_t> convert_compact_ranges(uint8_t shard,
    uint32_t offset, uint32_t len) {
    assert(offset % chunk_size == 0);
    assert(len % chunk_size == 0);
    const vector<uint32_t>& ranges = get_chunk_compact_range(shard);
    if (ranges.empty()) {
      return make_pair(0, 0);
    }
    uint32_t start_chunk = 0;
    if (offset) {
      assert((offset / chunk_size - 1) < ranges.size());
      start_chunk = ranges[offset / chunk_size - 1];
    }

    uint32_t end_chunk = 0;
    if ((offset + len) / chunk_size > 1)
      end_chunk = (offset + len) / chunk_size - 1;
    if (end_chunk >= ranges.size())
      end_chunk = ranges.size() - 1;
    assert(ranges[end_chunk] >= start_chunk);
    return make_pair(start_chunk, ranges[end_chunk] - start_chunk);
  }

  uint32_t conver_compact_min_range(uint8_t shard, uint32_t offset) {
    if (offset == 0)
      return 0;
    const vector<uint32_t>& ranges = get_chunk_compact_range(shard);
    for (unsigned i = 0; i < ranges.size(); i++) {
      if (offset < ranges[i]) {
        if (i)
          return (i - 1);
        else
          assert(false);
      } else if (offset == ranges[i])
        return i;
    }
    return ranges.size() - 1;
  }

  uint32_t conver_compact_range(uint8_t shard, uint32_t offset) {
    if (offset == 0)
      return 0;
    const vector<uint32_t>& ranges = get_chunk_compact_range(shard);
    for (unsigned i = 0; i < ranges.size(); i++) {
      if (offset == ranges[i])
        return  (i + 1);
    }
    assert(false);
  }

  void decompact(uint8_t shard, uint32_t offset, uint32_t len,
    const bufferlist& src, bufferlist& dst, bool whole_decode);

  uint64_t get_total_chunk_size(uint8_t shard) const {
    return get_chunk_compact_range(shard).back();
  }

  uint64_t get_total_origin_chunk_size() const {
    return total_origin_chunk_size;
  }

};
typedef ceph::shared_ptr<CompactInfo> CompactInfoRef;

bool is_cinfo_key_string(const string &key);
const string &get_cinfo_key();

}
WRITE_CLASS_ENCODER(ECUtil::HashInfo)
WRITE_CLASS_ENCODER(ECUtil::CompactInfo)
#endif
