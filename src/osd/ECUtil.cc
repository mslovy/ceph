// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#include <errno.h>
#include "include/encoding.h"
#include "ECUtil.h"

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd "

int ECUtil::decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  bufferlist *out) {

  uint64_t total_chunk_size = to_decode.begin()->second.length();

  assert(to_decode.size());
  assert(total_chunk_size % sinfo.get_chunk_size() == 0);
  assert(out);
  assert(out->length() == 0);

  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == total_chunk_size);
  }

  if (total_chunk_size == 0)
    return 0;

  for (uint64_t i = 0; i < total_chunk_size; i += sinfo.get_chunk_size()) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
    }
    bufferlist bl;
    int r = ec_impl->decode_concat(chunks, &bl);
    assert(bl.length() == sinfo.get_stripe_width());
    assert(r == 0);
    out->claim_append(bl);
  }
  return 0;
}

int ECUtil::decode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  map<int, bufferlist> &to_decode,
  map<int, bufferlist*> &out) {

  uint64_t total_chunk_size = to_decode.begin()->second.length();

  assert(to_decode.size());
  assert(total_chunk_size % sinfo.get_chunk_size() == 0);

  for (map<int, bufferlist>::iterator i = to_decode.begin();
       i != to_decode.end();
       ++i) {
    assert(i->second.length() == total_chunk_size);
  }

  if (total_chunk_size == 0)
    return 0;

  set<int> need;
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second);
    assert(i->second->length() == 0);
    need.insert(i->first);
  }

  for (uint64_t i = 0; i < total_chunk_size; i += sinfo.get_chunk_size()) {
    map<int, bufferlist> chunks;
    for (map<int, bufferlist>::iterator j = to_decode.begin();
	 j != to_decode.end();
	 ++j) {
      chunks[j->first].substr_of(j->second, i, sinfo.get_chunk_size());
    }
    map<int, bufferlist> out_bls;
    int r = ec_impl->decode(need, chunks, &out_bls);
    assert(r == 0);
    for (map<int, bufferlist*>::iterator j = out.begin();
	 j != out.end();
	 ++j) {
      assert(out_bls.count(j->first));
      assert(out_bls[j->first].length() == sinfo.get_chunk_size());
      j->second->claim_append(out_bls[j->first]);
    }
  }
  for (map<int, bufferlist*>::iterator i = out.begin();
       i != out.end();
       ++i) {
    assert(i->second->length() == total_chunk_size);
  }
  return 0;
}

int ECUtil::encode(
  const stripe_info_t &sinfo,
  ErasureCodeInterfaceRef &ec_impl,
  bufferlist &in,
  const set<int> &want,
  map<int, bufferlist> *out) {

  uint64_t logical_size = in.length();

  assert(logical_size % sinfo.get_stripe_width() == 0);
  assert(out);
  assert(out->empty());

  if (logical_size == 0)
    return 0;

  for (uint64_t i = 0; i < logical_size; i += sinfo.get_stripe_width()) {
    map<int, bufferlist> encoded;
    bufferlist buf;
    buf.substr_of(in, i, sinfo.get_stripe_width());
    int r = ec_impl->encode(want, buf, &encoded);
    assert(r == 0);
    for (map<int, bufferlist>::iterator i = encoded.begin();
	 i != encoded.end();
	 ++i) {
      assert(i->second.length() == sinfo.get_chunk_size());
      (*out)[i->first].claim_append(i->second);
    }
  }

  for (map<int, bufferlist>::iterator i = out->begin();
       i != out->end();
       ++i) {
    assert(i->second.length() % sinfo.get_chunk_size() == 0);
    assert(
      sinfo.aligned_chunk_offset_to_logical_offset(i->second.length()) ==
      logical_size);
  }
  return 0;
}

void ECUtil::HashInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(total_chunk_size, bl);
  ::encode(cumulative_shard_hashes, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::HashInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(total_chunk_size, bl);
  ::decode(cumulative_shard_hashes, bl);
  DECODE_FINISH(bl);
}

void ECUtil::HashInfo::dump(Formatter *f) const
{
  f->dump_unsigned("total_chunk_size", total_chunk_size);
  f->open_object_section("cumulative_shard_hashes");
  for (unsigned i = 0; i != cumulative_shard_hashes.size(); ++i) {
    f->open_object_section("hash");
    f->dump_unsigned("shard", i);
    f->dump_unsigned("hash", cumulative_shard_hashes[i]);
    f->close_section();
  }
  f->close_section();
}

void ECUtil::HashInfo::generate_test_instances(list<HashInfo*>& o)
{
  o.push_back(new HashInfo(3));
  {
    bufferlist bl;
    bl.append_zero(20);
    map<int, bufferlist> buffers;
    buffers[0] = bl;
    buffers[1] = bl;
    buffers[2] = bl;
    o.back()->append(0, buffers);
    o.back()->append(20, buffers);
  }
  o.push_back(new HashInfo(4));
}

const string HINFO_KEY = "hinfo_key";

bool ECUtil::is_hinfo_key_string(const string &key)
{
  return key == HINFO_KEY;
}

const string &ECUtil::get_hinfo_key()
{
  return HINFO_KEY;
}

void ECUtil::CompactInfo::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(total_origin_chunk_size, bl);
  ::encode(stripe_width, bl);
  ::encode(chunk_size, bl);
  ::encode(attrs, bl);
  ::encode(stripe_compact_range, bl);
  ENCODE_FINISH(bl);
}

void ECUtil::CompactInfo::decode(bufferlist::iterator &bl)
{
  DECODE_START(1, bl);
  ::decode(total_origin_chunk_size, bl);
  ::decode(stripe_width, bl);
  ::decode(chunk_size, bl);
  ::decode(attrs, bl);
  ::decode(stripe_compact_range, bl);
  DECODE_FINISH(bl);
}

void ECUtil::CompactInfo::dump(Formatter *f) const
{
  f->dump_unsigned("total_origin_chunk_size", total_origin_chunk_size);
  f->dump_unsigned("stripe_width", stripe_width);
  f->dump_unsigned("chunk_size", chunk_size);
  f->open_object_section("attrs");
  for (map<string, uint32_t>::const_iterator it = attrs.begin();
      it != attrs.end();
      ++it) {
    f->open_object_section("attr");
    f->dump_string("attr", it->first);
    f->dump_unsigned("value", it->second);
    f->close_section();
  }
  f->close_section();

  f->open_object_section("stripe_compact_range");
  for (map<uint8_t, vector<uint32_t> >::const_iterator it = stripe_compact_range.begin();
       it != stripe_compact_range.end();
       ++it) {
    f->open_object_section("shards_ranges");
    f->dump_unsigned("shard", it->first);
    f->open_object_section("ranges");
    for (unsigned i = 0; i != it->second.size(); ++i) {
      f->open_object_section("range");
      f->dump_unsigned("chunk", i);
      f->dump_unsigned("range", it->second[i]);
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
}

void ECUtil::CompactInfo::decompact(uint8_t shard, uint32_t offset, uint32_t len,
  const bufferlist& src, bufferlist& dst, bool whole_decode)
{
    assert(src.length() <= len);
    uint32_t start_chunk = conver_compact_range(shard, offset);
    const vector<uint32_t>& ranges = get_chunk_compact_range(shard);
    ldout(g_ceph_context, 20) << __func__ << " shard " << (unsigned)(shard)
                              << " ranges " << ranges << dendl;
    uint32_t decode_step = 0;
    for (uint32_t step = 0; step < src.length(); step += decode_step) {
      bufferlist bl, dbl;
      decode_step = ranges[start_chunk];
      if (start_chunk) {
        decode_step -= ranges[start_chunk - 1];
      }
      if (!whole_decode && step + decode_step > src.length()) {
        ldout(g_ceph_context, 20) << __func__ << " shard " << (unsigned)(shard) << " step " << step
                                  << " decode_step " << decode_step << " length " << src.length() << dendl;
        break;
      }
      assert(step + decode_step <= src.length());
      bl.substr_of(src, step, decode_step);
      bl.decompress(buffer::ALG_LZ4, dbl, chunk_size);
      dst.claim_append(dbl);
      start_chunk++;
    }
    assert(dst.length() % chunk_size == 0);
}

void ECUtil::CompactInfo::generate_test_instances(list<CompactInfo*>& o)
{
	
}

const string HCOMPACT_KEY = "cinfo_key";

bool ECUtil::is_cinfo_key_string(const string &key)
{
  return key == HCOMPACT_KEY;
}

const string &ECUtil::get_cinfo_key()
{
  return HCOMPACT_KEY;
}

