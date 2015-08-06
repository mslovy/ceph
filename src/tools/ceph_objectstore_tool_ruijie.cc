// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/scoped_ptr.hpp>

#include <stdlib.h>

#include "common/Formatter.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/xattr.h"
#include "include/encoding.h"
#include "os/chain_xattr.h"
#include "global/global_init.h"

#include "os/ObjectStore.h"
#include "os/FileStore.h"

#include "osd/PGLog.h"
#include "osd/OSD.h"
#include "osd/PG.h"

#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"

#include "include/rados/librados.hpp"

using namespace std;

static void get_attrname(const char *name, char *buf, int len)
{
  snprintf(buf, len, "user.ceph.%s", name);
}

int _fgetattr(int fd, const char *name, bufferptr& bp)
{
  char val[100];
  int l = chain_fgetxattr(fd, name, val, sizeof(val));
  if (l >= 0) {
    bp = buffer::create(l);
    memcpy(bp.c_str(), val, l);
  } else if (l == -ERANGE) {
    l = chain_fgetxattr(fd, name, 0, 0);
    if (l > 0) {
      bp = buffer::create(l);
      l = chain_fgetxattr(fd, name, bp.c_str(), l);
    }
  }
  assert(l != -EIO);
  return l;
}

int getattr(const char* filename, const char *name, bufferptr &bp)
{
  int fd = open(filename, O_RDWR, 0644);
  if (fd < 0) {
    return -EIO;
  }
  char n[CHAIN_XATTR_MAX_NAME_LEN];
  get_attrname(name, n, CHAIN_XATTR_MAX_NAME_LEN);
  int r = _fgetattr(fd, n, bp);
  if (r < 0) {
    close(fd);
    return -EIO;
  }
  close(fd);
  return 0;
}

int _fsetattrs(string filename, string name, bufferptr bp)
{ 
    int fd = open(filename.c_str(), O_RDWR, 0644);
    if (fd < 0) {
      return -EIO;
    }
    char n[CHAIN_XATTR_MAX_NAME_LEN];
    get_attrname(name.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);
    const char *val;
    if (bp.length())
      val = bp.c_str();
    else
      val = "";
    // ??? Why do we skip setting all the other attrs if one fails?
    int r = chain_fsetxattr(fd, n, val, bp.length());
    if (r < 0) {
      close(fd);
      derr << "FileStore::_setattrs: chain_setxattr returned " << r << dendl;
      return r;
    }
    close(fd);
    return 0;
}

int main(int argc, char **argv)
{

  bufferlist out;
  bufferptr bp;
  std::string filename = argv[1];

  std::cout << filename << std::endl;

  int r = getattr(filename.c_str(),
    OI_ATTR,
    bp);
  if (r >= 0) {
    out.clear();
    out.push_back(bp);
  }
  
  object_info_t oi(out);
  std::cout << oi << std::endl;
  
  struct stat st;
  r = ::stat(filename.c_str(), &st);
  oi.size = st.st_size; 
  //oi.size = 1024; 

  bufferlist bl;
  ::encode(oi, bl);
 
  r = _fsetattrs(filename, OI_ATTR, bufferptr(bl.c_str(), bl.length()));
  assert(r == 0);
 
  bufferptr bpp;
  r = getattr(filename.c_str(),
    OI_ATTR,
    bpp);
  if (r >= 0) {
    out.clear();
    out.push_back(bpp);
  }

  object_info_t oii(out);
  std::cout << oii << std::endl;

  return 0;
}
