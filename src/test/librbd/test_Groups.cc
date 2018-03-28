// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "include/int_types.h"
#include "include/stringify.h"
#include "include/rados/librados.h"
#include "include/rbd/librbd.hpp"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/internal.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/WatchNotifyTypes.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequestWQ.h"
#include "test/librados/test.h"
#include "gtest/gtest.h"
#include <boost/assign/std/set.hpp>
#include <boost/assign/std/map.hpp>
#include <boost/bind.hpp>
#include <boost/scope_exit.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <vector>

using namespace ceph;
using namespace boost::assign;
using namespace librbd::watch_notify;

void register_test_groups() {
}

class TestGroup : public TestFixture {

};

TEST_F(TestGroup, group_create)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, "mygroup"));

  vector<string> groups;
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(1U, groups.size());
  ASSERT_EQ("mygroup", groups[0]);

  ASSERT_EQ(0, rbd.group_remove(ioctx, "mygroup"));

  groups.clear();
  ASSERT_EQ(0, rbd.group_list(ioctx, &groups));
  ASSERT_EQ(0U, groups.size());
}

TEST_F(TestGroup, add_image)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const char *group_name = "mycg";
  const char *image_name = "myimage";
  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, group_name));
  int order = 14;
  ASSERT_EQ(0, rbd.create2(ioctx, image_name, 65535,
	                   RBD_FEATURE_LAYERING, &order)); // Specified features make image of new format.

  ASSERT_EQ(0, rbd.group_image_add(ioctx, group_name, ioctx, image_name));

  vector<librbd::group_image_status_t> images;
  ASSERT_EQ(0, rbd.group_image_list(ioctx, group_name, &images));
  ASSERT_EQ(1U, images.size());
  ASSERT_EQ("myimage", images[0].name);
  ASSERT_EQ(ioctx.get_id(), images[0].pool);

  ASSERT_EQ(0, rbd.group_image_remove(ioctx, group_name, ioctx, image_name));

  images.clear();
  ASSERT_EQ(0, rbd.group_image_list(ioctx, group_name, &images));
  ASSERT_EQ(0U, images.size());
}

TEST_F(TestGroup, add_snapshot)
{
  librados::IoCtx ioctx;
  ASSERT_EQ(0, _rados.ioctx_create(_pool_name.c_str(), ioctx));

  const char *group_name = "snap_group";
  const char *image_name = "snap_image";
  const char *snap_name = "snap_snapshot";

  librbd::RBD rbd;
  ASSERT_EQ(0, rbd.group_create(ioctx, group_name));

  int order = 14;
  ASSERT_EQ(0, rbd.create2(ioctx, image_name, 65535,
	                   RBD_FEATURE_LAYERING, &order)); // Specified features make image of new format.

  ASSERT_EQ(0, rbd.group_image_add(ioctx, group_name, ioctx, image_name));

  ASSERT_EQ(0, rbd.group_snap_create(ioctx, group_name, snap_name));

  std::vector<librbd::group_snap_spec_t> snaps;
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps));
  ASSERT_EQ(1U, snaps.size());

  ASSERT_EQ(snap_name, snaps[0].name);

  ASSERT_EQ(0, rbd.group_snap_remove(ioctx, group_name, snap_name));

  snaps.clear();
  ASSERT_EQ(0, rbd.group_snap_list(ioctx, group_name, &snaps));
  ASSERT_EQ(0U, snaps.size());
}
