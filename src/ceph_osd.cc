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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <uuid/uuid.h>
#include <boost/scoped_ptr.hpp>

#include <iostream>
#include <string>
using namespace std;

#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "include/ceph_features.h"

#include "common/config.h"

#include "mon/MonMap.h"


#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/color.h"
#include "common/errno.h"
#include "common/pick_address.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#include "erasure-code/ErasureCodePlugin.h"

#define dout_subsys ceph_subsys_osd

///*hf
#define BINDPORT_NUM	5
#define PUBLIC_PORT   6800
#define CLUSTER_PORT  6801
#define HB_B_S_PORT   6802
#define HB_F_S_PORT   6803
#define OBJECTER_PORT 6804
#define PUBLIC_PORT_OFFSET   0
#define CLUSTER_PORT_OFFSET  1
#define HB_B_S_PORT_OFFSET   2
#define HB_F_S_PORT_OFFSET   3
#define OBJECTER_PORT_OFFSET 4
//hf*/

OSD *osd = NULL;

///*hf
int cephosd_parse_port(const char *str, int *minport, int *maxport)
{
    //cout << " ******* HF: parse port" << str << std::endl;
	if (sscanf(str, "%u:%u", minport, maxport) != 2) {
    cout << " ******* HF: parse port -1 " << std::endl;
		return -1;//错误字符序列,读取失败
	}
    //cout << " ******* HF: min" << *minport << "max" << *maxport << std::endl;
	if (*minport == 0 || *maxport == 0 || *minport >= 0xffff || *maxport >= 0xffff) {
    cout << " ******* HF: parse port -2 " << std::endl;
		return -2;//端口非法
	}
    //cout << " ******* HF: ret " << (*maxport - *minport + 1) << "  BINDPORT_NUM " << BINDPORT_NUM << std::endl;
	if ((*maxport - *minport + 1) % BINDPORT_NUM != 0) {
    cout << " ******* HF: parse port -3 " << std::endl;
		return -3;//非OSD成组端口数目, 判断为1组大小
	}
    //cout << " ******* HF: min" << *minport << "max" << *maxport << std::endl;

	return 0;
}
//hf*/

void handle_osd_signal(int signum)
{
  if (osd)
    osd->handle_signal(signum);
}

void usage() 
{
  derr << "usage: ceph-osd -i osdid [--osd-data=path] [--osd-journal=path] "
       << "[--mkfs] [--mkjournal] [--convert-filestore]" << dendl;
  derr << "   --debug_osd N   set debug level (e.g. 10)" << dendl;
  generic_server_usage();
}

int preload_erasure_code()
{
  string directory = g_conf->osd_pool_default_erasure_code_directory;
  string plugins = g_conf->osd_erasure_code_plugins;
  stringstream ss;
  int r = ErasureCodePluginRegistry::instance().preload(plugins,
							directory,
							ss);
  if (r)
    derr << ss.str() << dendl;
  else
    dout(10) << ss.str() << dendl;
  return r;
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  vector<const char*> def_args;
  // We want to enable leveldb's log, while allowing users to override this
  // option, therefore we will pass it as a default argument to global_init().
  def_args.push_back("--leveldb-log=");

  global_init(&def_args, args, CEPH_ENTITY_TYPE_OSD, CODE_ENVIRONMENT_DAEMON, 0);
  ceph_heap_profiler_init();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  bool mkkey = false;
  bool flushjournal = false;
  bool dump_journal = false;
  bool convertfilestore = false;
  bool get_journal_fsid = false;
  bool get_osd_fsid = false;
  bool get_cluster_fsid = false;
  std::string dump_pg_log;

  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_flag(args, i, "--mkjournal", (char*)NULL)) {
      mkjournal = true;
    } else if (ceph_argparse_flag(args, i, "--mkkey", (char*)NULL)) {
      mkkey = true;
    } else if (ceph_argparse_flag(args, i, "--flush-journal", (char*)NULL)) {
      flushjournal = true;
    } else if (ceph_argparse_flag(args, i, "--convert-filestore", (char*)NULL)) {
      convertfilestore = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--dump-pg-log", (char*)NULL)) {
      dump_pg_log = val;
    } else if (ceph_argparse_flag(args, i, "--dump-journal", (char*)NULL)) {
      dump_journal = true;
    } else if (ceph_argparse_flag(args, i, "--get-cluster-fsid", (char*)NULL)) {
      get_cluster_fsid = true;
    } else if (ceph_argparse_flag(args, i, "--get-osd-fsid", "--get-osd-uuid", (char*)NULL)) {
      get_osd_fsid = true;
    } else if (ceph_argparse_flag(args, i, "--get-journal-fsid", "--get-journal-uuid", (char*)NULL)) {
      get_journal_fsid = true;
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    derr << "unrecognized arg " << args[0] << dendl;
    usage();
  }

  if (!dump_pg_log.empty()) {
    common_init_finish(g_ceph_context);
    bufferlist bl;
    std::string error;
    int r = bl.read_file(dump_pg_log.c_str(), &error);
    if (r >= 0) {
      pg_log_entry_t e;
      bufferlist::iterator p = bl.begin();
      while (!p.end()) {
	uint64_t pos = p.get_off();
	try {
	  ::decode(e, p);
	}
	catch (const buffer::error &e) {
	  derr << "failed to decode LogEntry at offset " << pos << dendl;
	  return 1;
	}
	derr << pos << ":\t" << e << dendl;
      }
    } else {
      derr << "unable to open " << dump_pg_log << ": " << error << dendl;
    }
    return 0;
  }

  // whoami
  char *end;
  const char *id = g_conf->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);
  if (*end || end == id || whoami < 0) {
    derr << "must specify '-i #' where # is the osd number" << dendl;
    usage();
  }

  if (g_conf->osd_data.empty()) {
    derr << "must specify '--osd-data=foo' data path" << dendl;
    usage();
  }

  // the store
  ObjectStore *store = ObjectStore::create(g_ceph_context,
					   g_conf->osd_objectstore,
					   g_conf->osd_data,
					   g_conf->osd_journal);
  if (!store) {
    derr << "unable to create object store" << dendl;
    return -ENODEV;
  }

  if (mkfs) {
    common_init_finish(g_ceph_context);
    MonClient mc(g_ceph_context);
    if (mc.build_initial_monmap() < 0)
      return -1;
    if (mc.get_monmap_privately() < 0)
      return -1;

    int err = OSD::mkfs(g_ceph_context, store, g_conf->osd_data,
			mc.monmap.fsid, whoami);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating empty object store in "
	   << g_conf->osd_data << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created object store " << g_conf->osd_data;
    if (!g_conf->osd_journal.empty())
      *_dout << " journal " << g_conf->osd_journal;
    *_dout << " for osd." << whoami << " fsid " << mc.monmap.fsid << dendl;
  }
  if (mkkey) {
    common_init_finish(g_ceph_context);
    KeyRing *keyring = KeyRing::create_empty();
    if (!keyring) {
      derr << "Unable to get a Ceph keyring." << dendl;
      return 1;
    }

    EntityName ename(g_conf->name);
    EntityAuth eauth;

    int ret = keyring->load(g_ceph_context, g_conf->keyring);
    if (ret == 0 &&
	keyring->get_auth(ename, eauth)) {
      derr << "already have key in keyring " << g_conf->keyring << dendl;
    } else {
      eauth.key.create(g_ceph_context, CEPH_CRYPTO_AES);
      keyring->add(ename, eauth);
      bufferlist bl;
      keyring->encode_plaintext(bl);
      int r = bl.write_file(g_conf->keyring.c_str(), 0600);
      if (r)
	derr << TEXT_RED << " ** ERROR: writing new keyring to " << g_conf->keyring
	     << ": " << cpp_strerror(r) << TEXT_NORMAL << dendl;
      else
	derr << "created new key in keyring " << g_conf->keyring << dendl;
    }
  }
  if (mkfs || mkkey)
    exit(0);
  if (mkjournal) {
    common_init_finish(g_ceph_context);
    int err = store->mkjournal();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error creating fresh journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "created new journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data << dendl;
    exit(0);
  }
  if (flushjournal) {
    common_init_finish(g_ceph_context);
    int err = store->mount();
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error flushing journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    store->sync_and_flush();
    store->umount();
    derr << "flushed journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data
	 << dendl;
    exit(0);
  }
  if (dump_journal) {
    common_init_finish(g_ceph_context);
    int err = store->dump_journal(cout);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error dumping journal " << g_conf->osd_journal
	   << " for object store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    derr << "dumped journal " << g_conf->osd_journal
	 << " for object store " << g_conf->osd_data
	 << dendl;
    exit(0);

  }


  if (convertfilestore) {
    int err = OSD::do_convertfs(store);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error converting store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
    exit(0);
  }
  
  if (get_journal_fsid) {
    uuid_d fsid;
    int r = store->peek_journal_fsid(&fsid);
    if (r == 0)
      cout << fsid << std::endl;
    exit(r);
  }

  string magic;
  uuid_d cluster_fsid, osd_fsid;
  int w;
  int r = OSD::peek_meta(store, magic, cluster_fsid, osd_fsid, w);
  if (r < 0) {
    derr << TEXT_RED << " ** ERROR: unable to open OSD superblock on "
	 << g_conf->osd_data << ": " << cpp_strerror(-r)
	 << TEXT_NORMAL << dendl;
    if (r == -ENOTSUP) {
      derr << TEXT_RED << " **        please verify that underlying storage "
	   << "supports xattrs" << TEXT_NORMAL << dendl;
    }
    exit(1);
  }
  if (w != whoami) {
    derr << "OSD id " << w << " != my id " << whoami << dendl;
    exit(1);
  }
  if (strcmp(magic.c_str(), CEPH_OSD_ONDISK_MAGIC)) {
    derr << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC
	 << dendl;
    exit(1);
  }

  if (get_cluster_fsid) {
    cout << cluster_fsid << std::endl;
    exit(0);
  }
  if (get_osd_fsid) {
    cout << osd_fsid << std::endl;
    exit(0);
  }

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC
                                |CEPH_PICK_ADDRESS_CLUSTER);

  if (g_conf->public_addr.is_blank_ip() && !g_conf->cluster_addr.is_blank_ip()) {
    derr << TEXT_YELLOW
	 << " ** WARNING: specified cluster addr but not public addr; we recommend **\n"
	 << " **          you specify neither or both.                             **"
	 << TEXT_NORMAL << dendl;
  }

  Messenger *ms_public = Messenger::create(g_ceph_context,
					   entity_name_t::OSD(whoami), "client",
					   getpid());
  Messenger *ms_cluster = Messenger::create(g_ceph_context,
					    entity_name_t::OSD(whoami), "cluster",
					    getpid());
  Messenger *ms_hbclient = Messenger::create(g_ceph_context,
					     entity_name_t::OSD(whoami), "hbclient",
					     getpid());
  Messenger *ms_hb_back_server = Messenger::create(g_ceph_context,
						   entity_name_t::OSD(whoami), "hb_back_server",
						   getpid());
  Messenger *ms_hb_front_server = Messenger::create(g_ceph_context,
						    entity_name_t::OSD(whoami), "hb_front_server",
						    getpid());
  Messenger *ms_objecter = Messenger::create(g_ceph_context,
					     entity_name_t::OSD(whoami), "ms_objecter",
					     getpid());
  ms_cluster->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hbclient->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_back_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms_hb_front_server->set_cluster_protocol(CEPH_OSD_PROTOCOL);

  cout << "starting osd." << whoami
       << " at " << ms_public->get_myaddr()
       << " osd_data " << g_conf->osd_data
       << " " << ((g_conf->osd_journal.empty()) ?
		  "(no journal)" : g_conf->osd_journal)
       << std::endl;

  boost::scoped_ptr<Throttle> client_byte_throttler(
    new Throttle(g_ceph_context, "osd_client_bytes",
		 g_conf->osd_client_message_size_cap));
  boost::scoped_ptr<Throttle> client_msg_throttler(
    new Throttle(g_ceph_context, "osd_client_messages",
		 g_conf->osd_client_message_cap));

  uint64_t supported =
    CEPH_FEATURE_UID | 
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH |
    CEPH_FEATURE_OSD_ERASURE_CODES;

  ms_public->set_default_policy(Messenger::Policy::stateless_server(supported, 0));
  ms_public->set_policy_throttlers(entity_name_t::TYPE_CLIENT,
				   client_byte_throttler.get(),
				   client_msg_throttler.get());
  ms_public->set_policy(entity_name_t::TYPE_MON,
                               Messenger::Policy::lossy_client(supported,
							       CEPH_FEATURE_UID |
							       CEPH_FEATURE_PGID64 |
							       CEPH_FEATURE_OSDENC));
  //try to poison pill any OSD connections on the wrong address
  ms_public->set_policy(entity_name_t::TYPE_OSD,
			Messenger::Policy::stateless_server(0,0));
  
  ms_cluster->set_default_policy(Messenger::Policy::stateless_server(0, 0));
  ms_cluster->set_policy(entity_name_t::TYPE_MON, Messenger::Policy::lossy_client(0,0));
  ms_cluster->set_policy(entity_name_t::TYPE_OSD,
			 Messenger::Policy::lossless_peer(supported,
							  CEPH_FEATURE_UID |
							  CEPH_FEATURE_PGID64 |
							  CEPH_FEATURE_OSDENC));
  ms_cluster->set_policy(entity_name_t::TYPE_CLIENT,
			 Messenger::Policy::stateless_server(0, 0));

  ms_hbclient->set_policy(entity_name_t::TYPE_OSD,
			  Messenger::Policy::lossy_client(0, 0));
  ms_hb_back_server->set_policy(entity_name_t::TYPE_OSD,
				Messenger::Policy::stateless_server(0, 0));
  ms_hb_front_server->set_policy(entity_name_t::TYPE_OSD,
				 Messenger::Policy::stateless_server(0, 0));

  ms_objecter->set_default_policy(Messenger::Policy::lossy_client(0, CEPH_FEATURE_OSDREPLYMUX));

///*hf
    std::vector <std::string> my_sections;
    std::string nat_addr_str;
    entity_addr_t nat_addr;
    g_conf->get_my_sections(my_sections);
    if (g_conf->get_val_from_conf_file(my_sections, "nat addr", nat_addr_str, true) == 0) {
      nat_addr.parse(nat_addr_str.c_str());
    } else {
      nat_addr = g_conf->public_addr;
    }
    ms_public->ip_addr = nat_addr;
    ms_hb_front_server->ip_addr = nat_addr;
    ms_objecter->ip_addr = nat_addr;
		int nat_port = nat_addr.get_port();
		if (nat_port != 0) {
			ms_public->ip_addr.set_port(nat_port + PUBLIC_PORT_OFFSET);
      ms_hb_front_server->ip_addr.set_port(nat_port + HB_F_S_PORT_OFFSET);
      ms_objecter->ip_addr.set_port(nat_port + OBJECTER_PORT_OFFSET);
		}
    std::string bindport_str;
		int bindport_min, bindport_max, bindport_start;
		int public_port, cluster_port, hb_b_s_port, hb_f_s_port, objecter_port;
    //cout << " ******* HF0: public port is " << public_port << std::endl;
    if ((g_conf->get_val_from_conf_file(my_sections, "bind port", bindport_str, true) == 0) && 
			(cephosd_parse_port(bindport_str.c_str(), &bindport_min, &bindport_max) == 0)) {
      public_port   = bindport_min + PUBLIC_PORT_OFFSET;
			cluster_port  = bindport_min + CLUSTER_PORT_OFFSET;
			hb_b_s_port   = bindport_min + HB_B_S_PORT_OFFSET;
			hb_f_s_port   = bindport_min + HB_F_S_PORT_OFFSET;
			objecter_port = bindport_min + OBJECTER_PORT_OFFSET;
    //cout << " ******* HF1: public port is " << public_port << std::endl;
    } else {
      public_port   = PUBLIC_PORT;
			cluster_port  = CLUSTER_PORT;
			hb_b_s_port   = HB_B_S_PORT;
			hb_f_s_port   = HB_F_S_PORT;
			objecter_port = OBJECTER_PORT;
    //cout << " ******* HF2: public port is " << public_port << std::endl;
    }
//hf*/

//hf
  entity_addr_t myaddr = g_conf->public_addr;
  myaddr.set_port(public_port);
    //cout << " ******* HF3: public port is " << public_port << std::endl;
  r = ms_public->bind(myaddr);
  if (r < 0) {
    cout << " ** ERROR: port is unavailable, please check set of ceph.conf " << std::endl;
    exit(1);
  }
  ms_public->bind_addr = myaddr;

  myaddr = g_conf->cluster_addr;
  myaddr.set_port(cluster_port);
  r = ms_cluster->bind(myaddr);
  if (r < 0) {
    cout << " ** ERROR: port is unavailable, please check set of ceph.conf " << std::endl;
    exit(1);
  }
  ms_cluster->bind_addr = myaddr;

  // hb back should bind to same ip as cluster_addr (if specified)
  entity_addr_t hb_back_addr = g_conf->osd_heartbeat_addr;
  if (hb_back_addr.is_blank_ip()) {
    hb_back_addr = g_conf->cluster_addr;
    if (hb_back_addr.is_ip())
      hb_back_addr.set_port(0);
  }
  myaddr = hb_back_addr;
  myaddr.set_port(hb_b_s_port);
  r = ms_hb_back_server->bind(myaddr);
  if (r < 0) {
    cout << " ** ERROR: port is unavailable, please check set of ceph.conf " << std::endl;
    exit(1);
  }
	ms_hb_back_server->bind_addr = myaddr;

  // hb front should bind to same ip as public_addr
  entity_addr_t hb_front_addr = g_conf->public_addr;
  if (hb_front_addr.is_ip())
    hb_front_addr.set_port(0);
  myaddr = hb_front_addr;
  myaddr.set_port(hb_f_s_port);
  r = ms_hb_front_server->bind(myaddr);
  if (r < 0) {
    cout << " ** ERROR: port is unavailable, please check set of ceph.conf " << std::endl;
    exit(1);
  }
	ms_hb_front_server->bind_addr = myaddr;

  myaddr = g_conf->public_addr;
  myaddr.set_port(objecter_port);
  ms_objecter->bind(myaddr);
	ms_objecter->bind_addr = myaddr;
//hf

  // Set up crypto, daemonize, etc.
  global_init_daemonize(g_ceph_context, 0);
  common_init_finish(g_ceph_context);

  if (g_conf->filestore_update_to >= (int)store->get_target_version()) {
    int err = OSD::do_convertfs(store);
    if (err < 0) {
      derr << TEXT_RED << " ** ERROR: error converting store " << g_conf->osd_data
	   << ": " << cpp_strerror(-err) << TEXT_NORMAL << dendl;
      exit(1);
    }
  }

  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  global_init_chdir(g_ceph_context);

  if (preload_erasure_code() < 0)
    return -1;

  osd = new OSD(g_ceph_context,
		store,
		whoami,
		ms_cluster,
		ms_public,
		ms_hbclient,
		ms_hb_front_server,
		ms_hb_back_server,
		ms_objecter,
		&mc,
		g_conf->osd_data, g_conf->osd_journal);

  int err = osd->pre_init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd pre_init failed: " << cpp_strerror(-err)
	 << TEXT_NORMAL << dendl;
    return 1;
  }

  // Now close the standard file descriptors
  global_init_shutdown_stderr(g_ceph_context);

  ms_public->start();
  ms_hbclient->start();
  ms_hb_front_server->start();
  ms_hb_back_server->start();
  ms_cluster->start();
  ms_objecter->start();

  // start osd
  err = osd->init();
  if (err < 0) {
    derr << TEXT_RED << " ** ERROR: osd init failed: " << cpp_strerror(-err)
         << TEXT_NORMAL << dendl;
    return 1;
  }

  // install signal handlers
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_osd_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_osd_signal);

  osd->final_init();

  if (g_conf->inject_early_sigterm)
    kill(getpid(), SIGTERM);

  ms_public->wait();
  ms_hbclient->wait();
  ms_hb_front_server->wait();
  ms_hb_back_server->wait();
  ms_cluster->wait();
  ms_objecter->wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_osd_signal);
  unregister_async_signal_handler(SIGTERM, handle_osd_signal);
  shutdown_async_signal_handler();

  // done
  delete osd;
  delete ms_public;
  delete ms_hbclient;
  delete ms_hb_front_server;
  delete ms_hb_back_server;
  delete ms_cluster;
  delete ms_objecter;
  client_byte_throttler.reset();
  client_msg_throttler.reset();
  g_ceph_context->put();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-osd: gmon.out should be in " << s << dendl;
  }

  return 0;
}
