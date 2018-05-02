#ifndef MASTERREGISTRY_H
#define MASTERREGISTRY_H

#include "readerwriterlock.h"
#include "tbb/concurrent_hash_map.h"
#include <atomic>
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

#define USE_TBB 1

using namespace std;

class KeyEntry {
public:
  KeyEntry(string key, bool consistency = false);
  bool cache_key(string location);
  bool uncache_key(string location);
  void clear();
  string get_location(string);
  // bool is_cached(string location);
  const bool consistency;
  const string key;
  ReaderWriterLock consistent_lock;

private:
  boost::shared_mutex lock;
  set<string> locations;
};

struct KeyVersion {
  string key;
  uint version;
};

struct LockState {
  string key;
  bool write;
};

class LambdaEntry {
public:
  LambdaEntry(uint);
  void depends_on(string key, uint version);
  void use_lock(string key, bool write) {
    LockState ls = {key, write};
    locks.push_back(ls);
  };
  uint lambda_id;
  vector<KeyVersion> dependency;
  vector<LockState> locks;
};

struct KeyHashCompare {
  static size_t hash(const string &x) {
    size_t h = 0;
    for (const char *s = x.c_str(); *s; ++s)
      h = (h * 17) ^ *s;
    return h;
  }
  static bool equal(const string &x, const string &y) { return x == y; }
};

typedef tbb::concurrent_hash_map<string, KeyEntry *, KeyHashCompare> KeyHashMap;

struct LambdaHashCompare {
  static size_t hash(const int &x) { return x; }
  static bool equal(const int &x, const int &y) { return x == y; }
};

typedef tbb::concurrent_hash_map<uint, LambdaEntry *, LambdaHashCompare>
    LambdaHashMap;

class MasterRegistry {
public:
  MasterRegistry();
  ~MasterRegistry();
  bool reg_key(string key, string location);
  bool cache_key(string key, string location);
  bool uncache_key(string key, string location);
  void clear_key(string key);
  uint get_key_version(string key, bool prev);
  string get_location(string key, string from);
  string get_location_version(string key, string from, uint version);

  string consistent_write_lock(string key, string location, string lambda_id,
                               int max_duration, bool snap_iso);
  string consistent_write_unlock(string key, string location, string lambda_id,
                                 bool modified);

  string consistent_read_lock(string key, string location, string lambda_id,
                              int max_duration, bool snap_iso);
  string consistent_read_unlock(string key, string location, string lambda_id,
                                bool modified);

  string consistent_delete(string key, string lambda);
  string delete_key(string key);
  string get_lineage(uint lambda_id);
  uint get_lambda_seq();
  void register_lineage(uint lambda, string key, uint version);
  void register_lock(uint lambda, string key, bool write);
  string failover_write_update(string key, uint version, string addr,
                               string lambda);
  string force_release_lock(vector<uint> lambdas);

private:
  LambdaEntry *get_lambda_entry(uint lambda_id);
  KeyEntry *get_key_entry(string key);
  atomic<uint> lambda_seq;
#if USE_TBB == 1
  KeyHashMap keys;
  LambdaHashMap lineage;
#else
  map<string, KeyEntry *> keys;
  boost::shared_mutex lock;
  map<uint, LambdaEntry *> lineage;
  boost::shared_mutex lineage_lock;
#endif
};

#endif
