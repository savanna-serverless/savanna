#include "masterregistry.h"
#include "log.h"
#include <queue>
#include <set>

KeyEntry::KeyEntry(string key, bool consistency)
    : key(key), consistency(consistency) {}

bool KeyEntry::cache_key(string location) {
  lock.lock();
  LOG_DEBUG << "Key " << key << " cached at " << location;
  locations.insert(location);
  lock.unlock();
  return true;
}

bool KeyEntry::uncache_key(string location) {
  lock.lock();
  LOG_DEBUG << "Key " << key << " removed from " << location;
  locations.erase(location);
  lock.unlock();
  return true;
}

void KeyEntry::clear() {
  lock.lock();
  LOG_DEBUG << "Key " << key << " cleared";
  locations.clear();
  lock.unlock();
}

string KeyEntry::get_location(string from) {
  lock.lock_shared();
  string ret("");
  if (locations.find(from) != locations.end()) {
    ret = "use_local";
  } else {
    int x = 0;
    for (auto &i : locations) {
      ret += i + ";";
      x++;
      if (x >= 3)
        break;
    }
  }
  lock.unlock_shared();
  LOG_DEBUG << "Key " << key << " is cached at " << ret;
  return ret;
}

/*
bool KeyEntry::is_cached(string location) {
  lock.lock_shared();
  bool res = locations.find(location) != locations.end();
  lock.unlock_shared();
  LOG_DEBUG << "Key " << key << " is cache at " << location << "? " << res;
  return res;
}
*/

LambdaEntry::LambdaEntry(uint lambda_id) : lambda_id(lambda_id) {}

void LambdaEntry::depends_on(string key, uint version) {
  KeyVersion kv = {key, version};
  dependency.push_back(kv);
}

MasterRegistry::MasterRegistry() : lambda_seq(0) {
  LOG_INFO << "Init MasterRegistry";
}

MasterRegistry::~MasterRegistry() {
  LOG_INFO << "Deleting MasterRegistry";
  for (auto it = keys.begin(); it != keys.end(); it++)
    delete it->second;
}

uint MasterRegistry::get_lambda_seq() {
  uint seq = lambda_seq.fetch_add(1);
  auto entry = new LambdaEntry(seq);
#if USE_TBB
  LambdaHashMap::accessor acc;
  lineage.insert(acc, seq);
  acc->second = entry;
#else
  lineage_lock.lock();
  lineage[seq] = entry;
  lineage_lock.unlock();
#endif
  return seq;
}

uint MasterRegistry::get_key_version(string input_key, bool prev) {
  auto key_entry = get_key_entry(input_key);
  if (key_entry != NULL) {
    if (prev)
      return key_entry->consistent_lock.get_prev_seq_num();
    else
      return key_entry->consistent_lock.get_seq_num();
  } else {
    assert(false);
  }
}

void MasterRegistry::register_lineage(uint lambda, string key, uint version) {
  auto entry = get_lambda_entry(lambda);
  if (entry == NULL) {
    LOG_ERROR << "lambda" << lambda << " not exist in lineage";
    assert(false);
  } else {
    entry->depends_on(key, version);
    LOG_DEBUG << "Sucessfully registered lineage for " << lambda << ", key "
              << key << ", version " << version;
  }
}

bool MasterRegistry::reg_key(string key, string location) {
  assert(key.at(0) != '~');
  bool ret = false;
  LOG_DEBUG << "reg_key " << key << " at location " << location;
  auto key_entry = get_key_entry(key);
  if (key_entry != NULL) {
    if (key_entry->consistency) {
      LOG_DEBUG << key << " is a consistent key";
      ret = false;
    } else {
      LOG_DEBUG << key << " exist, clear key location cache";
      key_entry->clear();
      ret = true;
    }
  } else {
    LOG_DEBUG << key << " does not exist, creating new entry";
#if USE_TBB == 1
    KeyHashMap::accessor acc;
    keys.insert(acc, key);
    key_entry = new KeyEntry(key);
    acc->second = key_entry;
#else
    lock.lock();
    key_entry = new KeyEntry(key);
    keys[key] = key_entry;
    lock.unlock();
#endif
    LOG_DEBUG << key << " entry created";
    ret = true;
  }

  if (ret) {
    LOG_DEBUG << key << " location " << location << " to be cached";
    key_entry->cache_key(location);
  }
  return ret;
}

bool MasterRegistry::cache_key(string key, string location) {
  auto key_entry = get_key_entry(key);
  LOG_DEBUG << key << " location to be cached";
  return key_entry->cache_key(location);
}

bool MasterRegistry::uncache_key(string key, string location) {
  auto key_entry = get_key_entry(key);
  LOG_DEBUG << key << " location to be uncached";
  return key_entry->uncache_key(location);
}

void MasterRegistry::clear_key(string key) {
#if USE_TBB == 1
  keys.erase(key);
#else
  lock.lock();
  LOG_DEBUG << key << " entry to be erased";
  keys.erase(key);
  lock.unlock();
#endif
}

string MasterRegistry::get_location(string input_key, string from) {
  auto key_entry = get_key_entry(input_key);
  if (key_entry == NULL) {
    LOG_DEBUG << "input_key " << input_key << ", key is not found";
    return "";
  } else {
    // if (key_entry->second->is_cached(from)) {
    //  LOG_DEBUG << "input_key " << input_key << " key " << key << ", key exist
    //  on local machien, returning use_local";
    //  return "use_local";
    //} else {
    LOG_DEBUG << "input_key " << input_key << ", query key entry for location";
    return key_entry->get_location(from);
    //}
  }
}

string MasterRegistry::get_location_version(string input_key, string from,
                                            uint version) {
  auto key_entry = get_key_entry(input_key);
  if (key_entry == NULL) {
    LOG_DEBUG << "input_key " << input_key << ", key is not found";
    return "";
  } else {
    LOG_DEBUG << "input_key " << input_key << ", version " << version
              << ", query key entry for location, from " << from;
    return key_entry->consistent_lock.get_locations_with_from(version, from);
  }
}

string MasterRegistry::consistent_read_lock(string input_key, string location,
                                            string lambda_id, int max_duration,
                                            bool snap_iso) {
  assert(input_key.at(0) == '~');
  string uri = location + "@" + lambda_id;
  uint lambda_seq = atoi(lambda_id.substr(6).c_str());
  string ret;
  auto key_entry = get_key_entry(input_key);
  if (key_entry != NULL) {
    if (key_entry->consistency) {
      ret = key_entry->consistent_lock.reader_lock(uri, max_duration,
                                                   lambda_seq, snap_iso);
    } else {
      ret = "exception: not_consistent_key";
    }
  } else {
    ret = "exception: key_not_found";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::consistent_read_unlock(string input_key, string location,
                                              string lambda_id, bool modified) {
  assert(input_key.at(0) == '~');
  string uri = location + "@" + lambda_id;
  string ret;
  auto key_entry = get_key_entry(input_key);
  if (key_entry == NULL) {
    LOG_ERROR << input_key << " does not exsit when unlock";
    ret = "exception: key_not_exist";
  } else {
    if (modified) {
      LOG_DEBUG << "key modified, after lock, caching key location";
      key_entry->cache_key(location);
    }
    ret = key_entry->consistent_lock.reader_unlock(uri);
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::consistent_write_lock(string input_key, string location,
                                             string lambda_id, int max_duration,
                                             bool snap_iso) {
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  string uri = location + "@" + lambda_id;
  uint lambda_seq = atoi(lambda_id.substr(6).c_str());
  string ret;
  auto key_entry = get_key_entry(input_key);
  if (key_entry != NULL) {
    if (key_entry->consistency) {
      ret = key_entry->consistent_lock.writer_lock(uri, max_duration,
                                                   lambda_seq, snap_iso);
    } else {
      ret = "exception: not_consistent_key";
    }
  } else {
// TODO: possible that keys[key] is not empty now....
// solution, must hold lock when delete
#if USE_TBB == 1
    KeyHashMap::accessor acc;
    keys.insert(acc, key);
    auto value = new KeyEntry(key, true);
    acc->second = value;
#else
    lock.lock();
    auto value = new KeyEntry(key, true);
    keys[key] = value;
    lock.unlock();
#endif
    ret = value->consistent_lock.writer_lock(uri, max_duration, lambda_seq,
                                             snap_iso);
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::consistent_delete(string input_key, string lambda_id) {
  assert(input_key.at(0) == '~');
  string key = input_key.substr(1);
  uint lambda_seq = atoi(lambda_id.substr(6).c_str());
  string ret;
  auto key_entry = get_key_entry(input_key);
  if (key_entry != NULL) {
    if (key_entry->consistency) {
      ret = key_entry->consistent_lock.writer_lock("null", 65536, lambda_seq,
                                                   false);
      if (ret == "success") {
#if USE_TBB == 1
        KeyHashMap::accessor acc;
        keys.find(acc, key);
        delete acc->second;
        keys.erase(key);
#else
        lock.lock();
        delete keys[key];
        keys.erase(key);
        lock.unlock();
#endif
      }
    } else {
      ret = "exception: not_consistent_key";
    }
  } else {
    ret = "exception: key_not_found";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::delete_key(string key) {
  assert(key.at(0) != '~');
  string ret;
  auto key_entry = get_key_entry(key);
  if (key_entry != NULL) {
    if (key_entry->consistency) {
      ret = "exception: key_is_consistent";
    } else {
#if USE_TBB == 1
      KeyHashMap::accessor acc;
      keys.find(acc, key);
      delete acc->second;
      keys.erase(key);
#else
      lock.lock();
      delete keys[key];
      keys.erase(key);
      lock.unlock();
#endif
      ret = "success";
    }
  } else {
    ret = "exception: key_not_found";
  }
  LOG_DEBUG << "Return: " << ret;
  return ret;
}

string MasterRegistry::consistent_write_unlock(string input_key,
                                               string location,
                                               string lambda_id,
                                               bool modified) {
  assert(input_key.at(0) == '~');
  string uri = location + "@" + lambda_id;
  string ret;
  auto key_entry = get_key_entry(input_key);
  if (key_entry == NULL) {
    LOG_ERROR << input_key << " does not exsit when unlock";
    ret = "exception: key_not_exist";
  } else {
    if (modified) {
      LOG_DEBUG << "key modified, after lock, caching key location";
      key_entry->clear();
      key_entry->cache_key(location);
    }
    ret = key_entry->consistent_lock.writer_unlock(uri);
  }
  LOG_DEBUG << "return: " << ret;
  return ret;
}

KeyEntry *MasterRegistry::get_key_entry(string input_key) {
  bool consistency = input_key[0] == '~';
  string key = consistency ? input_key.substr(1) : input_key;
#if USE_TBB == 1
  KeyHashMap::accessor key_acc;
  if (keys.find(key_acc, key)) {
    return key_acc->second;
  } else {
    return NULL;
  }
#else
  lock.lock_shared();
  auto key_entry = keys.find(key);
  lock.unlock_shared();
  return key_entry == keys.end() ? NULL : key_entry->second;
#endif
}

LambdaEntry *MasterRegistry::get_lambda_entry(uint lambda_id) {
#if USE_TBB == 1
  LambdaHashMap::accessor acc;
  if (lineage.find(acc, lambda_id)) {
    return acc->second;
  } else {
    return NULL;
  }
#else
  lineage_lock.lock_shared();
  auto lambda_entry = lineage.find(lambda_id);
  lineage_lock.unlock_shared();
  return lambda_entry == lineage.end() ? NULL : lambda_entry->second;
#endif
}

string MasterRegistry::get_lineage(uint lambda_id) {
  string res = "";
  set<uint> visited;
  queue<uint> pending;
  pending.push(lambda_id);
  visited.insert(lambda_id);
  while (!pending.empty()) {
    uint curr = pending.front();
    pending.pop();
    auto entry = get_lambda_entry(curr);
    for (auto kv : entry->dependency) {
      if (visited.find(kv.version) == visited.end()) {
        visited.insert(kv.version);
        pending.push(kv.version);
      }

      auto key_entry = get_key_entry(kv.key);
      LOG_DEBUG << "get_lineage from lambda " << lambda_id << " for key "
                << kv.key << " version " << kv.version;
      string locations = key_entry->consistent_lock.get_locations(kv.version);
      res += to_string(curr) + "," + kv.key + "," + to_string(kv.version) +
             "," + locations + "$";
    }
  }
  return res;
}

string MasterRegistry::failover_write_update(string key, uint version,
                                             string addr, string lambda_id) {
  auto key_entry = get_key_entry(key);
  if (key_entry == NULL)
    return "key_not_found";
  else {
    key_entry->clear();
    key_entry->cache_key(addr);
    return key_entry->consistent_lock.update_version_location(
        version, addr + "@" + lambda_id);
  }
}

string MasterRegistry::force_release_lock(vector<uint> lambdas) {
  LOG_DEBUG << "force release lock";
  for (uint lambda_id : lambdas) {
    LOG_DEBUG << "force release lock for lambda " << lambda_id;
    auto entry = get_lambda_entry(lambda_id);
    for (LockState ls : entry->locks) {
      LOG_DEBUG << "releaseing " << ls.key;
      get_key_entry(ls.key)->consistent_lock.force_release_lock();
    }
  }
  return "success";
}

void MasterRegistry::register_lock(uint lambda, string key, bool write) {
  auto entry = get_lambda_entry(lambda);
  if (entry == NULL) {
    LOG_ERROR << "can't file lambda in lineage";
    assert(false);
  } else {
    entry->use_lock(key, write);
    LOG_DEBUG << "Sucessfully registered lock for " << lambda << ", key " << key
              << ", write " << write;
  }
};
