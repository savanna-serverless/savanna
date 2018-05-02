#include "readerwriterlock.h"
#include "log.h"
#include <boost/algorithm/string.hpp>
#include <vector>

ReaderWriterLock::ReaderWriterLock() : seq_num(0) {}

string ReaderWriterLock::reader_lock(string reader, int max_duration,
                                     uint lambda_seq, bool snap_iso) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "reader " << reader << " num owner = " << owners.size()
            << " write = " << write_mode;
  if (owners.size() == 0) {
    if (!snap_iso || lambda_seq >= seq_num) {
      owners[reader] =
          chrono::system_clock::now() + chrono::seconds(max_duration);
      write_mode = false;
      ret = "success";
    } else {
      ret = "exception: key_seq_num_err";
    }
  } else if (!write_mode) {
    if (owners.find(reader) == owners.end()) {
      if (!snap_iso || lambda_seq >= seq_num) {
        owners[reader] =
            chrono::system_clock::now() + chrono::seconds(max_duration);
        ret = "success";
      } else {
        ret = "exception: key_seq_num_err";
      }
    } else {
      ret = "exception: you already have the lock";
    }
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::reader_unlock(string reader) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "reader " << reader << " num owner = " << owners.size()
            << " write = " << write_mode;
  if (owners.size() == 0) {
    LOG_DEBUG << reader << " attempts to unlock, but owner.size() == 0";
    ret = "exception: empty owner list";
  } else if (write_mode) {
    LOG_DEBUG << reader << " attempts to unlock, but the lock is in write mode";
    ret = "exception: lock in write mode";
  } else if (owners.find(reader) == owners.end()) {
    LOG_DEBUG << reader << " attempts to unlock, but could not find reader";
    ret = "exception: can't find reader";
  } else {
    owners.erase(reader);
    version_locations[seq_num].push_back(reader);
    ret = "success";
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::writer_lock(string writer, int max_duration,
                                     uint lambda_seq, bool snap_iso) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "writer " << writer << " num owner = " << owners.size()
            << " write = " << write_mode << " seq_num = " << seq_num
            << " lambda_seq = " << lambda_seq;
  if (owners.size() == 0) {
    if (!snap_iso || lambda_seq >= seq_num) {
      owners[writer] =
          chrono::system_clock::now() + chrono::seconds(max_duration);
      write_mode = true;
      seq_num = lambda_seq;
      ret = "success";
    } else {
      ret = "exception: key_seq_num_err";
    }
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::writer_unlock(string writer) {
  string ret = "fail";
  lock.lock();
  LOG_DEBUG << "writer " << writer << " num owner = " << owners.size()
            << " write = " << write_mode;
  if (owners.size() == 0) {
    LOG_DEBUG << writer << " attempts to unlock, but owner.size() == 0";
    ret = "exception: empty owner list";
  } else if (!write_mode) {
    LOG_DEBUG << writer << " attempts to unlock, but the lock is in read mode";
    ret = "exception: lock in read mode";
  } else if (owners.find(writer) == owners.end()) {
    LOG_DEBUG << writer << " attempts to unlock, but could not find writer";
    ret = "exception: can't find writer";
  } else {
    owners.erase(writer);
    version_history.push_back(seq_num);
    vector<string> v;
    v.push_back(writer);
    version_locations[seq_num] = v;
    ret = "success";
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::get_locations(uint version) {
  string ret = "";
  int count = 0;
  for (auto l : version_locations[version]) {
    ret += l + ";";
    count += 1;
    if (count > 3)
      break;
  }
  LOG_DEBUG << "returning " << ret;
  return ret;
}

string ReaderWriterLock::get_locations_with_from(uint version, string from) {
  string ret = "";
  vector<string> parts;
  int count = 0;
  for (auto l : version_locations[version]) {
    LOG_DEBUG << "location " << l;
    boost::split(parts, l, boost::is_any_of("@"));
    if (parts[0] == from)
      return "use_local";
    ret += parts[0] + ";";
    count += 1;
    if (count > 3)
      break;
  }
  LOG_DEBUG << "returning " << ret;
  return ret;
}

string ReaderWriterLock::update_version_location(uint version,
                                                 string location) {
  string ret = "success";
  lock.lock();
  if (version == seq_num) {
    // assert(write_mode);
    owners.clear();
    owners[location] = chrono::system_clock::now() + chrono::seconds(1000);
  } else if (version < seq_num) {
    version_locations[seq_num].clear();
    version_locations[seq_num].push_back(location);
  } else {
    LOG_DEBUG << "version " << version << " seq_num " << seq_num << " location "
              << location;
    // assert(false);
  }
  lock.unlock();
  return ret;
}

string ReaderWriterLock::force_release_lock() {
  LOG_DEBUG << "force release lock";
  lock.lock();
  if (write_mode) {
    if (owners.size() > 0) {
      version_history.push_back(seq_num);
      vector<string> v;
      v.push_back(owners.begin()->first);
      version_locations[seq_num] = v;
      LOG_DEBUG << "owner " << owners.begin()->first << " pushed to history";
    } else {
      LOG_DEBUG << "owner empty";
    }
  }
  owners.clear();
  lock.unlock();
  return "success";
}
