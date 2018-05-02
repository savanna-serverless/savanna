#ifndef READERWRITERLOCK_H
#define READERWRITERLOCK_H

#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <chrono>
#include <ctime>
#include <map>
#include <mutex>
#include <string>

using namespace std;

class ReaderWriterLock {

public:
  ReaderWriterLock();
  string reader_lock(string, int duration, uint lambda_seq, bool snap_iso);
  string reader_unlock(string);
  string writer_lock(string, int duration, uint lambda_seq, bool snap_iso);
  string writer_unlock(string);
  // TODO: implement stale object collection
  uint get_prev_seq_num() { return version_history.back(); }
  uint get_seq_num() { return seq_num; }
  string get_locations(uint version);
  string get_locations_with_from(uint version, string from);
  string update_version_location(uint version, string location);
  string force_release_lock();

private:
  boost::shared_mutex lock;
  map<string, chrono::time_point<std::chrono::system_clock>> owners;
  map<uint, vector<string>> version_locations;
  vector<uint> version_history;
  bool write_mode;
  uint seq_num;
};

#endif
