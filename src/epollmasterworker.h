#ifndef EPOLLMASTERWORKER_H
#define EPOLLMASTERWORKER_H

#include "masterworker.h"
#include <map>

class EpollMasterWorker {
public:
  EpollMasterWorker();
  ~EpollMasterWorker();
  void run();
  void add(int fd, MasterWorker *master_worker);
  void remove(int fd);
  int get_count() { return count; }
  static void *pthread_helper(void *EpollMasterWorker);

private:
  int epoll_fd;
  struct epoll_event *events;
  map<int, MasterWorker *> master_workers;
  int count;
};

#endif
