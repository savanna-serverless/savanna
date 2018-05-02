#ifndef EPOLLWORKER_H
#define EPOLLWORKER_H

#include "objworker.h"
#include <map>

class EpollWorker {
public:
  EpollWorker();
  ~EpollWorker();
  void run();
  void add(int fd, ObjWorker *obj_worker);
  void remove(int fd);
  int get_count() { return count; }
  static void *pthread_helper(void *EpollWorker);

private:
  int epoll_fd;
  struct epoll_event *events;
  map<int, ObjWorker *> obj_workers;
  int count;
};

#endif
