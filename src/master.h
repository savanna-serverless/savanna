#ifndef MASTER_H
#define MASTER_H

#include "epollmasterworker.h"
#include "masterregistry.h"
#include "masterworker.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <list>
#include <unistd.h>
#include <vector>
#define USE_EPOLL 1

class MasterWorker;

class Master {
public:
  Master(std::uint16_t port);
  ~Master(); // No virtual needed since no inheritance as of now.

  void run();
  MasterRegistry registry;

protected:
  bool init();
  void cleanup();
  int make_socket_non_blocking(int);

  vector<EpollMasterWorker *> epoll_master_workers;
  std::uint16_t port;
  std::list<MasterWorker *> workers; // One worker per client
  int socket_fd;
  int num_core;
};

#endif // !SERVER_HH
