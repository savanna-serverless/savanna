#ifndef EPOLLOBJSERVER_H
#define EPOLLOBJSERVER_H

#include "epollworker.h"
#include "objworker.h"
#include <map>
#include <string>

using namespace std;

class EpollObjServer {
public:
  EpollObjServer(int);
  void run();
  static void *run_helper(void *);

private:
  int port;
  vector<EpollWorker *> epoll_workers;
  int obj_server_sock;
  int make_socket_non_blocking(int sfd);
  void start_obj_server();
  void stop_obj_server();
};

#endif
