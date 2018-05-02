#ifndef OBJSERVER_H
#define OBJSERVER_H

#include "objworker.h"
#include <map>
#include <string>

using namespace std;

class ObjServer {
public:
  ObjServer(int);
  void run_obj_worker();
  static void *run_obj_worker_helper(void *);

private:
  int port;
  vector<ObjWorker *> obj_workers;
  int obj_server_sock;

  void start_obj_server();
  void stop_obj_server();
};

#endif
