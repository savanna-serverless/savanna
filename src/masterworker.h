#ifndef MASTERWORKER_H
#define MASTERWORKER_H
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class Master;

class MasterWorker {
public:
  MasterWorker(Master &master, int socket);
  void run();
  static void *pthread_helper(void *worker);
  void do_action();

protected:
  void init();
  void exit();
  string readline();
  string handle_msg(string);
  string handle_new_server(vector<string>);
  string handle_cache(vector<string>);
  string handle_uncache(vector<string>);
  string handle_reg(vector<string>);
  string handle_lookup(vector<string>);
  string handle_consistent_lock(vector<string>);
  string handle_consistent_unlock(vector<string>);
  string handle_consistent_delete(vector<string>);
  string handle_delete(vector<string>);
  string handle_lineage(vector<string>);
  string handle_failover_write_update(vector<string>);
  string handle_force_release_lock(vector<string>);

  Master &master;
  int socket;
  uint lambda_seq;
  string ip;
  string port;
  string addr;

private:
  MasterWorker(const MasterWorker &); // No copies!
};

#endif // !WORKER_HH
