#ifndef OBJWORKER_H
#define OBJWORKER_H
#include <iostream>
#include <string>
#include <vector>

using namespace std;

class ObjWorker {
public:
  ObjWorker(int socket);
  void run();
  static void *pthread_helper(void *worker);
  void handle_msg();

protected:
  void exit();
  int socket;
  string remote_ip;

private:
  ObjWorker(const ObjWorker &); // No copies!
  int reply(int socket, const char *msg, int size, int retry = 1,
            bool silent = false);
  void handle_get(vector<string> parts);
  void handle_put(vector<string> parts, char *remaining_start,
                  int remaining_size);
  string get_remote_ip(int socket);
};

#endif
