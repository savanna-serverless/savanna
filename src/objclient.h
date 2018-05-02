#ifndef OBJCLIENT_H
#define OBJCLIENT_H
#include <map>
#include <string>

using namespace std;

class ObjClient {
public:
  ObjClient();
  bool fetch(string node, string key);
  ~ObjClient();

private:
  map<string, int> obj_client_socks;
  int get_or_create_sock(string node);
  int pipefd[2];
};

#endif
