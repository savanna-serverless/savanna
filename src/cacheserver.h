#ifndef CACHESERVER_H
#define CACHESERVER_H

#define ENABLES3 0
#define USE_EPOLL 1

#include <map>
#include <mqueue.h>
#include <string>
#include <vector>
#if ENABLES3 == 1
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#endif
#include "epollobjserver.h"
#include "objclient.h"
#include "objserver.h"
#include "threadpool.h"
#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <fstream>
#include <memory>
using namespace std;

class ObjWorker;

struct MsgState;

class CacheServer {
public:
  CacheServer(string);
  void run();
  ~CacheServer();

private:
  ThreadPool tpool;
  string access_key_id;
  string secret_key;
  string master_ip;
  int master_sock;
  string ip;
  int port;
#if USE_EPOLL == 1
  EpollObjServer obj_server;
#else
  ObjServer obj_server;
#endif
  ObjClient obj_client;
  map<string, mqd_t> mqd_map;
  boost::shared_mutex mqd_map_lock;
  map<int, MsgState *> msg_states;
  boost::shared_mutex msg_states_lock;
  int msg_seq;

  mqd_t get_mqd(string);
  void delete_mqd(string);
  int send_master(string msg);
  shared_ptr<vector<string>> recv_master(int);
  void *recv_thread(void);
  static void *recv_thread_helper(void *);
#if ENABLES3 == 1
  std::shared_ptr<Aws::S3::S3Client> s3client;
#endif
  vector<string> lookup_key(string key);
  bool peer_read(string addr, string key);
  bool s3_write(string, string, bool);
  bool s3_read(string, string, string);
  bool s3_delete(string, string);
  void handle_put(vector<string> str);
  void handle_miss(vector<string> str);
  void handle_delete(vector<string> str);
  void handle_consistent_lock(vector<string> str);
  void handle_consistent_unlock(vector<string> str);
  void handle_consistent_delete(vector<string> str);
  void handle_write_s3(vector<string> str);
  void handle_lambda_exit(vector<string> str);
  void get_aws_credential();
  void setup_s3();
  string get_shm_name(string bucket, string key, bool consistency);
  void send(string name, string msg);
  void connect_master(string server, int port);
};

#endif
