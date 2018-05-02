#include "cacheserver.h"
#include "log.h"
#include "objworker.h"
#include "threadpool.h"
#include <boost/algorithm/string.hpp>
#include <errno.h>
#include <fstream>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define MSGSIZE 256
#define THRDPOOLSIZE 1
#define PORT 1222

#if ENABLES3 == 1
using namespace Aws::S3;
using namespace Aws::S3::Model;
using namespace Aws::Client;
using namespace Aws::Auth;
using namespace Aws::Utils;
#endif

struct MsgState {
  volatile bool replied;
  shared_ptr<vector<string>> ack;
};

CacheServer::CacheServer(string masterip)
    : tpool(THRDPOOLSIZE), port(PORT), master_ip(masterip), obj_server(PORT),
      msg_seq(0) {
  get_aws_credential();
  setup_s3();
  connect_master(master_ip, 1988);
  LOG_INFO << "Started Cache Server";
}

CacheServer::~CacheServer() { close(master_sock); }

void CacheServer::connect_master(string server_name, int portno) {
  LOG_INFO << "Connect to master server " << server_name << ":" << portno;
  ofstream master_file("/dev/shm/master");
  master_file << server_name;
  master_file.close();
  struct sockaddr_in serv_addr;
  struct hostent *server;
  master_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (master_sock < 0)
    DIE("Error opening socket");
  int yes = 1;
  if (setsockopt(master_sock, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)))
    LOG_ERROR << "error: unable to set socket option";
  server = gethostbyname(server_name.c_str());
  if (server == NULL)
    DIE("Can't find host");
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);
  serv_addr.sin_port = htons(portno);
  for (int count = 0; connect(master_sock, (struct sockaddr *)&serv_addr,
                              sizeof(serv_addr)) < 0;
       count++) {
    LOG_ERROR << "Error connecting to master " << strerror(errno) << " attempt "
              << count;
    if (count > 10)
      sleep(20);
    else
      sleep(1);
  }
  pthread_t t;
  if (pthread_create(&t, NULL, &CacheServer::recv_thread_helper, this))
    LOG_ERROR << "Failed to create recv thread";

  int id = send_master("new_server|" + to_string(port));
  auto ack = recv_master(id);
  if (ack->size() != 3 || ack->at(0) != "new_server_ack")
    DIE("Error return msg");
  ip = ack->at(1); // TODO: not correct
}

int CacheServer::send_master(string m) {
  MsgState *s = new MsgState;
  s->replied = false;
  msg_states_lock.lock();
  int msg_id = msg_seq++;
  msg_states[msg_id] = s;
  msg_states_lock.unlock();
  string msg = to_string(msg_id) + "|" + m + "\n";
  LOG_DEBUG << "Sending msg to master " << msg_id << "|" << m;
  int n = write(master_sock, msg.c_str(), msg.size());
  if (n < 0)
    LOG_ERROR << "Error writing to socket";
  return msg_id;
}

void *CacheServer::recv_thread(void) {
  LOG_INFO << "Started master receive thread";
  char buf[MSGSIZE];
  int n;
  MsgState *msg_state_p;
  while (true) {
    n = read(master_sock, buf, MSGSIZE - 1);
    if (n < 0)
      LOG_ERROR << "Error reading from socket";
    buf[n] = '\0';
    string msg(buf);
    boost::trim(msg);
    LOG_DEBUG << "Recvd msg from master: " << msg;
    shared_ptr<vector<string>> parts(new vector<string>());
    boost::split(*parts, msg, boost::is_any_of("|"));
    int msg_id = atoi(parts->at(0).c_str());
    msg_states_lock.lock_shared();
    auto ms = msg_states.find(msg_id);
    msg_state_p = (ms != msg_states.end()) ? ms->second : NULL;
    msg_states_lock.unlock_shared();
    if (msg_state_p) {
      parts->erase(parts->begin());
      msg_state_p->ack = parts;
      msg_state_p->replied = true;
    }
  }
  return 0;
}

void *CacheServer::recv_thread_helper(void *cs) {
  return ((CacheServer *)cs)->recv_thread();
}

shared_ptr<vector<string>> CacheServer::recv_master(int msg_id) {
  msg_states_lock.lock_shared();
  MsgState *msg_state_p = msg_states[msg_id];
  msg_states_lock.unlock_shared();
  while (!msg_state_p->replied)
    ;
  LOG_DEBUG << "msg " << msg_id << " acked";
  shared_ptr<vector<string>> ret = msg_state_p->ack;
  delete msg_state_p;
  msg_states_lock.lock();
  msg_states.erase(msg_id);
  msg_states_lock.unlock();
  return ret;
}

void CacheServer::get_aws_credential() {
#if ENABLES3 == 1
  LOG_INFO << "Reading AWS Credentials";
  ifstream botofile("/root/.boto");
  if (!botofile.is_open())
    DIE("/root/.boto does not exist");
  string line;
  vector<string> parts;
  while (getline(botofile, line)) {
    if (boost::starts_with(line, "aws_access_key_id")) {
      boost::split(parts, line, boost::is_any_of("="));
      access_key_id = boost::trim_copy(parts[1]);
    }
    if (boost::starts_with(line, "aws_secret_access_key")) {
      boost::split(parts, line, boost::is_any_of("="));
      secret_key = boost::trim_copy(parts[1]);
    }
  }
  botofile.close();
#endif
}

void CacheServer::setup_s3() {
#if ENABLES3 == 1
  LOG_INFO << "Setup s3";
  Aws::SDKOptions options;
  Aws::InitAPI(options);
  ClientConfiguration config;
  s3client = Aws::MakeShared<S3Client>(
      "alloc_tag", AWSCredentials(Aws::String(access_key_id.c_str()),
                                  Aws::String(secret_key.c_str())),
      config);
#endif
}

string CacheServer::get_shm_name(string bucket, string key, bool consistency) {
  string result = (consistency ? "~" : "") + bucket + string("~") + key;
  boost::replace_all(result, "/", "~");
  return result;
}

mqd_t CacheServer::get_mqd(string name) {
  mqd_map_lock.lock_shared();
  auto entry = mqd_map.find(name);
  mqd_map_lock.unlock_shared();
  if (entry == mqd_map.end()) {
    mqd_t mq = mq = mq_open(name.c_str(), O_WRONLY);
    if (mq < 0)
      DIE("mq_open %s ret %d ERR %s", name.c_str(), mq, strerror(errno));
    mqd_map_lock.lock();
    mqd_map[name] = mq;
    mqd_map_lock.unlock();
    return mq;
  } else {
    return entry->second;
  }
}

void CacheServer::delete_mqd(string name) {
  mqd_map_lock.lock();
  LOG_DEBUG << "Delete msg queue " << name;
  if (mqd_map.find(name) != mqd_map.end()) {
    mqd_t mq = mqd_map.at(name);
    mqd_map.erase(name);
    string msg = "lambda_exit_ret|/host";
    if (mq_send(mq, msg.c_str(), msg.size(), 0) < 0)
      DIE("Msg send err");
    if (mq_close(mq) != 0) {
      LOG_ERROR << "Failed to close mq " << name << " errno "
                << strerror(errno);
    }
  }
  mqd_map_lock.unlock();
}

void CacheServer::send(string name, string msg) {
  // TODO: keep the descriptor
  mqd_t mq = get_mqd(name);
  LOG_DEBUG << "Send msg to " << name << ", msg = " << msg;
  if (mq_send(mq, msg.c_str(), msg.size(), 0) < 0)
    DIE("Msg send err");
}

void CacheServer::run() {
  while (true)
    sleep(1000);
  char recv_buf[MSGSIZE];

  // Create message queue
  LOG_INFO << "Creating message queue";
  struct mq_attr ma;
  mqd_t mq;
  ma.mq_flags = 0;    // blocking read/write
  ma.mq_maxmsg = 256; // maximum number of messages allowed in queue
  ma.mq_msgsize = MSGSIZE;
  ma.mq_curmsgs = 0; // number of messages currently in queue

  if ((mq = mq_open("/host", O_RDONLY | O_CREAT, 0666, &ma)) < 0)
    DIE("mq_open %d ERR %s", mq, strerror(errno));

  // msg format type|id|content
  while (true) {
    size_t recv_size;
    recv_size = mq_receive(mq, recv_buf, MSGSIZE, NULL);
    if (recv_size < 0)
      DIE("msgrcv");

    LOG_DEBUG << "Received message " << recv_buf;
    std::vector<std::string> strs;
    boost::split(strs, recv_buf, boost::is_any_of("|"));
    if (strs.size() < 2) {
      LOG_ERROR << "Message format error, size < 2";
      continue;
    }

    if (boost::equals(strs[0], "put"))
      handle_put(strs);
    else if (boost::equals(strs[0], "miss"))
      handle_miss(strs);
    else if (boost::equals(strs[0], "delete"))
      handle_delete(strs);
    else if (boost::equals(strs[0], "consistent_lock"))
      handle_consistent_lock(strs);
    else if (boost::equals(strs[0], "consistent_unlock"))
      handle_consistent_unlock(strs);
    else if (boost::equals(strs[0], "consistent_delete"))
      handle_consistent_delete(strs);
    else if (boost::equals(strs[0], "write_s3"))
      handle_write_s3(strs);
    else if (boost::equals(strs[0], "lambda_exit"))
      handle_lambda_exit(strs);
    else
      LOG_ERROR << "Message type error, type: " << strs[0];
    LOG_DEBUG << "handle_" << strs[0] << " called";
  }
}

vector<string> CacheServer::lookup_key(string filename) {
  int id = send_master("lookup|" + filename);
  auto res = recv_master(id);
  if (res->at(0) != "lookup_ack")
    LOG_ERROR << "Lookup ack error: " << res->at(0) << res->at(1);
  vector<string> addrs;
  boost::split(addrs, res->at(1), boost::is_any_of(";"));
  return addrs;
}

bool CacheServer::peer_read(string addr, string filename) {
  if (obj_client.fetch(addr, filename)) {
    LOG_DEBUG << "Successfully fetch " << filename << " from " << addr;
    return true;
  } else {
    LOG_DEBUG << "Failed fetch " << filename << " from " << addr;
    return false;
  }
}

bool CacheServer::s3_write(string bucket, string key, bool consistency) {
#if ENABLES3 == 1
  string shm_name =
      string("/dev/shm/") + get_shm_name(bucket, key, consistency);
  LOG_DEBUG << "PUT bucket " << bucket << " key " << key << " shm " << shm_name;
  auto input_data = Aws::MakeShared<Aws::FStream>("ostream", shm_name.c_str(),
                                                  std::ios_base::in);
  PutObjectRequest objreq;
  objreq.SetBucket(bucket.c_str());
  objreq.SetKey(key.c_str());
  objreq.SetBody(input_data);
  auto outcome = s3client->PutObject(objreq);
  LOG_DEBUG << "S3PUT " << bucket << " " << key << " " << outcome.IsSuccess();
  return outcome.IsSuccess();
#else
  return true;
#endif
}

bool CacheServer::s3_delete(string bucket, string key) {
#if ENABLES3 == 1
  LOG_DEBUG << "DELETE bucket " << bucket << " key " << key;
  DeleteObjectRequest objreq;
  objreq.SetBucket(bucket.c_str());
  objreq.SetKey(key.c_str());
  auto outcome = s3client->DeleteObject(objreq);
  LOG_DEBUG << "S3DELETE " << bucket << " " << key << " "
            << outcome.IsSuccess();
  return outcome.IsSuccess();
#else
  return true;
#endif
}

bool CacheServer::s3_read(string bucket, string key, string shm_name) {
#if ENABLES3 == 1
  LOG_DEBUG << "FETCH bucket " << bucket << " key " << key << " shm "
            << shm_name;
  GetObjectRequest objreq;
  objreq.SetBucket(bucket.c_str());
  objreq.SetKey(key.c_str());
  auto outcome = s3client->GetObject(objreq);
  LOG_DEBUG << "S3GET " << bucket << " " << key << " " << outcome.IsSuccess();
  if (outcome.IsSuccess()) {
    ofstream shm_file(shm_name, std::ofstream::binary);
    if (!shm_file.is_open())
      LOG_FATAL << "Can't open " << shm_name;
    char buf[4096];
    size_t actual_read;
    while (!outcome.GetResult().GetBody().eof()) {
      actual_read = outcome.GetResult().GetBody().read(buf, 4096).gcount();
      shm_file.write(buf, actual_read);
    }
    shm_file.close();
    return true;
  } else {
    return false;
  }
#else
  return true;
#endif
}

void CacheServer::handle_consistent_lock(std::vector<std::string> strs) {
  // Msg from client: consistent_lock|client_q|bucket|key|rw|lambda|duration
  // Msg to master: consistent_lock|read/write|key|lambda|duration_in_sec
  tpool.add(
      [this](string client_q, string bucket, string key, string rw,
             string lambda, string duration) {
        shared_ptr<vector<string>> ack;
        for (int i = 0; i < 10000; i++) {
          int id = send_master("consistent_lock|" + rw + "|" +
                               get_shm_name(bucket, key, true) + "|" + lambda +
                               "|" + duration);
          ack = recv_master(id);
          if (ack->at(1) != "fail")
            break;
          usleep(100000);
        }
        send(client_q, "consistent_lock_ret|/host|" + ack->at(1));
      },
      strs[1], strs[2], strs[3], strs[4], strs[5], strs[6]);
}

void CacheServer::handle_consistent_unlock(std::vector<std::string> strs) {
  // Msg from client: consistent_unlock|client_q|bucket|key|rw|lambda|modified
  // Msg to master: consistent_unlock|read/write|key|lambda|modified
  tpool.add(
      [this](string client_q, string bucket, string key, string rw,
             string lambda, string modified) {
        string ret;
        int id = send_master("consistent_unlock|" + rw + "|" +
                             get_shm_name(bucket, key, true) + "|" + lambda +
                             "|" + modified);
        auto ack = recv_master(id);
        send(client_q, "consistent_unlock_ret|/host|" + ack->at(1));
      },
      strs[1], strs[2], strs[3], strs[4], strs[5], strs[6]);
}

void CacheServer::handle_consistent_delete(std::vector<std::string> strs) {
  // Msg from client: consistent_delete|client_q|bucket|key
  // Msg to master: consistent_delete|key
  tpool.add(
      [this](string client_q, string bucket, string key) {
        string ret;
        int id =
            send_master("consistent_delete|" + get_shm_name(bucket, key, true));
        auto ack = recv_master(id);
        if (ack->at(1) == "success") {
          if (remove(("/dev/shm/" + get_shm_name(bucket, key, true)).c_str()) !=
              0) {
            LOG_ERROR << "removing /dev/shm/" << get_shm_name(bucket, key, true)
                      << " fail";
          }
          if (!s3_delete(bucket, key)) {
            LOG_ERROR << "removing " << bucket << " " << key << " from s3 fail";
          }
        }
        send(client_q, "consistent_delete_ret|/host|" + ack->at(1));
      },
      strs[1], strs[2], strs[3]);
}

void CacheServer::handle_write_s3(std::vector<std::string> strs) {
  tpool.add(
      [this](string client_q, string bucket, string key, bool consistency) {
        bool res = false;
        for (int i = 0; i < 10 && res == false; i++)
          res = s3_write(bucket, key, consistency);
        send(client_q, "write_s3_ret|/host|" + res ? "success" : "fail");
      },
      strs[1], strs[2], strs[3], strs[4][0] == '1');
}

void CacheServer::handle_lambda_exit(std::vector<std::string> strs) {
  tpool.add([this](string client_q) { delete_mqd(client_q); }, strs[1]);
}

void CacheServer::handle_put(std::vector<std::string> strs) {
  tpool.add(
      [this](string client_q, string bucket, string key) {
        string shm_name =
            string("/dev/shm/") + get_shm_name(bucket, key, false);
        int id = send_master("reg|" + get_shm_name(bucket, key, false));
        auto ack = recv_master(id);
        string ret;
        if (ack->at(2) == "success") {
          ret = "put_ret|/host|success|" + get_shm_name(bucket, key, false);
        } else {
          ret = "put_ret|/host|fail|" + get_shm_name(bucket, key, false);
        }
        send(client_q, ret);
        s3_write(bucket, key, false);
      },
      strs[1], strs[2], strs[3]);
}

void CacheServer::handle_miss(std::vector<std::string> strs) {
  tpool.add(
      [this](string client_q, string bucket, string key, bool consistency) {
        string return_msg("");
        string filename = get_shm_name(bucket, key, consistency);
        string shm_name = string("/dev/shm/") + filename;
        int updated = 0;
        // fetch from other nodes
        vector<string> addrs = lookup_key(filename);
        if (addrs[0] == "use_local") {
          return_msg = "success:use_local";
        } else {
          if (access(shm_name.c_str(), F_OK) != -1) {
            if (remove(shm_name.c_str()) != 0)
              LOG_ERROR << "can't remove " << shm_name << " errno "
                        << strerror(errno);
          }
        }
        if (return_msg == "") {
          for (auto &addr : addrs) {
            if (addr != "") {
              LOG_DEBUG << "Reading " << filename << " from peer " << addr;
              if (peer_read(addr, filename)) {
                return_msg = "success:from_peer";
                updated = 1;
                break;
              }
            }
          }
        }
        // fetch from s3
        if (return_msg == "") {
          if (s3_read(bucket, key, shm_name)) {
            return_msg = "success:from_s3";
            updated = 2;
          } else
            return_msg = "fail";
        }
        string msg = "miss_ret|/host|" + return_msg;
        if (updated > 0) {
          int id =
              send_master((updated == 1 ? string("cache") : string("reg")) +
                          "|" + get_shm_name(bucket, key, false));
          auto ack = recv_master(id);
        }
        send(client_q, msg);
        LOG_DEBUG << "Done handle miss, sending " << msg;
      },
      strs[1], strs[2], strs[3], strs[4][0] == '1');
}

void CacheServer::handle_delete(std::vector<std::string> strs) {
  tpool.add(
      [this](string client_q, string bucket, string key) {
        string msg;
        string filename = get_shm_name(bucket, key, false);
        string shm_name = string("/dev/shm/") + filename;

        int id = send_master("delete|" + filename);
        auto ack = recv_master(id);
        if (ack->at(1) == "success") {
          if (remove(("/dev/shm/" + filename).c_str()) != 0) {
            LOG_ERROR << "removing /dev/shm/" << filename << " fail";
          }
          if (!s3_delete(bucket, key)) {
            LOG_ERROR << "removing " << bucket << " " << key << " from s3 fail";
          }
        }
        msg = "delete_ret|/host|" + ack->at(1);
        send(client_q, msg);
        LOG_DEBUG << "Done handle delete, sending " << msg;
      },
      strs[1], strs[2], strs[3]);
}
