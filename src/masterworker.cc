
#include "masterworker.h"
#include "log.h"
#include "master.h"
#include <arpa/inet.h>
#include <boost/algorithm/string.hpp>
#include <ctime>
#include <fcntl.h>
#include <iomanip>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using namespace std;

MasterWorker::MasterWorker(Master &master, int socket)
    : master(master), socket(socket) {
  init();
}

void MasterWorker::init() {
  int yes = 1;
  if (setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(int)))
    LOG_ERROR << "error: unable to set socket option TCP_NODELAY";
  // We want non-blocking reads
  // fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK);
  struct sockaddr_in addr;
  socklen_t addr_size = sizeof(struct sockaddr_in);
  int res = getpeername(socket, (struct sockaddr *)&addr, &addr_size);
  char ip_str[20];
  strcpy(ip_str, inet_ntoa(addr.sin_addr));
  ip = ip_str;
  this->addr = ip;
  LOG_DEBUG << "Connection from " << ip;
}

void MasterWorker::exit() { close(socket); }

string MasterWorker::readline() {
  string line("");
  char buf[1024 * 1024];
  int n = 0;
  while (n = read(socket, buf, 1024 * 1024)) {
    char *pos = std::find(buf, buf + n, '\n');
    if (pos != buf + n) {
      assert(pos == buf + n - 1);
      *pos = '\0';
      line += buf;
      break;
    }
    line += buf;
  }
  return line;
}

void MasterWorker::do_action() {
  vector<string> cmds;
  vector<string> rets;
  string ret, msg;
  msg = readline();
  if (msg == "")
    return;
  cmds.clear();
  rets.clear();
  LOG_DEBUG << "Received msg<-" << addr << ":lambda" << lambda_seq << " "
            << msg;
  boost::split(cmds, msg, boost::is_any_of("/"));
  for (auto const &cmd : cmds) {
    rets.push_back(handle_msg(cmd));
  }
  ret = boost::algorithm::join(rets, "/");
  LOG_DEBUG << "Sending msg->" << addr << ":lambda" << lambda_seq << " " << ret;
  string response = ret + "\n";
  assert(response.size() < 1024 * 1024);
  int total_written = 0, written = 0;
  while (true) {
    if ((written = write(socket, response.c_str() + total_written,
                         response.size() - total_written)) < 0) {
      if (errno == EAGAIN)
        continue;
      else if (errno == ECONNRESET || errno == EPIPE)
        return;
      else {
        std::cerr << "error: unable to write socket " << socket << std::endl;
        break;
      }
    }
    total_written += written;
    if (total_written >= response.size())
      break;
  }
  assert(total_written == response.size());
}

void MasterWorker::run() {
  while (true) {
    do_action();
  }
  LOG_DEBUG << "Connection disconnected";
  exit();
}

string MasterWorker::handle_msg(string msg) {
  string ret;
  vector<string> parts;
  boost::split(parts, msg, boost::is_any_of("|"));
  string id = parts[0];
  parts.erase(parts.begin());
  if (parts[0] == "new_server")
    ret = handle_new_server(parts);
  else if (parts[0] == "reg")
    ret = handle_reg(parts);
  else if (parts[0] == "cache")
    ret = handle_cache(parts);
  else if (parts[0] == "uncache")
    ret = handle_uncache(parts);
  else if (parts[0] == "lookup")
    ret = handle_lookup(parts);
  else if (parts[0] == "delete")
    ret = handle_delete(parts);
  else if (parts[0] == "consistent_lock")
    ret = handle_consistent_lock(parts);
  else if (parts[0] == "consistent_unlock")
    ret = handle_consistent_unlock(parts);
  else if (parts[0] == "consistent_delete")
    ret = handle_consistent_delete(parts);
  else if (parts[0] == "lineage")
    ret = handle_lineage(parts);
  else if (parts[0] == "failover_write_update")
    ret = handle_failover_write_update(parts);
  else if (parts[0] == "force_release_lock")
    ret = handle_force_release_lock(parts);
  else {
    LOG_ERROR << "error msg type";
    ret = string("");
  }
  return id + "|" + ret;
}

string MasterWorker::handle_new_server(vector<string> parts) {
  // new_server|port|lambda_id(optional)
  port = parts[1];
  addr =
      ip + ":" + port; // TODO addr should be cacheserver addr, not lambda addr
  LOG_DEBUG << "handle new_server from " << addr;
  if (parts.size() < 3 || parts[2] == "") {
    lambda_seq = master.registry.get_lambda_seq();
  } else {
    lambda_seq = atoi(parts[2].substr(6).c_str());
  }
  return "new_server_ack|" + parts[1] + "|" + to_string(lambda_seq);
}

string MasterWorker::handle_reg(vector<string> parts) {
  bool ret = master.registry.reg_key(parts[1], addr);
  return "reg_ack|" + parts[1] + "|" + (ret ? "success" : "fail");
}

string MasterWorker::handle_cache(vector<string> parts) {
  bool ret = master.registry.cache_key(parts[1], addr);
  return "cache_ack|" + parts[1] + "|" + (ret ? "success" : "fail");
}

string MasterWorker::handle_uncache(vector<string> parts) {
  bool ret = master.registry.uncache_key(parts[1], addr);
  return "uncache_ack|" + parts[1] + "|" + (ret ? "success" : "fail");
}

string MasterWorker::handle_lookup(vector<string> parts) {
  string ret = master.registry.get_location(parts[1], addr);
  return "lookup_ack|" + ret;
}

string MasterWorker::handle_consistent_lock(vector<string> parts) {
  // consistent_lock|read/write|key|lambda|duration_in_sec|use_s3|snap|check_loc|version
  string pre_check_loc = "";
  if (parts[5] == "s3" || parts[7] == "check_loc")
    // TODO: state may change after get_location....
    pre_check_loc = master.registry.get_location(parts[2], addr);

  LOG_DEBUG << "handle consistent_lock from " << addr << " pre_check_loc "
            << pre_check_loc;

  if (parts[1] == "write" || (parts[5] == "s3" && pre_check_loc == "")) {
    LOG_DEBUG << "write branch";
    string ret, loc;
    ret = master.registry.consistent_write_lock(
        parts[2], addr, parts[3], atoi(parts[4].c_str()), parts[6] == "snap");
    loc = master.registry.get_location(parts[2], addr);
    if (ret == "success") {
      LOG_DEBUG << "ret = success";
      uint lambda_id = atoi(parts[3].substr(6).c_str());
      if (parts[7] == "check_loc" &&
          parts[5] != "s3") { // check_loc iff open as rw
        LOG_DEBUG << "check_loc and s3";
        uint key_version = master.registry.get_key_version(parts[2], true);
        master.registry.register_lineage(lambda_id, parts[2], key_version);
      }
      master.registry.register_lock(lambda_id, parts[2], true);
    } else {
      if (parts[8] == "recent") {
        LOG_DEBUG << "part[8] == recent, lock failed";
      } else {
        LOG_DEBUG << "part[8] != recent";
        ret = "success";
        loc = master.registry.get_location_version(parts[2], addr,
                                                   atoi(parts[8].c_str()));
      }
    }
    return "consistent_lock_ack|" + ret + "|write|" + loc;
  } else if (parts[1] == "read") {
    LOG_DEBUG << "read branch";
    string ret, loc;
    if (parts[8] == "recent") {
      // LOG_DEBUG << "parts[8] == recent";
      ret = master.registry.consistent_read_lock(
          parts[2], addr, parts[3], atoi(parts[4].c_str()), parts[6] == "snap");
      loc = master.registry.get_location(parts[2], addr);
      if (ret == "success") {
        uint lambda_id = atoi(parts[3].substr(6).c_str());
        uint key_version = master.registry.get_key_version(parts[2], false);
        master.registry.register_lineage(lambda_id, parts[2], key_version);
        master.registry.register_lock(lambda_id, parts[2], false);
      }
    } else {
      // LOG_DEBUG << "parts[8] != recent";
      ret = "success";
      if (parts[5] != "s3") {
        // LOG_DEBUG << "get_location_version";
        loc = master.registry.get_location_version(parts[2], addr,
                                                   atoi(parts[8].c_str()));
      } else { // you just want the newest version
        // LOG_DEBUG << "get_location";
        loc = master.registry.get_location(parts[2], addr);
      }
    }
    return "consistent_lock_ack|" + ret + "|read|" + loc;
  } else {
    return "consistent_lock_ack|wrong_cmd";
  }
}

string MasterWorker::handle_consistent_unlock(vector<string> parts) {
  // consistent_unlock|read/write|key|lambda|modified
  if (parts[1] == "write") {
    string ret = master.registry.consistent_write_unlock(
        parts[2], addr, parts[3], parts[4][0] == '1');
    return "consistent_unlock_ack|" + ret;
  } else if (parts[1] == "read") {
    string ret = master.registry.consistent_read_unlock(
        parts[2], addr, parts[3], parts[4][0] == '1');
    return "consistent_unlock_ack|" + ret;
  } else {
    return "consistent_unlock_ack|wrong_cmd";
  }
}

string MasterWorker::handle_consistent_delete(vector<string> parts) {
  // consistent_delete|key|lambda
  string ret = master.registry.consistent_delete(parts[1], parts[2]);
  return "consistent_delete_ack|" + ret;
}

string MasterWorker::handle_delete(vector<string> parts) {
  // delete|key
  string ret = master.registry.delete_key(parts[1]);
  return "delete_ack|" + ret;
}

string MasterWorker::handle_lineage(vector<string> parts) {
  // lineage|lambda_id
  string ret = master.registry.get_lineage(atoi(parts[1].c_str()));
  return "lineage_ack|" + ret;
}

string MasterWorker::handle_failover_write_update(vector<string> parts) {
  // failover_write_update|key|version|lambda_id
  return "failover_write_update_ack|" +
         master.registry.failover_write_update(parts[1], atoi(parts[2].c_str()),
                                               addr, parts[3]);
}

string MasterWorker::handle_force_release_lock(vector<string> parts) {
  // force_release_lock|lambdas
  vector<string> lambda_strings;
  vector<uint> lambdas;
  boost::split(lambda_strings, parts[1], boost::is_any_of(","));
  for (string ls : lambda_strings)
    lambdas.push_back(atoi(ls.c_str()));
  return "force_release_lock_ack|" +
         master.registry.force_release_lock(lambdas);
}

void *MasterWorker::pthread_helper(void *worker) {
  static_cast<MasterWorker *>(worker)->run();
  return nullptr;
}
