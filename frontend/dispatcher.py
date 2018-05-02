import socket, threading, select, sys, time
import pickle, cloudpickle
from sets import Set
from subprocess import call
import random, os, errno


class LineageRecord:
    @staticmethod
    def from_line(line):
        print "LineageLine", line
        [lambda_id, key, version, location] = line.strip().split(",")
        ret = LineageRecord(
            int(lambda_id), key, int(version),
            [x.split(":")[0] for x in location.strip(";").split(";")])
        #print "LineageRecord",ret
        return ret

    def __init__(self, lambda_id, key, version, location):
        self.lambda_id = lambda_id
        self.key = key
        self.version = version
        self.location = location

    def __repr__(self):
        return "%s,%s,%s,%s" % (self.lambda_id, self.key, self.version,
                                self.location)

    def __str__(self):
        return self.__repr__()


class Execution:
    def __init__(self, function, param):
        self.lambda_id = None
        self.function = function
        self.param = param
        self.result = Result()
        self.lineage = None

    def __str__(self):
        return str(self.lambda_id)

    def __repr__(self):
        return str(self.lambda_id)


class LambdaState:
    SUBMITTED = 3
    RUNNING = 0
    FINISHED = 1
    ABORTED = 2

    # invoker: invoker ip string
    # state: running, finished, aborted, waiting
    def __init__(self, invokerIP, lid, socket, state):
        self.invokerIP = invokerIP
        self.lid = lid
        self.state = state
        self.socket = socket
        self.execution = None

    def changeState(self, newState):
        self.state = newState

    def getState(self):
        return self.state

    def getSocket(self):
        return self.socket

    def getResult(self):
        return self.execution.result


class Result:
    def __init__(self):
        self.val = None
        self.evt = threading.Event()

    def abort(self):
        self.evt.set()

    def finish(self, val):
        self.val = val
        self.evt.set()

    def get(self):
        self.evt.wait()
        return self.val


class InvokerState:
    def __init__(self, invokerIP):
        self.lamb = Set()
        self.lock = threading.Lock()
        self.last_heartbeat = time.time()
        self.invokerIP = invokerIP

    def pop(self):
        #self.lock.acquire()
        r = self.lamb.pop()
        #self.lock.release()
        return r

    def add(self, l):
        #self.lock.acquire()
        r = self.lamb.add(l)
        #self.lock.release()

    def remove(self, l):
        self.lamb.remove(l)

    def __len__(self):
        return len(self.lamb)

    def __repr__(self):
        return "%s:%s" % (self.invokerIP, len(self.lamb))


class Dispatcher:
    # memory: memory allocated for each lambda, default 256 MB
    # nslot: number of lambdas per invoker
    def __init__(self,
                 memory=2048,
                 nslot=4,
                 nworker=1,
                 port=random.randint(2048, 65535),
                 openwhisk=False,
                 worker_file=os.path.dirname(os.path.realpath(__file__)) +
                 "/../conf/agent"):
        self.memory = memory
        self.nslot = nslot
        self.nworker = nworker
        self.port = port
        self.openwhisk = openwhisk
        self.worker_file = worker_file
        # start long running lambda
        # wait for all long running lambda to connect
        # obtain the IP address of each long running lambda and keep them into a vector
        self.lambdaStates = {}
        self.lambdaStatesBySocket = {}
        self.invokerState = {}
        self.invokerStateByHeatBeatSocket = {}
        self.executions = {}
        self.__del = False
        self.port = port
        self.has_free = threading.Event()

        self.can_schedule = threading.Event()
        self.can_schedule.set()
        self.failed_lambda_execution = []
        self.failed_invoker = []
        self.start_time = None

        self.master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master.connect(("localhost", 1988))
        self.master.send("0|new_server|1222\n")
        self.master.recv(1024)

        #threading.Thread(target=self.debug).start()

        def loop(port):
            server = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)  # create a socket object
            server.setblocking(0)
            host = socket.gethostname()  # get local machine name
            server.bind((host, port))  # bind to the port
            server.listen(5)

            epoll = select.epoll()
            epoll.register(server.fileno())
            self.connections = {}

            while not self.__del:
                try:
                    events = epoll.poll(1)
                except IOError as e:
                    if e.errno != errno.EINTR:
                        print "epoll error", str(e)
                    continue
                except Exception as e:
                    print "epoll error", str(e)
                    continue
                #print readable, writable, exceptional
                # Handle inputs
                for fd, event_type in events:
                    if fd == server.fileno():
                        # A "readable" server socket is ready to accept a connection
                        connection, client_address = server.accept()
                        #print >> sys.stderr, 'new connection from', client_address
                        connection.setblocking(0)
                        child_fd = connection.fileno()
                        epoll.register(child_fd)
                        self.connections[child_fd] = connection
                    else:
                        s = self.connections[fd]
                        if event_type & select.EPOLLIN:
                            try:
                                data = s.recv(1024 * 4)
                            except Exception as e:
                                print str(e)
                                continue
                            if data:
                                if data.startswith("stop"):
                                    epoll.unregister(s.fileno())
                                    if fd in self.connections:
                                        del self.connections[fd]
                                    #print >> sys.stderr, 'closed socket'
                                    s.close()
                                    continue
                                # A readable client socket has data
                                #print >> sys.stderr, 'received "%s" from %s\n' % (data, s.getpeername())
                                # record info when connection first established
                                if data.startswith("heartbeat"):
                                    invokerIP = data.split(" ")[1].strip()
                                    if invokerIP not in self.invokerState:
                                        self.invokerState[
                                            invokerIP] = InvokerState(
                                                invokerIP)
                                    if s not in self.invokerStateByHeatBeatSocket:
                                        self.invokerStateByHeatBeatSocket[
                                            s] = self.invokerState[invokerIP]
                                    self.invokerState[
                                        invokerIP].last_heartbeat = time.time(
                                        )

                                elif data.startswith("Lambda Connected."):
                                    lid = data.split(":")[1]
                                    invokerIP = lid.split("_")[0]

                                    ls = LambdaState(invokerIP, lid, s,
                                                     LambdaState.FINISHED)
                                    self.lambdaStates[lid] = ls
                                    self.lambdaStatesBySocket[s] = ls
                                    if invokerIP not in self.invokerState:
                                        self.invokerState[
                                            invokerIP] = InvokerState(
                                                invokerIP)
                                    self.invokerState[invokerIP].add(lid)
                                else:
                                    ls = self.lambdaStatesBySocket[s]
                                    if ls.invokerIP not in self.invokerState:
                                        print "ls.invokerIP not in self.invokerState, data = %s" % data
                                        self.lambda_failure(s)
                                        epoll.unregister(s.fileno())
                                        if fd in self.connections:
                                            del self.connections[fd]
                                    elif data.startswith("subLambda Running."):
                                        lambda_id = int(
                                            data[len("subLambda Running.") +
                                                 6:])
                                        ls.changeState(LambdaState.RUNNING)
                                        if not (ls.execution.lambda_id is None
                                                or ls.execution.lambda_id ==
                                                lambda_id):
                                            print ls.execution.lambda_id, lambda_id, id(
                                                ls.execution), ls.lid
                                            print data
                                            assert False
                                        ls.execution.lambda_id = lambda_id
                                        self.executions[
                                            lambda_id] = ls.execution
                                    elif data.startswith(
                                            "subLambda Finished."):
                                        res = data[len(
                                            "subLambda Finished. ---> "):]
                                        ls.getResult().finish(
                                            pickle.loads(res))
                                        ls.execution = None
                                        ls.changeState(LambdaState.FINISHED)
                                        self.invokerState[ls.invokerIP].add(
                                            ls.lid)
                                        self.has_free.set()
                                    elif data.startswith("subLambda Aborted."):
                                        print data, ls.lid
                                        ls.getResult().abort()
                                        ls.execution = None
                                        ls.changeState(LambdaState.ABORTED)
                                        self.invokerState[ls.invokerIP].add(
                                            ls.lid)
                                        self.has_free.set()
                                    else:
                                        print >> sys.stderr, 'Don\'t know what happened "%s" from %s' % (
                                            data, s.getpeername())
                            else:
                                #print >> sys.stderr, 'Socket disconnected {}'.format(s.getpeername())
                                if s in self.lambdaStatesBySocket:
                                    print self.lambdaStatesBySocket[
                                        s].lid, "died"
                                    self.lambda_failure(s)
                                    epoll.unregister(s.fileno())
                                    if fd in self.connections:
                                        del self.connections[fd]
                                if s in self.invokerStateByHeatBeatSocket:
                                    print self.invokerStateByHeatBeatSocket[
                                        s].invokerIP, "died"
                                    epoll.unregister(s.fileno())
                                    if fd in self.connections:
                                        del self.connections[fd]
                                    self.invoker_failure(
                                        self.invokerStateByHeatBeatSocket[s]
                                        .invokerIP)
                        elif event_type & select.EPOLLERR or event_type & select.EPOLLHUP:
                            print >> sys.stderr, 'handling exceptional condition for', s.getpeername(
                            )
                            # Stop listening for input on the connection
                            epoll.unregister(s.fileno())
                            if fd in self.connections: del self.connections[fd]
                            s.close()
            epoll.unregister(server.fileno())
            epoll.close()
            server.close()

        # start loop thread
        self.thread = threading.Thread(target=loop, args=(self.port, ))
        self.thread.start()

        if openwhisk:
            # initialization finished, it's time to start long running lambda
            call(
                "cp worker.py __main__.py; cp ../cacheclient/cacheclient.py ./",
                shell=True)
            call(
                "zip -r l.zip __main__.py cloudpickle.py cacheclient.py io_man.py",
                shell=True)
            call(
                "/home/ubuntu/openwhisk/bin/wsk -i action delete long-lambda",
                shell=True)
            call(
                "/home/ubuntu/openwhisk/bin/wsk -i action create long-lambda --kind python:2 l.zip -m {}".
                format(memory),
                shell=True)
            for lid in range(nslot * nworker):
                call(
                    "/home/ubuntu/openwhisk/bin/wsk -i action invoke long-lambda \
           --param host {} --param port {} --param id {}".format(
                        socket.gethostname(), port, lid),
                    shell=True)
        else:
            #start ervers
            self.start_longrunning_lambda()

        while len(self.lambdaStates) < nslot * nworker:
            print len(self.lambdaStates), "lambdas connected"
            time.sleep(5)
        print >> sys.stderr, 'Initialization finished'

    def recover(self):
        print "recover from failure"
        time.sleep(5)
        print "starting actual recovery %s, failed lambda: %s, failed ink %s" % (
            time.time() - self.start_time, str(self.failed_lambda_execution),
            str(self.failed_invoker))
        for execu in self.failed_lambda_execution:
            if execu.lambda_id is not None:
                print "getting record for lambda%s" % execu.lambda_id
                self.executions[execu.lambda_id].lineage = [
                    LineageRecord.from_line(l)
                    for l in self.get_lineage(execu.lambda_id)
                ]
        execution_plan = [set(self.failed_lambda_execution)]
        for failed in self.failed_lambda_execution:
            if failed.lineage is not None and failed.lambda_id is not None:
                #process on execution
                lambda_needs_rerun = [[failed.lambda_id]]
                for lr in failed.lineage:
                    for ss in range(len(lambda_needs_rerun)):
                        curr_stage = lambda_needs_rerun[ss]
                        if lr.lambda_id in curr_stage:
                            lost_cache = 0
                            for loc in lr.location:
                                if loc in self.failed_invoker:
                                    lost_cache += 1
                            if lost_cache == len(
                                    lr.location
                            ) and lr.version not in lambda_needs_rerun:
                                while len(lambda_needs_rerun) <= ss + 2:
                                    lambda_needs_rerun.append([])
                                lambda_needs_rerun[ss + 1].append(lr.version)
                print "rerun lambdas lineage %s" % lambda_needs_rerun
                for i in range(1, len(lambda_needs_rerun)):
                    for lamb in lambda_needs_rerun[i]:
                        if len(execution_plan) < i + 1:
                            execution_plan.append(set())
                        #create lineage for the execution
                        target_exec = self.executions[lamb]
                        target_exec.lineage = []
                        needed = [target_exec.lambda_id]
                        for ll in failed.lineage:
                            if ll.lambda_id in needed:
                                target_exec.lineage.append(ll)
                            if ll.lambda_id in needed and ll.version not in needed:
                                needed.append(ll.version)
                        #
                        execution_plan[i].add(target_exec)
        print "execution plan:"
        for s in execution_plan:
            print s
        execution_plan.reverse()
        for s in execution_plan:
            self.resubmit(s)

        msg = "0|force_release_lock|%s\n" % (",".join([
            str(x.lambda_id) for x in self.failed_lambda_execution
            if x.lambda_id is not None
        ]))
        self.master.sendall(msg)
        self.master.recv(1024)
        self.can_schedule.set()

    def notify_fail(self):
        if self.can_schedule.is_set():
            threading.Thread(target=self.recover).start()
        self.can_schedule.clear()

    def invoker_failure(self, invokerIP):
        self.notify_fail()
        print "Handling %s failure" % invokerIP
        #clear lambda on the invoker
        while len(self.invokerState[invokerIP]) > 0:
            lid = self.invokerState[invokerIP].pop()
            print "removing lid", lid
            s = self.lambdaStates[lid].socket
            del self.lambdaStatesBySocket[s]
            del self.lambdaStates[lid]
        #clear invoker state
        del self.invokerState[invokerIP]
        invoker_socket = None
        for k, v in self.invokerStateByHeatBeatSocket.iteritems():
            if v.invokerIP == invokerIP:
                invoker_socket = k
        if invoker_socket is not None:
            del self.invokerStateByHeatBeatSocket[invoker_socket]
        self.failed_invoker.append(invokerIP)
        print "Removed all states associated to %s" % invokerIP

    def lambda_failure(self, s):
        print "lambda_failure on socket %s" % s
        self.notify_fail()
        lid = self.lambdaStatesBySocket[s].lid
        invokerIP = self.lambdaStatesBySocket[s].invokerIP
        if self.lambdaStatesBySocket[s].getState() in [
                LambdaState.SUBMITTED, LambdaState.RUNNING
        ]:
            lambda_id = self.lambdaStatesBySocket[s].execution.lambda_id
            print lambda_id, "on", lid, "failed"
            self.failed_lambda_execution.append(
                self.lambdaStatesBySocket[s].execution)
        del self.lambdaStatesBySocket[s]
        del self.lambdaStates[lid]
        if invokerIP in self.invokerState and lid in self.invokerState[invokerIP].lamb:
            self.invokerState[invokerIP].remove(lid)
        print "socket %s failure is handled" % s

    def run(self, s, cmd):
        l = "ssh -f -o StrictHostKeyChecking=no %s '%s'" % (s, cmd)
        os.system(l)

    def start_longrunning_lambda(self):
        i = 0
        with open(self.worker_file) as worker_file:
            servers = [l.strip() for l in worker_file][0:self.nworker]
        for s in servers:
            cmd = "ulimit -n 65535; ulimit -q 100000000; cd %s;" % os.path.dirname(
                os.path.realpath(__file__))
            cmd += " unbuffer python heartbeat.py %s %s %s > /tmp/heartbeat & " % (
                socket.gethostname(), self.port, s + "_heartbeat")
            cmd += " unbuffer python gc.py %s %s %s > /tmp/gc & " % (
                socket.gethostname(), self.port, s + "_gc")
            for k in range(self.nslot):
                lid = s + "_" + str(i)
                cmd += "unbuffer python worker_main.py %s %s %s 2>&1 > /tmp/invoker_%s & " % (
                    socket.gethostname(), self.port, lid, lid)
                i += 1
            self.run(s, cmd)
        time.sleep(5)

    def get_loc(self, keys):
        query = "0|lookup|%s\n" % '/'.join(keys)
        assert len(query) < 1024 * 1024
        self.master.send("0|lookup|%s\n" % '/'.join(keys))
        ack = self.master.recv(1024 * 1024).strip()
        addrs = ack.split("|")[2].split("/")
        assert len(addrs) == len(keys)
        return {keys[i]: addrs[i].split(";") for i in range(len(keys))}

    def debug(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        debug_port = random.randint(10000, 60000)
        print "__debug_port__", debug_port
        os.system("echo %s > /tmp/debug_port" % debug_port)
        sock.bind(("", debug_port))
        sock.listen(1)
        connection, client_address = sock.accept()
        try:
            while True:
                data = connection.recv(1024).strip()
                if data == "exit":
                    break
                elif data == "pending":
                    res = ""
                    for k, v in self.lambdaStates.iteritems():
                        if v.state != LambdaState.FINISHED:
                            res += k + "\n"
                    connection.sendall(res)
                else:
                    connection.sendall("Unknown\n")
        except Exception as e:
            print str(e)
        connection.close()
        sock.close()

    def get_lineage(self, lambda_id):
        self.master.sendall("0|lineage|%s\n" % lambda_id)
        data = self.master.recv(1024 * 1024)
        return [
            l for l in data.split("|")[2].strip().strip("$").split("$")
            if len(l) > 0
        ]

    def get_free_counts(self):
        # return a map<invoker, free_count>, where invoker is a string, free_count is int
        return self.invokerState

    def submit_lambda(self, invokerIP, function, params, timeout=300):
        # serialize function using cloudpickle
        # send the function to long running lambda, invoke the function
        # invoker should return if the function is invoked successfully.
        # change the status of lambda to "running"
        # the lambda should be aborted in timeout
        ls = self.get_free_ls(invokerIP)
        if ls is None:
            return None
        else:
            pf = cloudpickle.dumps(function)
            if not hasattr(params, '__iter__'):
                params = [params]
            pp = cloudpickle.dumps(params)
            return self.submit_lambda_with_execution(ls, Execution(pf, pp))

    def get_free_ls(self, invokerIP):
        free_count = len(self.invokerState[invokerIP])
        if free_count <= 0:
            print >> sys.stderr, 'invoker full'
            return None
        else:
            id = self.invokerState[invokerIP].pop()
            return self.lambdaStates[id]

    def submit_lambda_with_execution(self, ls, execution):
        if self.start_time is None:
            self.start_time = time.time()
        assert ls.execution is None, "execution is not None, lid %s" % ls.lid
        ls.execution = execution
        ls.changeState(LambdaState.SUBMITTED)
        s = ls.getSocket()
        pf_size = len(execution.function)
        pp_size = len(execution.param)
        extra = {}
        if execution.lambda_id is not None:
            extra["lambda_id"] = "lambda%s" % execution.lambda_id
            extra["replay_inputs"] = {
                x.key: x
                for x in execution.lineage
                if x.lambda_id == execution.lambda_id
            }
        p_extra = cloudpickle.dumps(extra)
        p_extra_size = len(p_extra)
        s.sendall("%s,%s,%s,%s%s%s" %
                  (pf_size, pp_size, p_extra_size, execution.function,
                   execution.param, p_extra))
        return ls.execution.result

    def map(self, func, param, async=False):
        return self.map_multi([(func, param)], async=async)

    def map_multi(self, func_param_list, async=False):
        start_time = time.time()
        res = []
        for fp_pair in func_param_list:
            function = fp_pair[0]
            params = fp_pair[1]
            for p in params:
                submitted = False
                while not submitted:
                    #self.can_schedule.wait()
                    invokers = self.invokerState.keys()
                    random.shuffle(invokers)
                    for ivk in invokers:
                        lset = self.invokerState[ivk]
                        if len(lset) > 0:
                            r = self.submit_lambda(ivk, function, p)
                            res.append(r)
                            submitted = True
                            break
                    if not submitted:
                        #self.has_free.clear()
                        #self.has_free.wait()
                        time.sleep(0.01)
        print "Submitted %s tasks" % len(res)
        if async:
            return res
        res = [r.get() for r in res]
        print "Duration", time.time() - start_time
        return res

    def resubmit(self, executions):
        start_time = time.time()
        res = []
        for e in executions:
            submitted = False
            while not submitted:
                invokers = self.invokerState.keys()
                random.shuffle(invokers)
                for ivk in invokers:
                    lset = self.invokerState[ivk]
                    if len(lset) > 0:
                        ls = self.get_free_ls(ivk)
                        print "lambda %s is submitted to %s" % (e.lambda_id,
                                                                ls.lid)
                        r = self.submit_lambda_with_execution(ls, e)
                        res.append(r)
                        submitted = True
                        break
                if not submitted:
                    time.sleep(0.01)
        print "Resubmitted %s tasks" % len(executions)
        [r.get() for r in res]
        print "Resubmitted tasks done %s" % (time.time() - start_time)

    def delete(self):
        # close the sockets
        # terminate long running lambda
        if not self.__del:
            self.__del = True
            for id in self.lambdaStates:
                s = self.lambdaStates[id].getSocket()
                #print >> sys.stderr, 'sending stop %s' % (id)
                s.send("stop")
                s.close()
            for s in self.invokerStateByHeatBeatSocket:
                s.send("stop")
                s.close()
            time.sleep(2)
            self.thread.join(3)
            return
