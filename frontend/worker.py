import socket, sys, time
import pickle
import random
import traceback
from cacheclient import *
from io_man import IOMan
import gc
import boto3


def main(args):
    if args:
        host = args.get("host", "error")  # assign dispatcher hostname
        port = int(args.get("port",
                            "12345"))  # port that dispatcher listens to
        ip = open("/dev/shm/ip-address", "r").read().strip()
        lambda_id = ip + "_" + str(args.get("id", "error"))
    else:
        host = sys.argv[1]
        port = int(sys.argv[2])
        lambda_id = sys.argv[3]
        ip = lambda_id.split("_")[0]

    ID = "Lambda ID:%s" % lambda_id
    CONNECTED = "Lambda Connected. %s" % (ID)
    RUNNING = "subLambda Running."
    FINISHED = "subLambda Finished. ---> "
    ABORTED = "subLambda Aborted."

    server_address = (host, port)

    # Create a TCP/IP socket
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # Connect the socket to the port where the server is listening
    print 'connecting to %s port %s' % server_address
    client.connect(server_address)
    # Send initial message to dispatcher
    print '%s: sending "%s"' % (client.getsockname(), CONNECTED)
    client.sendall(CONNECTED)

    result = "Nothing"
    while True:
        # Read instruction from dispatcher
        data = client.recv(1024 * 1024)
        print '%s: received %s bytes' % (client.getsockname(), len(data))
        if data.startswith("stop") or not data:
            client.sendall("stop")
            time.sleep(5)
            client.close()
            print "I'm done"
            return {"result": result}
        while True:
            try:
                data_arr = data.split(",", 3)
                print "len", len(data_arr), int(data_arr[0]), int(
                    data_arr[1]), len(data_arr[2])
                data_f = data_arr[3][0:int(data_arr[0])]
                data_p = data_arr[3][int(data_arr[0]):
                                     int(data_arr[0]) + int(data_arr[1])]
                data_extra = data_arr[
                    3][int(data_arr[0]) + int(data_arr[1]):
                       int(data_arr[0]) + int(data_arr[1]) + int(data_arr[2])]
                func = pickle.loads(data_f)
                params = list(pickle.loads(data_p))
                extra = pickle.loads(data_extra)
                print "params:", params
                print "extra:", extra

                ucache_vars = {}
                ucache_vars["cache"] = CacheClient(extra=extra)
                ucache_vars["CacheClient"] = globals()["CacheClient"]
                ucache_vars["IOMan"] = globals()["IOMan"]
                client.sendall(RUNNING + ucache_vars["cache"].lambda_id)

                if func.__code__.co_argcount < len(params):
                    raise Exception("Function %s, Provided %s" % (
                        func.__code__.co_varnames[0:func.__code__.co_argcount],
                        params))
                elif func.__code__.co_argcount == len(params):
                    pass
                else:
                    l_params = len(params)
                    for v in func.__code__.co_varnames[
                            l_params:func.__code__.co_argcount]:
                        if v in ucache_vars:
                            params.append(ucache_vars[v])
                        else:
                            raise Exception(
                                "Invalid parameter %s. Provided %s, Requires %s"
                                % (v, params[0:l_params],
                                   func.__code__.co_varnames[
                                       0:func.__code__.co_argcount]))
                result = func(*params)
                ucache_vars["cache"].commit_write()
                print "result:", result
                func = None
                params = None
            except LockException as e:
                print "LockException:", str(e)
                ucache_vars["cache"].shutdown()
                print "retry in 3 sec\n\n"
                time.sleep(3)
                continue
            except Exception as e:
                print "Exception:", str(e)
                traceback.print_tb(sys.exc_info()[2])
                client.sendall(ABORTED + str(e))
            else:
                client.sendall(FINISHED + pickle.dumps(result))
            ucache_vars["cache"].shutdown()
            print "Lambda execution finished.\n\n\n\n"
            break
