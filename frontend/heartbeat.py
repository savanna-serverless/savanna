import socket, sys, time, datetime


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

    server_address = (host, port)
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(server_address)
    client.setblocking(0)

    while True:
        print "%s sending heartbeat" % datetime.datetime.now()
        client.send("heartbeat %s" % ip)
        try:
            data = client.recv(1024)
            if data.startswith("stop"):
                print "heartbeat exit"
                client.send("stop")
                time.sleep(5)
                client.close()
                break
        except Exception:
            pass
        time.sleep(10)


main(None)
