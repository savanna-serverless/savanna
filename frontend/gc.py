import socket, sys, time, datetime, os
import multiprocessing
from cacheclient import STORAGE
import boto3
import posix_ipc, mmap

HIGH_MARK = int(0 * 1024 * 1024 * 1024)
POOL_SIZE = 25


def has_storage():
    if HIGH_MARK == 0 or not os.path.exists(STORAGE):
        return True
    st = os.statvfs(STORAGE)
    return st.f_bavail * st.f_frsize > HIGH_MARK


def get_files():
    if os.path.exists(STORAGE):
        os.chdir(STORAGE)
        files = [(os.path.getmtime(f), f) for f in os.listdir(STORAGE)
                 if not f.startswith("~~tmp~")]
        files.sort()
        return files


def get_bucket_key(fn):
    [bucket, key] = fn.replace(STORAGE, "").strip("~").split("~", 1)
    key = key.replace("~", "/")
    return (bucket, key)


def upload_and_remove(link_name):
    link = STORAGE + link_name
    realname = os.path.realpath(link)
    os.unlink(link)
    (bucket, key) = get_bucket_key(link_name)
    print "uploading %s to %s %s" % (realname, bucket, key)
    client = boto3.client("s3")
    client.upload_file(realname, bucket, key)
    os.remove(realname)
    print "finished uploading %s to %s %s" % (realname, bucket, key)
    return True


def do_gc(pool):
    print "doing gc"
    while True:
        res = []
        for f in get_files()[0:POOL_SIZE]:
            fn = f[1]
            r = pool.apply_async(upload_and_remove, (fn, ))
            res.append(r)
        [u.get() for u in res]
        if has_storage():
            print "finished gc"
            return


def main(args):
    pool = multiprocessing.Pool(processes=POOL_SIZE)
    shm = posix_ipc.SharedMemory(
        "savanna_gc", flags=posix_ipc.O_CREAT, mode=0666, size=10)
    mm = mmap.mmap(shm.fd, shm.size)
    mm[0] = '1'
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
        time.sleep(1)
        if not has_storage():
            mm[0] = '0'
            do_gc(pool)
        mm[0] = '1'
    pool.close()


main(None)
