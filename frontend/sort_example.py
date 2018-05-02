from dispatcher import *
import time
import numpy
import datetime
import ConfigParser

io_mode = "c"  #c , nc, s3, c is consistent mode, nc is inconsistent mode, s3 shuffles with s3.
parallelism = 4  #number of total concurrent invocations
n_partition = 2 * parallelism  #number of total partitions to sort
partition_size = 16  #sort partition size, in MB
obj_size = 100  #sort object size
n_worker_machine = 1  #number of worker machines to use
n_slot = 4  #number of concurrent invocations per machine

conf = ConfigParser.ConfigParser()
conf.read("../conf/conf")
s3_bucket = conf.get("Savanna", "s3_bucket")
assert s3_bucket != "None"


def gen(i):
    import string, random, StringIO, boto3
    chars = string.digits
    f = StringIO.StringIO()
    s3 = boto3.client("s3")
    for x in range(partition_size * 1024 * 1024 / obj_size):
        v = ''.join(random.choice(chars) for _ in range(obj_size - 1))
        f.write(v + "\n")
    s3.put_object(
        Bucket=s3_bucket,
        Key="sort_input_%s/f%s" % (partition_size, i),
        Body=f.getvalue())
    return i


def sort_map(i, cache, IOMan):
    io = IOMan(io_mode, i, s3_bucket, "sort_shuffle", cacheclient=cache)
    files = [
        io.s3_read(s3_bucket, "sort_input_%s/f%s" % (partition_size, k))
        for k in range(i, n_partition, parallelism)
    ]
    st = time.time()
    for o in files:
        while True:
            s = o.read(obj_size)
            if len(s) == 0:
                break
            key = int(int(s[0:5]) / (100000 / parallelism))
            io.write(key, s)
    duration = time.time() - st
    io.close()
    return duration


def sort_reduce(k, cache, IOMan):
    io = IOMan(io_mode, k, s3_bucket, "sort_result", cacheclient=cache)
    values = []
    start_time = time.time()
    for i in range(parallelism):
        fin = io.read(s3_bucket, "sort_shuffle", i)
        while True:
            v = fin.read(obj_size)
            if len(v) == 0:
                break
            values.append(v)
    sort_time = time.time()
    values.sort()
    for v in values:
        io.write(0, v, async_s3=True)
    io.close()
    end_time = time.time()
    #cache.fsync()
    return (sort_time - start_time, end_time - sort_time)


def run():
    d = Dispatcher(nslot=n_slot, nworker=n_worker_machine, openwhisk=False)
    start = time.time()
    if io_mode == "gen":
        d.map(gen, range(n_partition))
    else:
        d.map(sort_map, range(parallelism))
        d.map(sort_reduce, range(parallelism))
    duration = time.time() - start
    print "Finish job in %s seconds" % duration
    time.sleep(1)
    d.delete()


io_mode = sys.argv[1]
run()
