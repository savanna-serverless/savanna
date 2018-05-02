import boto3
import StringIO
import string
import sys
import os


class S3ReadStream:
    def __init__(self, s3):
        self.s3 = s3
        self.buf = None
        self.begin = 0

    def read(self, size=None):
        if size is None:
            return self.s3.read()
        else:
            return self.s3.read(size)

    def readline(self):
        ret = ""
        while True:
            if self.buf is None:
                self.buf = self.s3.read(1024 * 1024).decode('utf-8')
            if self.buf == "":
                return None
            curr = self.begin
            while curr < len(self.buf) and self.buf[curr] != '\n':
                curr += 1
            if curr < len(self.buf):
                ret += self.buf[self.begin:curr]
                if curr == len(self.buf) - 1:
                    self.begin = 0
                    self.buf = None
                else:
                    self.begin = curr + 1
                return ret
            else:
                ret += self.buf[self.begin:]
                self.buf = self.s3.read(1024 * 1024).decode('utf-8')
                self.begin = 0

    def __iter__(self):
        return self

    def __next__(self):
        l = self.readline()
        if l is None:
            raise StopIteration
        return l

    def next(self):
        return self.__next__()


class EFSCache:
    def __init__(self, cdir, s3):
        self.cdir = cdir
        self.s3 = s3
        self.writes = {}

    def read(self, bucket, key):
        path = self.getpath(bucket, key)
        if not os.path.isfile(path):
            self.s3.meta.client.download_file(bucket, key, path)
        return open(path, "r")

    def write(self, bucket, key, value):
        path = self.getpath(bucket, key)
        if path not in self.writes:
            self.writes[path] = open(path, "w")
        self.writes[path].write(value)

    def getpath(self, bucket, key):
        return self.cdir + bucket + "~" + key.replace("/", "~")

    def close():
        for p, fd in self.writes.iteritems():
            close(fd)


class IOMan:
    def __init__(self,
                 mode,
                 i=None,
                 bucket=None,
                 key=None,
                 cacheclient=None,
                 snap_iso=True):
        assert mode in ["c", "nc", "s3", "efs"]
        self.output = {}
        self.i = i
        self.s3 = boto3.resource('s3')
        self.mode = mode
        self.bucket = bucket
        self.key_pre = key
        self.dummy_close = lambda *args: None
        self.snap_iso = snap_iso
        if mode in ["c", "nc"]:
            if cacheclient is None:
                from cacheclient import CacheClient
                print "init cacheclient"
                self.cache = CacheClient()
            else:
                self.cache = cacheclient
        if mode == "efs":
            self.efs = EFSCache("/efs/efscache/", self.s3)

    def read_file(self, bucket, key, s3=False):
        if s3:
            f = self.s3_read(bucket, key)
        else:
            f = self.read_key(bucket, key)
        content = f.read()
        f.close()
        return content

    def write_file(self, bucket, key, body, s3=False):
        if self.mode in ["c", "nc"]:
            f = self.cache.open(
                bucket,
                key,
                "w",
                consistency=self.mode == "c",
                snap_iso=self.snap_iso,
                s3=s3)
            f.write(body)
            f.close()
        else:
            self.s3.meta.client.put_object(Bucket=bucket, Key=key, Body=body)

    def s3_read(self, bucket, key):
        if self.mode in ["s3"]:
            rs = S3ReadStream(
                self.s3.meta.client.get_object(Bucket=bucket, Key=key)["Body"])
            rs.close = self.dummy_close
            return rs
        elif self.mode == "efs":
            return self.efs.read(bucket, key)
        else:
            return self.cache.open(
                bucket,
                key,
                "r",
                consistency=self.mode == "c",
                s3=True,
                snap_iso=self.snap_iso)

    def read(self, bucket, key_pre, pi):
        return self.read_key(bucket, "%s/f_%s_%s" % (key_pre, pi, self.i))

    def read_key(self, bucket, key):
        if self.mode == "s3":
            return self.s3_read(bucket, key)
        elif self.mode == "efs":
            return self.efs.read(bucket, key)
        else:
            return self.cache.open(
                bucket,
                key,
                "r",
                consistency=self.mode == "c",
                snap_iso=self.snap_iso)

    def write(self, key, v, async_s3=False):
        if self.mode == "s3":
            if key not in self.output:
                self.output[key] = StringIO.StringIO()
            self.output[key].write(v)
        elif self.mode == "efs":
            self.efs.write(self.bucket, "%s/f_%s_%s" % (self.key_pre, self.i,
                                                        key), v)
        else:
            if key not in self.output:
                self.output[key] = self.cache.open(
                    self.bucket,
                    "%s/f_%s_%s" % (self.key_pre, self.i, key),
                    "w",
                    consistency=self.mode == "c",
                    snap_iso=self.snap_iso,
                    s3=async_s3)
            self.output[key].write(v)

    def close(self):
        for k, v in self.output.iteritems():
            if self.mode == "s3":
                self.s3.meta.client.put_object(
                    Bucket=self.bucket,
                    Key="%s/f_%s_%s" % (self.key_pre, self.i, k),
                    Body=v.getvalue())
            elif self.mode == "efs":
                self.efs.close()
            else:
                v.close()
