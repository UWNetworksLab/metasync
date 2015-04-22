import os
import dbg
import util

from base import StorageAPI, AppendOnlyLog

class DiskAPI(StorageAPI, AppendOnlyLog):
    "disk@path    : mock disk service rooting on path"

    def __init__(self, root):
        self.root = root
        util.mkdirs(root)
        self._sid  = util.md5(self.root) % 10000

    def copy(self):
        return DiskAPI(self.root)

    def get_path(self, path):
        return os.path.join(self.root, path.lstrip("/"))

    def get(self, path):
        pn = self.get_path(path)
        if os.path.exists(pn):
            return util.read_file(pn)

    def put(self, path, content):
        dbg.api('put:' + self.root +',' + path)
        pn = self.get_path(path)
        util.mkdirs(os.path.dirname(pn))
        return util.write_file(pn, content)

    def putdir(self, path):
        pn = self.get_path(path)
        util.mkdirs(pn)

    def listdir(self, path):
        pn = self.get_path(path)
        if not os.path.exists(pn):
            return []
        else:
            return os.listdir(pn)

    def update(self, path, content):
        dbg.api('put:' + self.root +',' + path)
        pn = self.get_path(path)
        util.mkdirs(os.path.dirname(pn))
        return util.write_file(pn, content)

    def exists(self, path):
        return os.path.exists(self.get_path(path))

    def rm(self, path):
        os.unlink(self.get_path(path))


    def sid(self):
        return self._sid

    #AppendOnlyLog
    def init_log(self, path):
        if(not self.exists(path)):
            self.put(path, "")

    def reset_log(self, path):
        if(self.exists(path)):
            self.rm(path)

    def append(self, path, msg):
        import portalocker
        import time
        pn = self.get_path(path)
        util.mkdirs(os.path.dirname(pn))
        with open(pn, "a+") as log:
            while True:
                try:
                    portalocker.lock(log, portalocker.LOCK_EX)
                    break
                except:
                    dbg.dbg("lock failed")
                    time.sleep(0.1)
            log.write("%d\t%s\n" % (util.current_sec(), msg))

    def get_logs(self, path, last_clock):
        import portalocker
        import tailer
        import time
        pn = self.get_path(path)
        with open(pn, "r+") as log:
            while True:
                try:
                    portalocker.lock(log, portalocker.LOCK_EX)
                    break
                except:
                    dbg.dbg("lock failed")
                    time.sleep(0.1)
            curtime = int(util.current_sec())
            lines = tailer.tail(log, 20)
        ret = []
        if last_clock is None: last_clock = 0
        for line in lines:
            sp = line.strip().split("\t")
            if(int(sp[0]) < last_clock): continue
            #log = {
            #    'time': eval(sp[0]),
            #    'message': sp[1]
            #}
            #ret.append(log)
            ret.append(sp[1])
        return ret, curtime

    def __str__(self):
        return "disk@%s" % self.root
