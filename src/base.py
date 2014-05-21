from params import *

#
# abstractions for sync services
#  - get()     :
#  - put()     :
#  - update()  :
#  - listdir() : list of filenames
#  - exists()  : path exists or not
#  - sid()     : service id, persistent, integer (for paxos run)
#
#  - info_storage   : storage space
#  - info_preference: bandwidth? upload/download speed?
#  - info_free      : free space
#
class StorageAPI:
    def get(self, path):
        pass
    def put(self, path, content):
        pass
    def putdir(self, path):
        pass
    def listdir(self, path):
        pass
    def update(self, path, content):
        pass
    def exists(self, path):
        pass
    def rm(self, path):
        pass
    def rmdir(self, path):
        pass
    def metadata(self, path):
        pass
    def sid(self):
        pass
    def poll(self, path=None, cursor=None, timeout=30):
        pass
    def share(self, path, target_email):
        raise Exception('share is not supported in this storageAPI %s' % str(self))

    # criteria for determinstic mapping
    def info_storage(self):
        return 2*GB
    def info_preference(self):
        return 0
    def info_free(self):
        return 0
#
# abstractions for paxos primitives
#   - append() : append msg
#   - get()    : get logs
#
class AppendOnlyLog:
    def init_log(self, path):
        pass
    def reset_log(self, path):
        pass
    def append(self, path, msg):
        pass
    def get_logs(self, path, last_clock):
        pass
