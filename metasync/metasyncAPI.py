import os
import io
import sys
import time
import threading
import struct
import pickle
import tempfile
import shutil
import types

from threading import Thread
from Queue import Queue
from multiprocessing import cpu_count
from mapping import DetMap2

import dbg
import util
import services
import translators

from blobs import *
from params import *

#
# basic use of ThreadPool:
#  pool.submit(func, arg1, arg2)
#  pool.join()
#
class Worker(Thread):
    """Thread executing tasks from a given tasks queue"""
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            clone, func, args, kargs = self.tasks.get()
            #print 'work on job now %s' % self.ident
            try:
                srv = clone()
                args = (srv, ) + args
                func(*args, **kargs)
            except Exception as e:
                print(str(srv))
                print(str(func))
                print(e)
            self.tasks.task_done()

class ThreadPool:
    """Pool of threads consuming tasks from a queue"""
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def submit(self, c, func, *args, **kargs):
        self.tasks.put((c, func, args, kargs))

    def join(self):
        self.tasks.join()

class ServiceThread(Thread):
    """A dedicated thread for each service
    requests to this thread will be serialized"""
    def __init__(self, service):
        Thread.__init__(self)
        self.srv = service
        self.tasks = Queue(5) # TODO: what's the proper number
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            args = (self.srv, ) + args
            try: func(*args, **kargs)
            except Exception as e:
                print e
            self.tasks.task_done()

    def submit(self, func, *args, **kargs):
        self.tasks.put((func, args, kargs))

    def join(self):
        self.tasks.join()

class Scheduler:
    def __init__(self, services, maxthreads=None):
        self.srv_threads = {}
        # XXX. inflexible in dealing with changes of services (list)
        if(maxthreads is None or cpu_count() > maxthreads): maxthreads = cpu_count()
        nthreads = maxthreads - len(services)
        for srv in services:
            self.srv_threads[srv] = ServiceThread(srv)
        # thredpool thread number?
        dbg.dbg("init scheduler: %s" % nthreads)
        self.pool = ThreadPool(min(nthreads, 3*len(services)))

    def submit(self, srv, serialize, func, *args, **kargs):

        if serialize:
            thread = self.srv_threads[srv]
            thread.submit(func, *args, **kargs)
        else:
            """
            # haichen what's the purpose of the following code?
            if srv in self.srv_threads:
                dbg.dbg("putting into service thread")
                thread = self.srv_threads[srv]
                if thread.tasks.empty():
                    thread.submit(func, *args, **kargs)
            else:
            """
            #dbg.dbg("putting into thread pool")
            self.pool.submit(srv.copy, func, *args, **kargs)

    def join(self):
        self.pool.join()
        for srv in self.srv_threads:
            self.srv_threads[srv].join()

# handle user's inputs of config options
def _get_conf_services(default):
    assert type(default) in [types.NoneType, list]

    if default is not None:
        return ",".join(default)

    # dirty user's input
    slugs = ",".join(slug for (slug, _) in services.backends())
    print "input storage backends, (e.g., %s)" % slugs
    for (_, doc) in services.backends():
        print "    %s" % doc
    return raw_input("> ").strip()

def _get_conf_nreplicas(default):
    assert type(default) in [types.NoneType, int]

    if default is not None:
        return str(default)

    # dirty user's input
    print "input the number of replicas (default=2)"
    replicas = raw_input("> ").strip()
    if replicas == "":
        replicas = "2"
    return replicas

def _get_conf_encryptkey(default):
    assert type(default) in [types.NoneType, str]

    if default is not None:
        return default

    # NOTE.
    #  empty encrypt_key means, no-encryption
    encrypt_key = ""

    print "do you use encryption (y/n)?"
    encrypt_yn = raw_input("> ").strip().lower()
    if(encrypt_yn not in ['y','n']):
        dbg.err("input with y/n")
        exit(-1)
    if(encrypt_yn == 'y'):
        print "input keyphrase:"
        encrypt_key = raw_input("> ").strip()

    return encrypt_key


# in charge of a local machine (.metasync)
#
class MetaSync:
    def __init__(self, root, opts=None):
        #
        #  repo/.metasync/
        #  ^    ^
        #  |    +-- meta
        #  +-- root

        # useful path info
        self.path_root   = self._find_root(root)
        self.path_meta   = os.path.join(self.path_root, META_DIR)
        self.path_conf   = self.get_path("config")
        self.path_objs   = self.get_path("objects")
        self.path_master = self.get_path("master")
        self.path_master_history = self.get_path("master_history")
        self.path_head_history = self.get_path("head_history")
        self.options     = opts

        # local blob store
        self.blobstore   = BlobStore2(self) #BlobStore(self.path_objs)

        # load on demand
        self.config      = None
        self.srvmap      = {}
        self.scheduler   = None
        self.translators = []
        self.mapping     = None

        # post init
        self._load()

    def _find_root(self, curpath):
        # find repo
        curpath = os.path.abspath(curpath)
        orgpath = curpath
        auth_dir = os.path.join(os.path.expanduser("~"), ".metasync")
        while True:
            path = os.path.join(curpath, META_DIR)
            if(path != auth_dir and os.path.exists(os.path.join(curpath, META_DIR))): return curpath
            sp = os.path.split(curpath)
            if(sp[1] == ""): break
            curpath = sp[0] 
        return orgpath


    @property
    def services(self):
        return self.srvmap.values()

    # load member variables from config
    def _load(self):
        if not self.check_sanity():
            return

        if(not os.path.exists(AUTH_DIR)): os.mkdir(AUTH_DIR)

        # load config
        self.config    = util.load_config(self.path_conf)
        self.namespace = self.config.get("core", "namespace")
        self.clientid  = self.config.get("core", "clientid")

        # load services from config
        self.srvmap = {}
        for tok in self.config.get("backend", "services").split(","):
            srv = services.factory(tok)
            self.srvmap[srv.sid()] = srv

        self.nreplicas = int(self.config.get("backend", "nreplicas"))
        nthreads = self.options.nthreads if self.options is not None else 2
        self.scheduler = Scheduler(self.services, (nthreads+1)*len(self.srvmap))

        # load translator pipe
        if self.is_encypted():
            self.translators.append(translators.TrEncrypt(self))

        # TODO. for integrity option
        # if self.is_signed():
        #     self.translators.append(TrSigned(self))

        beg = time.time()
        if(os.path.exists(self.get_path("mapping.pcl"))):
            with open(self.get_path("mapping.pcl")) as f:
                self.mapping = pickle.load(f)
        else:
            mapconfig = []
            for srv in self.services:
                mapconfig.append((srv.sid(), srv.info_storage()/GB))
            hspacesum = sum(map(lambda x:x[1], mapconfig))
            hspace = max(hspacesum+1, 1024)
            self.mapping = DetMap2(mapconfig, hspace=hspace, replica=self.nreplicas)
            self.mapping.pack()
            with open(self.get_path("mapping.pcl"), "w") as f:
                pickle.dump(self.mapping, f)
        end = time.time()
        dbg.time("mapping init %s" % (end-beg))
        dbg.dbg("head: %s", self.get_head_name())

    def cmd_reconfigure(self, backends, replica):
        srvmap = {}
        for tok in backends.split(","):
            srv = services.factory(tok)
            srvmap[srv.sid()] = srv
        lst_services = srvmap.values()
        mapconfig = []
        lock_dic = {}
        for srv in lst_services:
            mapconfig.append((srv.sid(), srv.info_storage()/GB))
        for srv in srvmap:
            lock_dic[srv] = threading.Lock()
            if srv not in self.srvmap:
                srvmap[srv].putdir(self.get_remote_path("objects"))
        for srv in self.srvmap:
            if srv not in lock_dic: 
                lock_dic[srv] = threading.Lock()

        beg = time.time()
        self.mapping.reconfig(mapconfig, eval(replica))
        end = time.time()
        dbg.info("remap: %.3fs" % (end-beg))
        beg = time.time()
        lst_objs = self.blobstore.list()
        added, removed = self.mapping.get_remapping(lst_objs)
        nthreads = self.options.nthreads if self.options is not None else 2

        #REFACTOR 
        def __put_next(srv, lst, lock):
            dbg.job("submitted to: %s" % srv)
            while True:
                lock.acquire()
                if(len(lst) == 0):
                    lock.release()
                    break
                next = lst.pop()
                lock.release()
                if next is not None:
                    with open(self.get_local_obj_path(next), "rb") as f:
                        blob = f.read()
                        for tr in self.translators:
                            blob = tr.put(blob)
                        # XXX HACK
                        backoff = 0.5
                        remote_path = self.get_remote_obj_path(next)
                        while not srv.put(remote_path, blob):
                            time.sleep(backoff)
                            backoff *= 2

        def __rm_next(srv, lst, lock):
            dbg.job("submitted to: %s" % srv)
            while True:
                lock.acquire()
                if(len(lst) == 0):
                    lock.release()
                    break
                next = lst.pop()
                lock.release()
                if next is not None:
                    remote_path = self.get_remote_obj_path(next)
                    srv.rm(remote_path)
        cnt_added = 0 
        for srv in added:
            if(len(added[srv]) == 0): continue
            cnt_added += len(added[srv])
            for i in range(nthreads):
                self.scheduler.submit(srvmap[srv], False, __put_next, added[srv], lock_dic[srv])   
        self._join()

        end = time.time()
        dbg.info("remap put: %.3fs" % (end-beg))

        beg = time.time()
        cnt_removed = 0
        for srv in removed:
            if(len(removed[srv]) == 0): continue
            cnt_removed += len(removed[srv])
            for i in range(nthreads):
                self.scheduler.submit(self.srvmap[srv], False, __rm_next, removed[srv], lock_dic[srv])   
        self._join()
        end = time.time()
        dbg.info("remap rm: %.3fs" % (end-beg))
        dbg.info("added %d, removed %d" % (cnt_added, cnt_removed))


    # config-related parser
    def is_encypted(self):
        key = self.config.get('core', 'encryptkey').strip()
        return key != ""

    # handling dir/path names
    def get_path(self, path):
        return os.path.join(self.path_meta, path)

    def get_head(self):
        return self.get_path(self.get_head_name())

    def get_head_name(self):
        return "head_%s" % self.get_client_id()

    def get_head_value(self):
        with open(self.get_head()) as f:
            return f.read().strip()
        return None

    def get_client_id(self):
        return self.clientid

    def get_relative_path(self, path):
        return os.path.relpath(os.path.abspath(path), self.path_root)

    def get_local_path(self, *path):
        return os.path.join(self.path_root, *[p.strip("/") for p in path])

    def get_local_obj_path(self, hv):
        return os.path.join(self.path_objs, hv)

    def get_remote_path(self, *path):
        #return os.path.join(self.namespace, *path).rstrip("/")
        return "/".join([self.namespace] + list(path)).rstrip("/").rstrip("\\")

    def get_remote_obj_path(self, *hashes):
        return self.get_remote_path("objects", *hashes)

    def get_root_blob(self):
        return self.blobstore.get_root_blob()

    # check basic sanity of repo's meta info
    def check_sanity(self, whynot=False):
        def __err(why):
            if whynot:
                print >> sys.stderr, why
            return False
        if not os.path.exists(self.path_meta):
            return __err("Can't find the root of repo (%s)" % self.path_meta)
        if not os.path.exists(self.path_conf):
            return __err("Can't find config (%s)" % self.path_conf)
        if not os.path.exists(self.path_objs):
            return __err("Can't find objects store (%s)" % self.path_objs)
        return True

    # schedule-related
    def _put_all_content(self, content, remote_path, serial=False):
        def __put(srv):
            #dbg.job("submitted to: %s" % srv)
            srv.put(remote_path, content)

        # submit jobs
        for srv in self.services:
            self.scheduler.submit(srv, serial, __put)

    def _put_all_dir(self, remote_path):
        # XXX. handle errs
        def __putdir(srv):
            srv.putdir(remote_path)

        # submit jobs
        for srv in self.services:
            self.scheduler.submit(srv, True, __putdir)

    def _put_all(self, path, remote_path):
        # XXX. handle errs
        def __put(srv):
            with open(path, "rb") as f:
                srv.put(remote_path, f.read())

        # submit jobs
        for srv in self.services:
            self.scheduler.submit(srv, True, __put)

    def _update_all(self, path, remote_path):
        # XXX. handle errs
        def __update(srv):
            #dbg.job("submitted to: %s" % srv)
            with open(path, "rb") as f:
                #print 'start to put'
                srv.update(remote_path, f.read())
                #print 'put ends'

        # submit jobs
        for srv in self.services:
            self.scheduler.submit(srv, True, __update)

    def _join(self):
        self.scheduler.join()

    def _get(self, srv, path, remote_path):
        def __get(srv, path, remote_path):
            dbg.job("submitted to: %s (%s)" % (srv, path))
            with open(path,  "wb") as f:
                blob = srv.get(remote_path)
                if(blob is None):
                    time.sleep(1)
                    blob = srv.get(remote_path)
                for tr in reversed(self.translators):
                    blob = tr.get(blob)
                f.write(blob)

        self.scheduler.submit(srv, False, __get, path, remote_path)

    # bstore-related
    def bstore_download(self):
        # TODO, handle when R > 1
        lst = self.blobstore.list()
        #dbg.dbg("lst files:%s" % lst)

        lock = threading.Lock()
        def __get_next(srv, hash_dic, lock, allset, srvname):
            if(len(hash_dic[srvname]) == 0): return
            while True:
                lock.acquire()
                try:
                    next = hash_dic[srvname].pop()
                    l = len(hash_dic[srvname])
                    if(l%10 == 0):
                        dbg.dbg("%s left %d" % (srvname, l))
                    if(next not in allset):
                        allset.add(next)
                    else:
                        next = None
                except:
                    lock.release()
                    break
                lock.release()
                if(next is not None):
                    remote_path = self.get_remote_obj_path(next)
                    path = os.path.join(self.path_objs, next)
                    with open(path, "wb") as f:
                        backoff = 0.5
                        while True:
                            blob = srv.get(remote_path)
                            if(blob is not None): break
                            dbg.dbg("back off %s" % srvname)
                            time.sleep(backoff)
                            backoff*=2
                            
                        for tr in reversed(self.translators):
                            blob = tr.get(blob)
                        f.write(blob)

        hash_dic = {}
        allset = set([])
        for srv in self.services:
            hash_dic[str(srv)] = []
            srvlist = srv.listdir(self.get_remote_obj_path())
            backoff = 1
            while srvlist is None:
                dbg.dbg("back off - listdir %s" % str(srv))
                time.sleep(backoff)
                srvlist = srv.listdir(self.get_remote_obj_path())

            for hashname in srvlist:
                if(hashname in lst):
                    #dbg.dbg("%s is already in bstore" % hashname)
                    continue
                hash_dic[str(srv)].append(hashname)

        nthreads = self.options.nthreads if self.options is not None else 2
        for srv in self.services:
            dbg.dbg("%s:%d dn" % (str(srv), len(hash_dic[str(srv)])))
            ##HACK
            for i in range(nthreads):
                self.scheduler.submit(srv, False, __get_next, hash_dic, lock, allset, str(srv))

    def bstore_sync_left(self, hashdic):
        cnt = 0
        for i in hashdic:
            cnt += len(hashdic[i])
        if(cnt == 0): return

        def __put_next(srv, lst, lock):
            dbg.job("submitted to: %s" % srv)
            while True:
                lock.acquire()
                if(len(lst) == 0):
                    lock.release()
                    break
                next = lst.pop()
                lock.release()
                if next is not None:
                    with open(self.get_local_obj_path(next), "rb") as f:
                        blob = f.read()
                        for tr in self.translators:
                            blob = tr.put(blob)
                        # XXX HACK
                        backoff = 0.5
                        remote_path = self.get_remote_obj_path(next)
                        while not srv.put(remote_path, blob):
                            time.sleep(backoff)
                            backoff *= 2

        lock_dic = {}
        for i in hashdic:
            lock_dic[i] = threading.Lock()

        nthreads = self.options.nthreads if self.options is not None else 2
        for srv in hashdic:
            for i in range(nthreads):
                self.scheduler.submit(self.srvmap[srv], False, __put_next, hashdic[srv], lock_dic[srv])   

        self._join()

    #XXX: it needs to return after one set is put, and continue on replication. 
    def bstore_sync(self, hashnames):
        dbg.dbg("need to sync: %s..@%d" % (hashnames[0], len(hashnames)))
        def __put_next(srv, hashdic, hashdic_left, allset, key, lock):
            dbg.job("submitted to: %s" % srv)
            while True:
                lock.acquire()
                if(len(hashdic[key]) == 0 or len(allset) == 0):
                    lock.release()
                    break

                next = hashdic[key].pop()
                if(next in allset):
                    allset.remove(next)
                else:
                    hashdic_left[key].append(next)
                    next = None
                lock.release()
                if next is not None:
                    with open(self.get_local_obj_path(next), "rb") as f:
                        blob = f.read()
                        for tr in self.translators:
                            blob = tr.put(blob)
                        # XXX HACK
                        backoff = 0.5
                        remote_path = self.get_remote_obj_path(next)
                        while not srv.put(remote_path, blob):
                            dbg.dbg("backoff %s" % srv)
                            time.sleep(backoff)
                            backoff *= 2

        nthreads = self.options.nthreads if self.options is not None else 2
        hashdic = {}
        hashdic_left = {}
        allset = set()
        lock = threading.Lock()
        for srv in self.srvmap:
            hashdic[srv] = []
            hashdic_left[srv] = []

        for hashname in hashnames:
            allset.add(hashname)
            for i in self.mapping.get_mapping(hashname):
                hashdic[i].append(hashname)

        for srv in hashdic:
            for i in range(nthreads):
                self.scheduler.submit(self.srvmap[srv], False, __put_next, hashdic, hashdic_left, allset, srv, lock)   

        self._join()
        return hashdic_left        

    # iterate bstore
    def bstore_iter(self):
        for root, dirs, files in os.walk(self.path_objs):
            for name in files:
                yield name

    def bstore_iter_remote(self, srv):
        assert srv in self.services

        # NOTE. at some point, we need cascaded directory hierarchy
        for obj in srv.listdir(self.get_remote_obj_path()):
            yield obj

    #XXX. update only changed files (SY)
    def restore_from_master(self):
        root = self.get_root_blob()
        dbg.dbg("restore")
        for name, blob in root.walk():
            pn = os.path.join(self.path_root, name)
            if blob.thv == "F":
                content = blob.read()
                util.write_file(pn, content.getvalue())
                content.close()
            if blob.thv == "m":
                content = blob.read()
                util.write_file(pn, content)
            elif blob.thv == "D" or blob.thv == "M":
                try:
                    os.mkdir(pn)
                except:
                    pass
        return True

    def lock_master(self):
        from paxos import Proposer
        self.proposer = Proposer(self.clientid, self.services, self.get_remote_path("lock"))
        if(self.proposer.check_locked()):
            dbg.dbg("already locked")
            return False
        if(self.proposer.propose() == self.clientid):
            return True
        else:
            return False

    def unlock_master(self):
        self.proposer.done()
        self.proposer.join()
        return True

    # need to truncate if history is too long.
    def get_history(self, is_master=False): 
        pn = self.path_master_history if is_master else self.path_head_history
        content = util.read_file(pn).strip()
        if content:
            history = content.split("\n")
            history.reverse()
        else:
            history = []
        
        return history

    def get_common_ancestor(self, head_history, master_history, known_common_history=None):
        # change to use known_common_history
        for head in head_history:
            if(head in master_history):
                return head
        return None

    def try_merge(self, head_history, master_history):
        # this need to be fixed.
        dbg.dbg("Trying to merge")
        # we may need to cache the last branched point
        common = self.get_common_ancestor(head_history, master_history)
        dbg.dbg("%s %s %s", head_history[0], master_history[0], common)
        common = self.blobstore.get_blob(common, "D")
        head = self.get_root_blob()
        master = self.blobstore.get_blob(master_history[0], "D")

        added1 = head.diff(common) 
        added2 = master.diff(common)

        def intersect(a, b):
            return list(set(a) & set(b))

        if(len(intersect(added1.keys(), added2.keys())) != 0):
            dbg.err("both modified--we need to handle it")
            return False
        for i in added2.keys():
            path = os.path.join(self.path_root, i)
            dirblob = self.blobstore.load_dir(os.path.dirname(path), dirty=True)
            dirblob.add(os.path.basename(path), added2[i], dirty=False)

        # HACK, need to go through all the non-overlapped history.
        self.append_history(master.hv)
        head.store()
        self.append_history(head.hv)
        # HACK, need to be changed
        newblobs = self.blobstore.get_added_blobs() 

        # push new blobs remotely
        self.bstore_sync(newblobs)
        self._join()

        return True

    def check_master_uptodate(self):
        srv = self.services[0]
        remote_master = srv.get(self.get_remote_path("master"))
        with open(self.path_master) as f:
            master_head = f.read().strip()
        if(master_head != remote_master): return False
        return True

    def cmd_poll(self):
        srv = self.services[0]
        srv.poll(self.namespace)

    #
    # end-user's interfaces (starting with cmd_ prefix)
    #   NOTE. explicitly return True/False to indicate status of 'cmd'
    #

    def cmd_share(self, target_email):
        if not self.check_sanity():
            dbg.err("this is not metasync repo")
            return False

        for srv in self.services:
            srv.share(self.namespace, target_email)

    def cmd_diff(self):
        # work only for 1-level directory
        # need to add diff for file
        if not self.check_sanity():
            dbg.err("this is not metasync repo")
            return False
        root = self.get_root_blob()
        added = []
        removed = []
        files = os.listdir(".")
        for f in files:
            if(f == ".metasync"): continue
            if("/"+f not in root.files):
                added.append(f)

        for f in root.files:
            if(f[1:] not in files):
                removed.append(f[1:])

        for f in added:
            print("+++ %s" % f)

        for f in removed:
            print("--- %s" % f)

    def cmd_mv(self, src_pn, dst_pn):
        if not self.check_sanity():
            dbg.err("it's not a metasync repo.")
            return False
        src_pn = os.path.abspath(src_pn)
        dst_pn = os.path.abspath(dst_pn)

        #TODO: check src_pn exists
        beg = time.time()
        try:
            dirname = os.path.dirname(src_pn)
            dirblob = self.blobstore.load_dir(dirname, False, dirty=True)
            if(dirblob is None):
                dbg.err("%s does not exist" % src_pn)
                return False
        except NotTrackedException as e:
            dbg.err(str(e))
            return False

        fname = os.path.basename(src_pn)
        if(not fname in dirblob): 
            dbg.err("%s does not exist" % pn)
            return False
        fblob = dirblob[fname]
        dirblob.rm(fname)

        dst_dirname = os.path.dirname(dst_pn)
        if(dirname != dst_dirname):
            dirblob = self.blobstore.load_dir(dirname, True, dirty=True)
            assert dirblob is not None

        dst_fname = os.path.basename(dst_pn)
        dirblob.add(dst_fname, fblob, dirty=False)

        root = self.get_root_blob()
        root.store()
        newblobs = self.blobstore.get_added_blobs()

        util.write_file(self.get_head(), root.hv)
        self.append_history(root.hv)

        end = time.time()
        dbg.time("local write: %f" % (end-beg))

        # push new blobs remotely
        self.bstore_sync(newblobs)
        self._put_all(self.get_head(), self.get_remote_path(self.get_head_name()))

        end = time.time()
        dbg.time("remote write: %f" % (end-beg))

        # move the file
        shutil.move(src_pn, dst_pn)
        self._join()

        return True 

    def cmd_peek(self):
        root = self.get_root_blob()
        for i in root.walk():
            print(i)
        # print("hash: %s" % root.hash_head)
        # print(root.dump_info())
        # with open(self.path_master) as f:
        #     master_head = f.read().strip()
        # with open(self.get_head()) as f:
        #     head = f.read().strip()
        # print("head_history %s" % ",".join(self.get_history(head)))
        # print("master_history %s" %  ",".join(self.get_history(master_head)))

    def cmd_fetch(self):
        if not self.check_sanity():
            dbg.err("it's not a metasync repo.")
            return False

        # TODO: change it into comparing between masters
        if(9654 in self.srvmap):
            srv = self.srvmap[9654]
        else:
            srv = self.services[0]
        self._get(srv, self.path_master, self.get_remote_path("master"))
        self._get(srv, self.path_master_history, self.get_remote_path("master_history"))
        self.bstore_download()

        self._join()
        return True

    def update_changed(self, head, master):
        def _file_create(blob, pn): 
            if(blob.thv == "D" or blob.thv == "M"):
                util.mkdirs(pn)
                for i in blob.entries:
                    _file_create(blob[i], os.path.join(pn, i))
            elif(blob.thv == "F"):
                content = blob.read()
                util.write_file(pn, content.getvalue())
                content.close()
                # touch metadata blob (for cmd_status)
                os.utime(os.path.join(self.path_objs, blob.hv), None)
            elif(blob.thv == "m"):
                content = blob.read()
                util.write_file(pn, content)
                # touch metadata blob (for cmd_status)
                os.utime(os.path.join(self.path_objs, blob.hv), None)
            else:
                assert False

        def _update(old_dirblob, new_dirblob, path):
            for fname in new_dirblob.entries:
                blob = new_dirblob[fname]
                if(fname not in old_dirblob): 
                    _file_create(blob, os.path.join(path, fname))
                elif(blob.hv != old_dirblob[fname].hv):
                    if(blob.thv == "D"):
                        _update(old_dirblob[fname], blob, os.path.join(path, fname))
                    elif(blob.thv == "F"): 
                        _file_create(blob, os.path.join(path, fname))
                    else:
                        print(blob.thv)
                        assert False

        headblob = self.blobstore.get_blob(head, "D")
        masterblob = self.blobstore.get_blob(master, "D")
        _update(headblob, masterblob, self.path_root)



    def cmd_update(self):
        def _update_head():
            shutil.copyfile(self.path_master, self.get_path(self.get_head_name()))
            shutil.copyfile(self.path_master_history, self.path_head_history)

        head_history = self.get_history()
        master_history = self.get_history(True)
        if(len(master_history) == 0):
            dbg.err("no master history")
            return False
        if(len(head_history) != 0):
            head = head_history[0]
            master = master_history[0] 
            if(head == master): #does not need to update
                return True
            if(head_history[0] not in master_history):
                if(not self.try_merge(head_history, master_history)):
                    raise Exception('Merge required')
            else:
                _update_head()
            self.update_changed(head, master)
        else:
            _update_head()
            self.restore_from_master()
        self.blobstore.rootblob = None
        dbg.info("update done %s" % time.ctime())
        #self.restore_from_master()
        return True

    def cmd_clone(self, namespace, backend=None, encrypt_key=None):
        # if wrong target
        if self.check_sanity():
            return False

        # reset all the path by including the namespace
        self.path_root   = os.path.join(self.path_root, namespace)
        self.path_meta   = os.path.join(self.path_root, META_DIR)
        self.path_conf   = self.get_path("config")
        self.path_objs   = self.get_path("objects")
        self.path_master = self.get_path("master")
        self.path_master_history = self.get_path("master_history")
        self.path_head_history = self.get_path("head_history")

        if os.path.exists(self.path_root):
            dbg.err("%s already exists." % self.path_root)
            return False

        if backend is None:
            print "input one of the storage backends, (e.g., dropbox,google,box)"
            print "  for testing, use disk@/path (e.g., disk@/tmp)"
            backend = raw_input("> ")

        srv  = services.factory(backend)
        self.namespace = namespace

        # create repo directory
        os.mkdir(namespace)

        seed = srv.get(self.get_remote_path("config"))
        seed = srv.get(self.get_remote_path("configs/%s" % seed))
        conf = util.loads_config(seed)

        os.mkdir(self.path_meta)
        os.mkdir(self.path_objs)

        # setup client specific info
        conf.set('core', 'clientid'  , util.gen_uuid())
        conf.set('core', 'encryptkey', _get_conf_encryptkey(encrypt_key))

        with open(self.path_conf, "w") as fd:
            conf.write(fd)

        self._load()
        beg = time.time()
        self.bstore_download()

        # need to change into checking all the masters and use the latest version
        self._get(srv, self.path_master, self.get_remote_path("master"))
        self._get(srv, self.path_master_history, self.get_remote_path("master_history"))
        self._join()

        # copy master to local repo
        shutil.copyfile(self.path_master, self.get_path(self.get_head_name()))
        shutil.copyfile(self.path_master_history, self.path_head_history)

        # send my head to remote
        self._put_all(self.get_head(), self.get_remote_path(self.get_head_name()))
        self._join()

        ret = self.restore_from_master()
        end = time.time()
        dbg.dbg("clone: %ss" % (end-beg))
        return ret

    def cmd_init(self, namespace, backend=None, nreplicas=None, encrypt_key=None):
        # already initialized?
        if self.check_sanity():
            dbg.warn("already initialized %s (%s)" \
                     % (self.path_root, self.namespace))
            return False

        os.mkdir(self.path_meta)
        os.mkdir(self.path_objs)

        # build config opts
        conf = util.new_config()

        # core: unique/permanent info about local machine (often called client)
        #   NOTE. not sure if encryption_key should be in core, or unchangable
        conf.add_section('core')
        conf.set('core', 'namespace' , namespace)
        conf.set('core', 'clientid'  , util.gen_uuid())
        conf.set('core', 'encryptkey', _get_conf_encryptkey(encrypt_key))

        # backend: info about sync service providers
        conf.add_section('backend')
        conf.set('backend', 'services' , _get_conf_services(backend))
        conf.set('backend', 'nreplicas', _get_conf_nreplicas(nreplicas))

        # flush
        with open(self.path_conf, "w") as fd:
            conf.write(fd)

        try: 
            self._load()
        except NameError:
            shutil.rmtree(self.path_meta)
            return False

        # put config into remote
        conf.remove_option('core','clientid')
        conf.remove_option('core','encryptkey')

        with io.BytesIO() as out:
            conf.write(out)
            val = out.getvalue()
            configname = util.sha1(val) 
            self._put_all_content(val, self.get_remote_path("configs/%s" % configname), True)

            #temporary --- move this to pPaxos
            self._put_all_content(configname, self.get_remote_path("config"), True)

        # re-init the repo
        util.empty_file(self.path_master)
        util.empty_file(self.get_head())
        util.empty_file(self.path_master_history)
        util.empty_file(self.path_head_history)

        self._put_all_dir(self.get_remote_path("objects"))
        self._put_all(self.path_master, self.get_remote_path("master"))
        self._put_all(self.path_master_history, self.get_remote_path("master_history"))
        self._put_all(self.get_head() , self.get_remote_path(self.get_head_name()))
        self._join()

        return True


    def cmd_gc(self):
        if not self.check_sanity():
            dbg.err("this is not a metasync repo")
            return False

        def _find_all_blobs(blob, tracked):
            # we may need to move this to blobstore
            if(blob.hv in tracked): return
            tracked.add(blob.hv)
            if(blob.thv == "C"): return
            for name, childblob in blob.entries.iteritems():
                _find_all_blobs(childblob, tracked)

        # check head
        head = self.get_head_value()
        tracked = set([])
        if(head is not None and len(head)>0):
            blob = self.blobstore.get_blob(head, "D") 
            _find_all_blobs(blob, tracked)
        # check master
        with open(self.path_master) as f:
            master_head = f.read().strip()
        if(len(master_head) > 0):
            blob = self.blobstore.get_blob(master_head, "D") 
            _find_all_blobs(blob, tracked)

        allblobs = set(self.blobstore.list())

        # remove following 
        blobs_to_remove = allblobs - tracked
        
        def __rm(srv, remote_path):
            dbg.job("submitted to: %s (%s)" % (srv, remote_path))
            srv.rm(remote_path)

        for hashname in blobs_to_remove:
            for i in self.mapping.get_mapping(hashname):
                self.scheduler.submit(self.srvmap[i], True, __rm, self.get_remote_obj_path(hashname))
            os.unlink(self.get_local_obj_path(hashname)) 

        return True 

    def cmd_rm(self, pn):
        if not self.check_sanity():
            dbg.err("this is not a metasync repo")
            return False
        #TODO: check if the file exists

        beg = time.time()
        try:
            dirname = os.path.dirname(pn)
            dirblob = self.blobstore.load_dir(dirname, False)
            if(dirblob is None):
                dbg.err("%s does not exist" % pn)
                return False
        except NotTrackedException as e:
            dbg.err(str(e))
            return False

        fname = os.path.basename(pn)
        if(not fname in dirblob): 
            dbg.err("%s does not exist" % pn)
            return False

        dirblob.rm(fname)
        root = self.get_root_blob()
        root.store()
        newblobs = self.blobstore.get_added_blobs()

        # we may need to include pointer for previous version.
        util.write_file(self.get_head(), root.hv)
        self.append_history(root.hv)

        end = time.time()
        dbg.time("local write: %f" % (end-beg))

        # push new blobs remotely
        self.bstore_sync(newblobs)
        self._put_all(self.get_head(), self.get_remote_path(self.get_head_name()))

        end = time.time()
        dbg.time("remote write: %f" % (end-beg))
        self._join()

        # drop local copy
        # TODO: rm only tracked files if removing file.
        try:
            os.unlink(pn)
        except:
            dbg.err("failed to rm %s" % pn)
            return False

        return True

    def append_history(self, hv):
        util.append_file(self.path_head_history, hv+"\n")

    def cmd_checkin(self, paths, unit=BLOB_UNIT, upload_only_first=False):
        if not self.check_sanity():
            dbg.err("this is not a metasync repo")
            return False
        if type(paths) != types.ListType:
            paths = [paths] 
        for pn in paths: 
            if not os.path.exists(pn):
                dbg.err("File %s doesn't exits." % pn)
                return False
            
        beg = time.time()
        #XXX: considering mtime, check hash of chunks?
        changed = False
        for path in paths:
            if(not os.path.isfile(path)): 
                changed = True
                for root, dirs, files in os.walk(path):
                    fsizesum = 0
                    for fname in files:
                        fsizesum += os.stat(os.path.join(root,fname)).st_size
                    print(root + " " + str(fsizesum))
                    if(fsizesum < unit):
                        dirblob = self.blobstore.load_dir(root, dirty=True, merge=True)
                        for fname in files:
                            dirblob.add_file(fname, os.path.join(root, fname))
                        dirblob.done_adding()
                    else:
                        dirblob = self.blobstore.load_dir(root, dirty=True)
                        for fname in files:
                            fileblob = self.blobstore.load_file(os.path.join(root, fname), unit)
                            if(fname in dirblob and dirblob[fname].hv == fileblob.hv):
                                continue
                            dirblob.add(fname, fileblob)
            else:
                fileblob = self.blobstore.load_file(path, unit)
                dirname = os.path.dirname(path)
                if(dirname == ""): dirname = "."
                dirblob = self.blobstore.load_dir(dirname, dirty=True)
                fname = os.path.basename(path)
                if(fname in dirblob and dirblob[fname].hv == fileblob.hv):
                    continue
                changed = True
                dirblob.add(fname, fileblob)
        if(not changed): return True
        root = self.get_root_blob()
        root.store()
        newblobs = self.blobstore.get_added_blobs()

        # we may need to include pointer for previous version.
        util.write_file(self.get_head(), root.hv)
        self.append_history(root.hv)

        end = time.time()
        dbg.time("local write: %f" % (end-beg))

        # push new blobs remotely
        leftover = self.bstore_sync(newblobs)
        self._update_all(self.get_head(), self.get_remote_path(self.get_head_name()))

        self._join()
        end = time.time()
        dbg.time("remote write for R1: %f" % (end-beg))
        if(not upload_only_first):
            self.bstore_sync_left(leftover)
            end = time.time()
            dbg.time("remote write for left: %f" % (end-beg))
            return []
        else:
            return leftover


    def cmd_push(self):
        if(not self.lock_master()):
            raise Exception('locking failed')

        if(not self.check_master_uptodate()):
            dbg.err("You should fetch first")
            self.unlock_master()
            return False

        with open(self.path_master) as f:
            master_head = f.read().strip()
        with open(self.get_head()) as f:
            head = f.read().strip()
        if(len(master_head) > 0):
            head_history = self.get_history()
            if(not master_head in head_history):
                dbg.err("You should update first")
                self.unlock_master()
                return False
        # check master is ancestor of the head
        shutil.copyfile(self.get_path(self.get_head_name()), self.path_master)
        shutil.copyfile(self.get_path(self.path_head_history), self.path_master_history)
        self._update_all(self.path_master, self.get_remote_path("master"))
        self._update_all(self.path_master_history, self.get_remote_path("master_history"))
        self._join()

        self.unlock_master()
        return True

    def cmd_status(self, unit=BLOB_UNIT):

        def simple_walk(folder):
        # simple_walk will skip dipping into the folder 
        # that are not tracked in the repo
            untracked = []
            changed = []

            for f in os.listdir(folder):
                if f == META_DIR:
                    continue
                basename = os.path.basename(folder)
                if basename == '.' or basename == '':
                    relpath = f
                else:
                    relpath = os.path.join(folder, f)
                if relpath in tracked:
                    if os.path.isdir(f):
                        _untracked, _changed = simple_walk(relpath)
                        untracked.extend(_untracked)
                        changed.extend(_changed)
                    else:
                        fblob = tracked[relpath]
                        # compare the file modified time and its metadata blob modified time
                        curr_mtime = os.path.getmtime(relpath)
                        last_mtime = os.path.getmtime(os.path.join(self.path_objs, fblob.hv))
                        if curr_mtime > last_mtime:
                            # only load file when the file modified time is greater than metadata modified time
                            fblob._load()
                            flag = False
                            # compare chunk hash
                            for (offset, chunk) in util.each_chunk2(relpath, unit):
                                if util.sha1(chunk) != fblob.entries[offset].hv:
                                    flag = True
                                    break
                            if flag:
                                changed.append(relpath)
                else:
                    if os.path.isdir(relpath):
                        relpath = os.path.join(relpath, '')
                    untracked.append(relpath)
            return untracked, changed

        if not self.check_sanity():
            dbg.err("this is not a metasync repo")
            return False

        # switch to metasync repo root folder
        os.chdir(self.path_root)

        # compare the head and master history
        head_history = self.get_history()
        master_history = self.get_history(True)
        head_diverge = 0
        for head in head_history:
            if (head in master_history):
                break
            head_diverge += 1
        if head_diverge == len(head_history):
            master_diverge = len(master_history)
        else:
            master_diverge = master_history.index(head_history[head_diverge])

        if head_diverge == 0 and master_diverge == 0:
            print "\nYour branch is up-to-date with master."
        elif head_diverge == 0:
            print "\nYour branch is behind master by %d commit(s)." % master_diverge
        elif master_diverge == 0:
            print "\nYour branch is ahead of master by %d commit(s)." % head_diverge
        else:
            print "\nYour branch and master have diverged,"
            print "and have %d and %d different commits each, respectively" % (head_diverge, master_diverge)

        root = self.get_root_blob()
        tracked = {}
        for (path, blob) in root.walk():
            tracked[path] = blob

        untracked, changed = simple_walk('.')
        if changed:
            print("\nChanges not checked in:")
            for f in changed:
                print("\033[31m\tmodified:   %s\033[m" % f)

        if untracked:
            print("\nUntracked files:")
            for f in untracked:
                print("\033[31m\t%s\033[m" % f)

        return True
