import os
import sys
import time
import inspect
import filecmp

import util
import dbg
import services
import blobs
import random, string

from metasyncAPI import MetaSync
from mapping import DetMap, DetMap2

from params import *

# inspect doesn't respect the defined order
def __to_line(m):
    try:
        return m[1].func_code.co_firstlineno
    except AttributeError:
        return -1

def get_all_tests():
    if not hasattr(get_all_tests, "tests"):
        tests = []
        for (n, f) in inspect.getmembers(sys.modules[__name__]):
            if "test_" in n:
                tests.append((n[5:], f))
        get_all_tests.tests = sorted(tests, key=__to_line)

    return get_all_tests.tests

def _init_disk_metasync(metasync, opts, nbackend=3, nreplicas=2, encrypt_key=None):
    # use encrypt_key from cmd args
    if encrypt_key is None:
        encrypt_key = opts.encrypt_key

    # see. services api (DiskAPI)
    services = ["disk@%s/s%d" % (opts.tmpdir, i) for i in range(nbackend)]
    metasync.cmd_init("testing", services, nreplicas, encrypt_key)

#
# NOTE. test cases, starting with "_test" prefix
#
def test_help(metasync, opts):
    "print out help string for sub tests"

    opts.notree = True
    print "> List of subcommands"
    for (n, f) in get_all_tests():
        print "  %-20s: %s" % (n, f.__doc__)

def test_all(metasync, opts):
    "run all test cases"

    opts.notree = True
    for (n, f) in get_all_tests():
        if n == "all" or n.startswith("bench_"):
            continue
        dbg.info("#R<testing %s#> (%s)" % (n, f.__doc__))
        f(metasync, opts)

def test_path(metasync, _):
    "test path constraints"

    assert metasync.check_sanity() is False
    assert metasync.path_meta.endswith("/.metasync")
    assert metasync.path_conf.endswith("/.metasync/config")
    assert metasync.path_objs.endswith("/.metasync/objects")

def test_init(metasync, opts):
    "test inititation"

    _init_disk_metasync(metasync, opts, 3, 2, opts.encrypt_key)

    # create/commit some files
    file_sizes = [1024, 2048]
    if opts.slow:
        # bigger one that splits by blob
        MB = 1024*1024
        file_sizes.append(33 * MB)

    for size in file_sizes:
        pn = os.path.join(opts.root, "file-%s" % size)
        util.create_random_file(pn, size)
        metasync.cmd_checkin(pn)

    metasync.cmd_push()

    root = metasync.get_root_blob()
    assert len(root.entries) == len(file_sizes)

def test_encryption(metasync, opts):
    "test encryption layer"

    # set a encryption key
    _init_disk_metasync(metasync, opts, 3, 2, "testkey")

    import translators
    tr = translators.TrEncrypt(metasync)
    assert tr.get(tr.put(test_encryption.__doc__)) == test_encryption.__doc__

def test_clone(metasync, opts, need_init=True):
    "test cloning, after init"

    if need_init:
        test_init(metasync, opts)

    dst = os.path.join(opts.tmpdir, "repo_clone")
    util.mkdirs(dst)

    # pick first backend
    srv = metasync.config.get("backend", "services").split(",")[0]

    clone = MetaSync(dst)
    clone.cmd_clone("testing", srv, opts.encrypt_key)

    # compare file side-by-side
    for root, dirs, files in os.walk(clone.path_root):
        for name in files:
            dst = os.path.join(root, name)
            src = metasync.path_root + dst[len(clone.path_root):]
            try:
                if not filecmp.cmp(dst, src):
                    assert dst.endswith("config")
            except OSError as e:
                assert name.startswith("head") or name.startswith("prev")

    return clone

def test_checkin_dir(metasync, opts):
    "test checkin with directory"

    test_init(metasync, opts)

    dst = os.path.join(metasync.path_root, "a/b")
    util.mkdirs(dst)
    pn = os.path.join(dst, "test-1024")
    util.create_random_file(pn, 1024)

    dst = os.path.join(metasync.path_root, "a")

    metasync.cmd_checkin(dst)
    metasync.cmd_push()

    test_clone(metasync, opts, False)

def test_checkin_samefile(metasync, opts):
    "test checkin one file twice"

    test_init(metasync, opts)
    metasync.cmd_checkin(os.path.join(metasync.path_root, "file-1024"))

def test_uptodate_master(metasync, opts):
    "check uptodate master"
    #XXX not yet done
    clone = test_clone(metasync, opts)
    assert metasync.get_next_version() == 2
    assert clone.get_next_version() == 2
    assert metasync.get_uptodate_master() != None

    file_sizes = [1024, 2048]
    for size in file_sizes:
        pn = os.path.join(clone.path_root, "file-%s-2" % size)
        util.create_random_file(pn, size)
        clone.cmd_checkin(pn)
    clone.cmd_push()

    master = metasync.get_uptodate_master() 
    metasync.cmd_fetch()
    metasync.cmd_update()
    assert master == metasync.get_prev_value()

def test_fetch(metasync, opts):
    "test fetching"

    clone = test_clone(metasync, opts)

    file_sizes = [1024, 2048]
    for size in file_sizes:
        pn = os.path.join(clone.path_root, "file-%s-2" % size)
        util.create_random_file(pn, size)
        clone.cmd_checkin(pn)
    pn = os.path.join(clone.path_root, "dir1")
    util.mkdirs(pn)
    clone.cmd_checkin(pn)
    pn = os.path.join(clone.path_root, "dir2")
    util.mkdirs(pn)
    pn = os.path.join(clone.path_root, "dir2", "file-1024")
    util.create_random_file(pn, 1024)
    pn = os.path.join(clone.path_root, "dir2")
    clone.cmd_checkin(pn)
    clone.cmd_push()
    root2 = clone.get_root_blob()
    metasync.cmd_fetch()
    metasync.cmd_update()
    root = metasync.get_root_blob()
    cnt = 0
    for i in root.walk():
        cnt += 1
    assert cnt == 7

    # XXX. what to assert?

def test_rm(metasync, opts):
    "test rm file"

    _init_disk_metasync(metasync, opts, 3, 2, opts.encrypt_key)

    # create/commit some files
    size = 512
    for i in range(5):
        pn = os.path.join(opts.root, "file-%s-%s" % (size, i))
        util.create_random_file(pn, size)
        metasync.cmd_checkin(pn)

    pn = os.path.join(opts.root, "a/b")
    util.mkdirs(pn)
    metasync.cmd_checkin(pn)
    metasync.cmd_push()

    pn = os.path.join(opts.root, "a/b/e")
    util.mkdirs(pn)

    # try to remove non-exist directory
    pn = os.path.join(opts.root, "a/b/c/d")
    assert not metasync.cmd_rm(pn)

    pn = os.path.join(opts.root, "a/b/e/f")
    assert not metasync.cmd_rm(pn)

    # try to remove non-exist file

    for i in range(3):
        pn = os.path.join(opts.root, "file-%s-%s" % (size, i))
        metasync.cmd_rm(pn)
        assert not os.path.exists(pn)

    metasync.cmd_rm(os.path.join(opts.root,"a/b"))

    metasync.cmd_push()

    # TODO. fire gc
    # check blobs

def test_mapping(metasync, opts):
    "test mapping strategies"

    opts.notree = True

    m = DetMap([(1, 2*GB), (2, 5*GB), (3, 3*GB)])

    def __do_check(m):
        assert len([n for n in m.distrib if n == 1]) == len(m.distrib)/10*2
        assert len([n for n in m.distrib if n == 2]) == len(m.distrib)/10*5
        assert len([n for n in m.distrib if n == 3]) == len(m.distrib)/10*3

        copyset = set()
        for i in range(len(m.distrib)):
            c = m.get_mapping(i)
            assert len(c) == m.replica
            copyset.add(tuple(c))

        assert len(copyset) < len(m.distrib)

        dbg.info("mapping: %s ..." % str(m.distrib)[:30])
        dbg.info("copyset: %s" % str(list(copyset)))

    __do_check(m)
    __do_check(DetMap.restore(m.store()))

    # small_test
    m = DetMap([(2, 2*GB), (1, 5*GB)])
    assert m.mapinfo[0].config[0][0] == 1 

def test_mapping_reconfig(metasync, opts):
    "test reconfiguration/versioning of mapping strategies"

    opts.notree = True

    m = DetMap([(1, 2*GB), (2, 5*GB), (3, 2*GB)])
    m.reconfig([(1, 2*GB), (2, 5*GB)])

    # two version of mapping info
    assert len(m.mapinfo) == 2
    for (ver, conf) in m.mapinfo.iteritems():
        dbg.info("%s -> %s"  % (ver, conf.store()))

    # where to remap of hash value: 0
    #  0: [1,2] -> [1,2]
    #  1: [2,3] -> [2,1]
    assert m.get_remmaping(0) == []
    assert m.get_remmaping(1) == [1]


def test_mapping2(metasync, opts):
    "test a new mapping scheme to tolerate rebalancing on node-failre"

    from itertools import permutations

    # goal
    # 1. non-uniformly locating blobs, approximately reflecting storage size of each node
    # 2. minimize realigning on a node failure

    # design
    # 0. node -> (node, storage)
    #    (e.g., (1,0), (1,1) if node 1 has 2G storage)
    # 1. fixed hspace, where h(blob) % hspace = index
    #    (hspace any large number, NOT depending on len(nodes))
    # 2. detmap[index] -> a group of nodes
    #    (a group includes all nodes, but different order see 3)
    # 3. order nodes in a group, by hash(index, node)
    #    (so deterministic)
    # 4. in each group, pick first #replication nodes

    # failure
    # node change
    # replication
    #   => in all of above situations, only blobs in old/new node
    #      will be re-balanced
    #

    def uniq(lst, n):
        rtn = []
        for (i, _) in lst:
            if i in rtn:
                continue
            rtn.append(i)
            if len(rtn) == n:
                break
        return rtn

    replication = 2
    config = [(0,2), (1,4), (2,4), (3,2)]
    hspace = 20
    detmap = DetMap2(config, hspace, replication)

    stats = [0] * len(config)
    for (k, v) in enumerate(detmap.mapinfo[0].detmap):
        """
        # doesn't it need to calculate over uniq?
        for i in range(replication):
            stats[v[i][0]] += 1
        """
        for i in uniq(v, replication):
            stats[i] += 1
        if opts.slow:
            dbg.info("%-2s: [" % k)
            for (n, priority) in v:
                dbg.info("  %s: %s" % (n, priority))
            dbg.info("]")
        else:
            dbg.info("%-2s: %s -> %s" \
                       % (k, [e[0] for e in v], detmap.get_mapping(k)))

    # approximately reflect storage?
    for (i, k) in enumerate(stats):
        dbg.info("%s (%s GB) -> #%s" % (i, config[i][1], k))

def test_map_pack(metasync, opts):
    config = [(0,2), (1,10), (2,4), (3,2)]
    hspace = 100 
    replication = 2
    N = 50
    detmap = DetMap2(config, hspace, replication)

    detmap2 = DetMap2(config, hspace, replication)
    detmap2.pack()

    for _ in range(100):
        randstr = ''.join(random.choice(string.letters + string.digits) for _ in range(N))
        hashid = util.sha1(randstr)
        assert detmap.get_mapping(hashid) == detmap2.get_mapping(hashid)


def test_remapping(metasync, opts):
    replication = 2
    config = [(0,2), (1,4), (2,4), (3,2)]
    hspace = 20
    detmap = DetMap2(config, hspace, replication)
    N = 50
    lst = []
    for _ in range(100):
        randstr = ''.join(random.choice(string.letters + string.digits) for _ in range(N))
        hashid = util.sha1(randstr)
        lst.append(hashid)

        #lst = detmap.get_mapping(hashid)
        #for i in lst:
        #    count[i] += 1
    detmap.reconfig(config, 3)
    assert len(detmap.mapinfo) == 2
    added, removed = detmap.get_remapping(lst)
    for i in removed:
        assert len(removed[i]) == 0
    import copy
    detmap = DetMap2(config, hspace, replication)
    config = copy.copy(config)
    config.pop()
    lst3 = []
    for hv in lst:
        if 3 in detmap.get_mapping(hv):
            lst3.append(hv)
    detmap.reconfig(config)
    added, removed = detmap.get_remapping(lst)
    assert len(removed[3]) == len(lst3)


def test_check_lock(metasync, opts):
    "test check lock lock"
    lock = 'locktest/ltest'

    test_init(metasync, opts)
    srvs = metasync.services

    for srv in srvs:
        if not srv.exists(lock):
            srv.put(lock, '')

    from paxos import Proposer
    proposer = Proposer("1", srvs, lock)
    assert not proposer.check_locked()
    val = proposer.propose("1")
    assert proposer.check_locked()
    proposer.done()
    assert not proposer.check_locked()
    proposer.join()

def test_paxos_latency(metasync, opts):
    lock = "locktest/ltest_latency"
    import services
    srvs = ["onedrive"]
    srvs_instance = map(services.factory, srvs)

    for srv in srvs_instance:
        if not srv.exists(lock):
            srv.put(lock, '')

    from paxos import Proposer
    proposer = Proposer("1", srvs_instance, lock)
    val = proposer.propose("1")
    assert val == "1"
    proposer.done()
    proposer.join()

def test_paxos(metasync, opts):
    "test paxos with disk_api"
    lock = 'locktest/ltest'

    test_init(metasync, opts)
    srvs = metasync.services

    for srv in srvs:
        if not srv.exists(lock):
            srv.put(lock, '')

    from paxos import Proposer

    proposer = Proposer("1", srvs, lock)
    val = proposer.propose("1")
    assert val == "1"
    proposer.done()
    proposer.join()

def test_paxos_services(metasync, opts):
    "test paxos with services"

    # init tmp repo to play with
    #test_init(metasync, opts)

    # init lock primitives
    lock = 'locktest/ltest2'
    targets = ["google", "box", "dropbox"]
    srvs = map(services.factory, targets)
    for srv in srvs:
        if not srv.exists(lock):
            srv.put(lock, '')

    from paxos import Proposer

    proposer = Proposer("1", srvs, lock)
    val = proposer.propose("1")
    assert val == "1"
    proposer.done()
    proposer.join()
    #     XXX. invoke python threads or async
    #       - srv.sid()
    #       - metasync.services
    #       - metasync.lockpath
    #     proposer = Proposer(srv, metasync)
    #     proposer.propose()
    #

    # XXX. check proposed one?

def test_gc(metasync, opts):
    "test garbage collector"

    test_init(metasync, opts)

    # 1. mark and sweep
    root = metasync.get_root_blob()
    print root

    # 2. due to reconfigure
    #   - if version is different
    #   - check all blos in each service

def test_blob_diff(metasync, opts):
    "test blob diff operation"
    test_init(metasync, opts)

    bs = blobs.BlobStore2(metasync)
    blob_dir = blobs.BlobDir2(bs)
    blob_dir.add("file", blobs.BlobFile2(bs))
    blob_dir_sub = blobs.BlobDir2(bs)
    blob_dir.add("dir1", blob_dir_sub)
    blob_dir_sub.add("file2", blobs.BlobFile2(bs))

    blob_dir2 = blobs.BlobDir2(bs)
    blob_dir2.add("file", blobs.BlobFile2(bs))
    blob_dir.diff(blob_dir2)

def test_large_blob(metasync, opts):
    test_init(metasync, opts)
    bs = blobs.BlobStore2(metasync)
    blob_dir = blobs.MBlobDir2(bs)
    pn = os.path.join(opts.root, "a")
    with open(pn, "w") as f:
        f.write("hello world")
    blob_dir.add_file("a", pn)    
    pn = os.path.join(opts.root, "b")
    with open(pn, "w") as f:
        f.write("hello world2")
    pn = os.path.join(opts.root, "b")
    blob_dir.add_file("b", pn)    
    blob_dir.store()

def test_blob(metasync, opts):
    "test blob-related operations"

    test_init(metasync, opts)

    bs = blobs.BlobStore2(metasync)
    blob_dir = blobs.BlobDir2(bs)

    # empty dir
    assert blob_dir.hv is not None \
        and len(blob_dir.entries) == 0

    # add three
    hv0 = blob_dir.hv

    blob_dir.add("dir1", blobs.BlobDir2(bs))
    blob_dir.add("dir2", blobs.BlobDir2(bs))
    blob_dir.add("dir3", blobs.BlobDir2(bs))
    blob_dir.add("file", blobs.BlobFile2(bs))

    hv3 = blob_dir.hv
    assert hv0 != hv3 \
        and len(blob_dir.entries) == 4

    for (name, blob) in blob_dir.entries.iteritems():
        # empty dir
        if isinstance(blob, blobs.BlobDir2):
            assert blob.hv == hv0
        # empty file
        if isinstance(blob, blobs.BlobFile2):
            assert blob.hv != hv0

    # delete one
    blob_dir.rm("dir2")
    hv2 = blob_dir.hv

    assert hv3 != hv2 \
        and len(blob_dir.entries) == 3

    dbg.dbg("hv: %s\n%s" % (hv2, blob_dir.dump()))

    # test store/load
    blob_dir.store()

    # loaded from disk
    loaded_blob = blobs.BlobDir2(bs, hv2)
    assert loaded_blob.dump() == blob_dir.dump()

def test_blob_file(metasync, opts):
    "test blobfile-related operations"

    test_init(metasync, opts)

    bs = blobs.BlobStore2(metasync)
    blob_file = blobs.BlobFile2(bs)

    # empty file
    assert blob_file.hv is not None \
        and len(blob_file.entries) == 0

    # random file with 3 chunks (last one is smaller than unit)
    unit = 1*MB
    size = 3*MB - 2*KB
    pn = os.path.join(opts.tmpdir, "file-%s" % size)
    util.create_random_file(pn, size)

    # store each chunk to blob_file
    blob_file = bs.load_file(pn, unit)

    # check entries and total size
    assert len(blob_file.entries) == 3 and blob_file.size == size

    # test store/load
    blob_file.store()

    # loaded from disk
    loaded_blob = blobs.BlobFile2(bs, blob_file.hv)
    assert loaded_blob.dump() == blob_file.dump()

def test_blob_walk(metasync, opts):
    "test creating/walking a blob dir"

    opts.notree = True

    bs = blobs.BlobStore2(metasync)
    root = blobs.BlobDir2(bs)

    # generate sample tree
    for i in range(1, 3):
        parent_dir = blobs.BlobDir2(bs)
        root.add("dir-%s" % i, parent_dir)
        for j in range(1, 4):
            child_dir = blobs.BlobDir2(bs)
            parent_dir.add("sub-%s" % j, child_dir)
            for k in range(1, 5):
                blob_file = blobs.BlobFile2(bs)
                child_dir.add("file-%s" % k, blob_file)

    # count all entries
    cnt = 0
    for (name, blob) in root.walk():
        dbg.dbg("%-18s: %s" % (name, blob.hv))
        cnt += 1

    assert cnt == 2*3*4 + 2*3 + 2

def test_blob_load(metasync, opts):
    "test loading file/dir from a path"

    _init_disk_metasync(metasync, opts)

    bs = blobs.BlobStore2(metasync)

    # /a/b/c
    dirp = metasync.get_local_path("a", "b", "c")
    util.mkdirs(dirp)

    # /a/b/c/file
    pn = os.path.join(dirp, "file")
    util.create_random_file(pn, 5*KB)

    blob = bs.load_dir(dirp)
    blob.add("file", bs.load_file(pn))

    # count how many blobs
    root = bs.get_root_blob()
    dbg.dbg("%-15s: %s" % ("/", root.hv))

    cnt = 0
    for (name, blob) in bs.walk():
        dbg.dbg("%-15s: %s" % (name, blob.hv))
        cnt += 1

    assert cnt == len(["a", "b", "c", "file"])

    # flush all new blobs
    assert len(os.listdir(metasync.path_objs)) == 0
    root.store()
    assert len(os.listdir(metasync.path_objs)) == 6

    # "." => root
    test_blob = bs.load_dir(metasync.get_local_path("."))
    assert test_blob == root

    test_blob = bs.load_dir(metasync.get_local_path(""))
    assert test_blob == root

def test_bstore_iterate(metasync, opts):
    "walk over all files in a service, and check if correctly distributed"

    test_init(metasync, opts)

    hashes = set()
    for srv in metasync.services:
        for hv in metasync.bstore_iter_remote(srv):
            dbg.info("%-10s: %s" % (srv, hv))
            hashes.add(hv)

    # covered by local's bstore?
    for hv in metasync.bstore_iter():
        hashes.remove(hv)

    assert len(hashes) == 0

def test_bstore_reconfig(metasync, opts):
    "rebalancing all blobs when conf changes"

    test_init(metasync, opts)

    dbg.info("old config: %s" % metasync.mapping)


# all benchmarks
def test_bench_upload(metasync, opts):
    "bencmark upload speed of storage services"

    # bump files
    tmpdir = os.path.join(opts.tmpdir, "metasync-files")
    sizes  = [1024, 2048, 1*MB]
    files  = []

    # for real bench
    if opts.slow:
        sizes = [10*MB, 100*MB]

    util.mkdirs(tmpdir)
    for size in sizes:
        fn = "file-%s" % size
        pn = os.path.join(tmpdir, fn)
        if not os.path.exists(pn):
            util.create_random_file(pn, size)
        files.append(fn)

    # try uploading each file
    result = [["Services"] + files]
    for cls in services.all_services:
        if cls in [services.DiskAPI]:
            continue
        if opt.slow and cls in [services.BaiduAPI]:
            continue
        row = [services.slug(cls)]
        srv = cls()
        print 'uploading:', row[0]

        if srv.exists('/upload_test'):
            srv.rmdir('/upload_test')
        srv.putdir('/upload_test')

        for f in files:
            #if row[0] == 'baidu' and f == 'file-104857600':
            #    continue
            content = open(os.path.join(tmpdir, f), 'r').read()
            beg = time.time()
            srv.put('/upload_test/' + f, content)
            end = time.time()
            row.append(end - beg)

        result.append(row)

    # tabularize
    for row in result:
        for e in row:
            print "%s\t" % e,
        print

def test_bench_download(metasync, opts):
    "bencmark upload speed of storage services"

    # bump files
    sizes  = [1024, 2048, 1*MB]
    files  = []

    # for real bench
    if opts.slow:
        sizes = [10*MB, 100*MB]

    for size in sizes:
        fn = "file-%s" % size
        files.append(fn)

    # try downloading each file
    result = [["Services"] + files]
    for cls in services.all_services:
        if cls in [services.DiskAPI]:
            continue
        if opt.slow and cls in [services.BaiduAPI]:
            continue
        row = [services.slug(cls)]
        srv = cls()
        print 'downloading:', row[0]

        if not srv.exists('/upload_test'):
            print 'Testing files no longer exist in %s' % row[0]
            return

        for f in files:
            #if row[0] == 'baidu' and f == 'file-104857600':
            #    continue
            beg = time.time()
            srv.get('/upload_test/' + f)
            end = time.time()
            row.append(end - beg)

        result.append(row)

    # tabularize
    for row in result:
        for e in row:
            print "%s\t" % e,
        print

def test_concurrent_upload(metasync, opts):

    def _put(srv, path, remote_path):
        with open(path, "rb") as f:
            srv.put(remote_path, f.read())

    # bump files
    tmpdir = os.path.join(opts.tmpdir, "metasync-files")
    sizes  = [1024, 2048, 4192, 8192, 1*MB]
    files  = []
    total_size = 1*MB

    print tmpdir

    util.mkdirs(tmpdir)
    for size in sizes:
        count = total_size / size
        fl = []
        for i in range(count):
            fn = "file-%s-%s" % (size, i)
            pn = os.path.join(tmpdir, fn)
            if not os.path.exists(pn):
                util.create_random_file(pn, size)
            fl.append(fn)
        files.append(fl)

    from metasyncAPI import Worker, ThreadPool
    from multiprocessing import cpu_count

    pool = ThreadPool(cpu_count())

    # try uploading each file
    result = [["Services"] + files]
    for cls in services.all_services:
        if cls in [services.DiskAPI]:
            continue
        row = [services.slug(cls)]
        srv = cls()
        if srv.exists('/concurrent_upload'):
            srv.rmdir('/concurrent_upload')
        srv.putdir('/concurrent_upload')
        print 'uploading:', row[0]

        for fl in files:
            beg = time.time()
            for f in fl:
                path = os.path.join(tmpdir, f)
                remote_path = '/concurrent_upload/%s' % f
                pool.submit(srv.copy, _put, path, remote_path)
            pool.join()
            end = time.time()
            row.append(end - beg)

        result.append(row)

    # tabularize
    for row in result:
        for e in row:
            print "%s\t" % e,
        print

def test_service_auth(metasync, opts):
    dropbox = services.factory('dropbox')
    google = services.factory('google')
    box = services.factory('box')

def test_lock(metasync, opts):
    clone = test_clone(metasync, opts)
    assert metasync.lock_master()
    assert not clone.lock_master()
    metasync.unlock_master()
    assert clone.lock_master()
    clone.unlock_master()

def test_util(metasync, opts):
    "test functions in util.py"

    opts.notree = True

    rtn = ["a", "a/b", "a/b/c"]
    for (crumb, name) in util.iter_path_crumb("./a/b/c/"):
        assert crumb == rtn.pop(0)

def test_merge(metasync, opts):
    clone = test_clone(metasync, opts)
    new_files = [3072, 4096]
    metasyncs = [metasync, clone]

    for i in range(2):
        dbg.info("checkin %d" % i)
        pn = os.path.join(metasyncs[i].path_root, "file-%s" % new_files[i]) 
        util.create_random_file(pn, new_files[i])
        metasyncs[i].cmd_checkin(pn)

    metasync.cmd_push()
    clone.cmd_fetch()
    assert not clone.cmd_push()
    clone.cmd_update()
    assert clone.cmd_push()

def test_mv(metasync, opts):
    test_init(metasync, opts)
    src = os.path.join(metasync.path_root, "file-1024")
    dst = os.path.join(metasync.path_root, "file-1024-2")
    metasync.cmd_mv(src, dst) 

def test_bench_paxos(metasync, opts):
    "bencmark latency of paxos with backends"

    from paxos import Proposer
    from threading import Thread

    class PaxosWorker(Thread):
        def __init__(self, services, path):
            Thread.__init__(self)
            self.clientid = str(util.gen_uuid())
            self.path = path
            self.proposer = Proposer(self.clientid, services, path)
            self.locked = False
            self.latency = 0

        def run(self):
            beg = time.time()
            #if self.proposer.check_locked():
            #    #dbg.dbg("%s already locked" % self.clientid)
            #    return
            val = self.proposer.propose(self.clientid).strip()
            if val == self.clientid:
                self.locked = True
            end = time.time()
            self.latency = max(end - beg, self.latency)
                #dbg.dbg("%s locked %s: %s" % (self.clientid, self.path, end-beg))
                
        def done(self):
            if self.locked:
                self.proposer.done()
            self.proposer.join()

    client_num = [1, 2, 3, 4, 5]
    #client_num = [2]
    backend_list = [["google"], ["dropbox"], ["onedrive"], ["box"], ["baidu"], \
        ["google", "dropbox", "onedrive"], ["google", "box", "dropbox", "onedrive", "baidu"]]
    #backend_list = [["google"], ["dropbox", "google"]]
    # remove test files
    """
    for cls in ["google", "box", "dropbox", "onedrive", "baidu"]:
        srv = services.factory(cls)
        if services.slug(srv) == 'onedrive':
            dirpath = '/Public/lock_test'
        else:
            dirpath = '/lock_test'
        if srv.exists(dirpath):
            srv.rmdir(dirpath)
    #return
    """

    result = [['Clients'] + [','.join(x) for x in backend_list]]

    # start to test
    for num in client_num:
        row = ['%d clients' % num]
        for backend in backend_list:
            dbg.dbg('test paxos for %d clients and %s' % (num, ','.join(backend)))
            path = '/lock_test/ltest-%d-%d' % (num, len(backend))
            srvs = map(services.factory, backend)
            for srv in srvs:
                srv.reset_log(path)
            for srv in srvs:
                srv.init_log(path)
            clients = []
            for i in range(num):
                srvs = map(services.factory, backend)
                worker = PaxosWorker(srvs, path)
                clients.append(worker)
                #dbg.dbg('client %d %s' % (i, worker.clientid))
            for worker in clients:
                worker.start()
            latency = [] 
            lock_latency = None
            for worker in clients:
                worker.join()
                latency.append(worker.latency)
                if(worker.locked):
                    assert lock_latency is None
                    lock_latency = worker.latency
            for worker in clients:
                worker.done()
            row.append(",".join(map(str,[min(latency), sum(latency)/float(len(latency)), lock_latency, max(latency)])))
        result.append(row)

    # tabularize
    for row in result:
        for e in row:
            print "%s\t" % e,
        print


def test_bench_disk_paxos(metasync, opts):
    "test disk paxos"
    "bencmark latency of paxos with backends"

    from disk_paxos import Proposer
    from threading import Thread

    class PaxosWorker(Thread):
        def __init__(self, services, block, blockList):
            Thread.__init__(self)
            self.clientid = str(util.gen_uuid())
            dbg.dbg("Client %s" % self.clientid)
            self.block = block
            self.proposer = Proposer(self.clientid, services, block, blockList)
            self.locked = False
            self.latency = 0

        def run(self):
            beg = time.time()
            #if self.proposer.check_locked():
            #    #dbg.dbg("%s already locked" % self.clientid)
            #    return
            val = self.proposer.propose(self.clientid).strip()
            if val == self.clientid:
                self.locked = True
                dbg.dbg("Proposal result: %s" % val)
            end = time.time()
            self.latency = max(end - beg, self.latency)
            # dbg.dbg("%s locked %s: %s" % (self.clientid, self.path, end-beg))
                
        def done(self):
            self.proposer.join()

    client_num = [1, 2, 3, 4, 5]
    backend_list = [["google"], ["dropbox"], ["onedrive"], ["box"], ["baidu"], \
        ["google", "dropbox", "onedrive"], ["google", "box", "dropbox", "onedrive", "baidu"]]
    # backend_list = [["google"], ["dropbox", "google"]]
    # remove test files

    result = [['Clients'] + [','.join(x) for x in backend_list]]

    # start to test
    for num in client_num:
        for backend in backend_list:
            srvs = map(services.factory, backend)
    
            for num_prop in range(1, num + 1):
                dbg.info('test paxos for %d/%d clients and %s' % (num_prop, num, ','.join(backend)))

                # initialize all disk blocks
                blockList = []
                for i in range(num):
                    path = '/diskpaxos/client%d' % i
                    for srv in srvs:
                        if not srv.exists(path):
                            srv.put(path, '')
                        else:
                            srv.update(path, '')
                    blockList.append(path)

                clients = []
                for i in range(num_prop):
                    storages = map(services.factory, backend)
                    worker = PaxosWorker(storages, blockList[i], blockList)
                    clients.append(worker)
                    #dbg.dbg('client %d %s' % (i, worker.clientid))

                for worker in clients:
                    worker.start()

                latency = [] 
                lock_latency = None
                for worker in clients:
                    worker.join()
                    latency.append(worker.latency)
                    if(worker.locked):
                        assert lock_latency is None
                        lock_latency = worker.latency

                for worker in clients:
                    worker.done()

                row = ['%d/%d clients' % (num_prop, num)]
                row.append(",".join(map(str,[min(latency), sum(latency)/float(len(latency)), lock_latency, max(latency)])))
                result.append(row)

    # tabularize
    for row in result:
        for e in row:
            print "%s\t" % e,
        print

def test_sid(metasync, opts):
    import services
    allset = set()
    for srv, doc in services.backends():
        if(srv != "disk"):
            instance = services.factory(srv)
            assert instance is not None
            sid = instance.sid()
            print(sid, instance)
            assert sid is not None
            assert sid not in allset
            allset.add(sid)

def test_bench_latency(metasync, opts):
    import services
    allset = set()
    path = '%d' % time.time()
    with open("/dev/urandom") as ifd:
        content = ifd.read(128)

    for srv, doc in services.backends():
        if(srv != "disk"):
            instance = services.factory(srv)
            beg = time.time()
            instance.put(path,content)
            end = time.time()
            print(srv + " up " + str(end-beg))
            beg = time.time()
            ct = instance.get(path)
            end = time.time()
            print(srv + " dn " + str(end-beg))

def test_mapping_fairness(metasync, opts):
    "test the fairness of mapping scheme"

    import string
    import random

    def evaluate(count, config):
        N = sum(count)
        C = sum(map(lambda x: x[1], config))
        score = 0.0
        for srv in config:
            score += (1.0*count[srv[0]]/srv[1] - 1.0*N/C) ** 2
        return score

    config = [(0,2), (1,7), (2,10), (3,15)]
    nspace = sum(map(lambda x: x[1], config))
    result = [['replication', 'factor', 'result', 'fairness', 'score']]
    N = 50
    random.seed(0)

    for replication in range(1, 4):
        for factor in range(100, 1001, 100):

            hspace = factor * nspace
            detmap = DetMap2(config, hspace, replication)
            count = [0, 0, 0, 0]
            
            for _ in range(5000):
                randstr = ''.join(random.choice(string.letters + string.digits) for _ in range(N))
                hashid = util.sha1(randstr)

                lst = detmap.get_mapping(hashid)
                for i in lst:
                    count[i] += 1
            fairness = [1.0 * count[i] / config[i][1] for i in range(4)]
            score = evaluate(count, config)
            row = [replication, factor, count, fairness, score]
            result.append(row)

    for row in result:
        for e in row:
            print "%s\t" % e,
        print

def test_mapping_dist(metasync, opts):
    mapping = [("dropbox", 2), ("google", 15), ("box", 10), ("onedrive", 7), ("baidu", 2048)]
    mapping = map(lambda x:(util.md5(x[0])%10000,x[1]), mapping)
    print(mapping)
    hspace = (2+15+10+7+2048)*5
    objs = []
    with open("result/linux_objs.txt") as f:
        for line in f:
            sp = line.strip().split("\t")
            hv = sp[0] 
            size = int(sp[1])
            objs.append( (hv, size) )

    for replication in range(1, 4):
        detmap = DetMap2(mapping, hspace, replication)
        sizes = {}
        counts = {}
        for srv, sz in mapping:
            sizes[srv] = 0
            counts[srv] = 0

        for obj in objs:
            hv = obj[0]
            size = obj[1]
            lst = detmap.get_mapping(hv)
            for srv in lst:
                counts[srv] += 1
                sizes[srv] += size
        print replication, 
        for srv, sz in mapping:
            print "%d/%d" % (counts[srv],sizes[srv]),

        print

