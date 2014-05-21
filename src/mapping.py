
from params import *
import dbg, util

class DetMapInfo2:
    def __init__(self, config, hspace, replica, version):
        self.config = config
        self.replica = replica
        self.hspace = hspace
        self.version = version
        self._load(config)

    def _load(self, config):
        nspace = []
        for n, size in config:
            for s in range(size):
                nspace.append((n,s))
        assert len(nspace) < self.hspace
        self.detmap = [None] * self.hspace
        for i in range(self.hspace):
            group = []
            for n in nspace:
                order = int(util.md5("%s.%s" % (i, n)))
                group.append((n[0], order))
            self.detmap[i] = sorted(group, key=lambda e:e[1])

    def pack(self):
        for i in range(self.hspace):
            lst = []
            prev = -1
            for j in self.detmap[i]:
                if(j[0] != prev):
                    lst.append((j[0],0))
                prev = j[0]
            self.detmap[i] = lst


def uniq(lst, n):
    rtn = []
    for (i, _) in lst:
        if i in rtn:
            continue
        rtn.append(i)
        if len(rtn) == n:
            break
    return rtn


class DetMap2:
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

    def __init__(self, config, hspace=1024, replica=2, version=0):
        self.version = version
        self.mapinfo = {}
        self.hspace = hspace
        self.mapinfo[version] = DetMapInfo2(config, hspace, replica, version)

    def reconfig(self, config, replica=2):
        self.version += 1
        self.mapinfo[self.version] \
            = DetMapInfo2(config, self.hspace, replica, self.version)

    def pack(self):
        self.mapinfo[self.version].pack()

    def get_remapping(self, hvs):
        assert self.version > 0
        added = {}
        removed = {}
        for srv, sz in (self.mapinfo[self.version].config + self.mapinfo[self.version-1].config):
            added[srv] = []
            removed[srv] = [] 

        for hv in hvs:
            old_map = self.get_mapping(hv, self.version - 1)
            new_map = self.get_mapping(hv, self.version)

            for srv in list(set(new_map) - set(old_map)):
                added[srv].append(hv)
            for srv in list(set(old_map) - set(new_map)):
                removed[srv].append(hv)
        return added,removed

    # get mapping info of hash value (on the latest version)
    def get_mapping(self, hv, version=None):
        # latest version by default
        if version is None:
            version = self.version
        # bigbang moment
        if version < 0:
            version = 0

        if type(hv) is str:
            hv = int(hv, 16)

        ver_modulo  = self.mapinfo[version].hspace
        ver_replica = self.mapinfo[version].replica

        i = hv % ver_modulo
        ver_detmap = self.mapinfo[version].detmap[i]
        return uniq(ver_detmap, ver_replica)


class DetMapInfo:
    def __init__(self, config, replica, version):
        self.config  = config
        self.replica = replica
        self.version = version
        self.distrib = map_to_distirb(config)
        self.modulo  = len(self.distrib)

    def store(self):
        return "%s:%s:%s" % (self.version, self.replica, self.config)

    @classmethod
    def restore(cls, store):
        # poor man's marshaling
        (version, replica, config) = store.split(":")
        return DetMapInfo(eval(config), int(replica), int(version))




class DetMap:
    #
    # interesting aspects/requirements of our settings
    #
    #  - heterogeneous nodes: different storage size
    #  - quick recovery (from local copy)
    #  - mininum info to keep the mapping and its changes
    #  - configuration changes (superset of node failure)
    #  - role of gc for balancing
    #
    # config: [(1, 2GB), (2, 5GB), (3, 2GB)]
    def __init__(self, config, replica=2, version=0):
        # normalize config
        config.sort(key=lambda t:t[0])

        self.version = version
        self.mapinfo = {}
        self.mapinfo[version] = DetMapInfo(config, replica, version)

    @property
    def replica(self):
        return self.mapinfo[self.version].replica
    @property
    def config(self):
        return self.mapinfo[self.version].config
    @property
    def distrib(self):
        return self.mapinfo[self.version].distrib
    @property
    def modulo(self):
        return self.mapinfo[self.version].modulo

    def reconfig(self, config, replica=2):
        # NOTE. do not support replica changes yet
        assert replica == self.replica

        self.version += 1
        self.mapinfo[self.version] \
            = DetMapInfo(config, replica, self.version)

    # get mapping info of hash value (on the latest version)
    def get_mapping(self, hv, version=None):
        # latest version by default
        if version is None:
            version = self.version
        # bigbang moment
        if version < 0:
            version = 0

        if type(hv) is str:
            hv = int(hv, 16)

        ver_modulo  = self.mapinfo[version].modulo
        ver_replica = self.mapinfo[version].replica
        ver_distrib = self.mapinfo[version].distrib

        i = hv % ver_modulo
        m = []

        while len(m) != ver_replica:
            v = ver_distrib[i]
            if v not in m:
                m.append(v)
            i = (i + 1) % ver_modulo
        return m

    # get re-mapping info of hash value (against the previous one)
    def get_remapping(self, hv):
        old_map = self.get_mapping(hv, self.version - 1)
        new_map = self.get_mapping(hv, self.version)

        # rebalance missing blob
        return list(set(new_map) - set(old_map))

    def store(self):
        ret = []
        for (ver, info) in self.mapinfo.iteritems():
            ret.append(info.store())
        return "\n".join(ret)

    @classmethod
    def restore(cls, store):
        mapinfo = {}
        for l in store.splitlines():
            info = DetMapInfo.restore(l)
            mapinfo[info.version] = info

        version = max(mapinfo.keys())
        config  = mapinfo[version].config
        replica = mapinfo[version].replica

        m = DetMap(config, replica, version)
        m.mapinfo = mapinfo

        return m

    def __str__(self):
        return "map:%s@%s" % (self.config, self.version)

# config: [(1, 2GB), (2, 5GB), (3, 2GB)]
#           -> (1, 2, 3, 1, 2, 3, 2, 2, 2)
def normalized(config):
    return [(id, size*100//GB) for (id, size) in config]

def map_to_distirb(config):
    q = [[id] * cap for (id, cap) in normalized(config)]
    m = []
    i = 0
    while len(q) != 0:
        i %= len(q)
        m.append(q[i].pop())
        if len(q[i]) == 0:
            del q[i]
            continue
        i += 1
    return tuple(m)