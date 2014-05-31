import os
import dbg
import util

from params import *
import cStringIO
#
# BlobFile (inode)
#  - unit : size of each chunk
#  - size : size of current chunk
#  - (offset, hash)*: list of hash blobs
#
#  - read(): read concatinated blobs
#
# BlobDir (dentry)
#  - (type, name, BlobFile|BlobDir)*: list of hash blobs and their types
#      e.g., (f, name, BlobFile) or (d, name, BlobDir)
#
#  - walk(): iterate child BlobFile/BlobDir
#  - diff(): diff against other BlobDir
#  - list(): all blobs inherited from this BlobDir
#
# BlobStore
#   - path objs
#   - path master
#
#   root(): BlobDir
#   walk(): iterate (path, BlobDir/BlobFile)
#   list(): all blobs
#   diff(): diff against another BlobStore
#
class NotTrackedException(Exception):
    def __init__(self, path):
        self.path = path
    def __str__(self):
        return "%s is not tracked by metasync" % self.path

# managing in-memory structure for blobs
class BlobStore2:
    # each metasync instance has a single BlobStore (META_DIR/objects)
    def __init__(self, metasync):
        self.metasync = metasync
        self.rootblob = None
        self.added = set([])

    def get_root_blob(self):
        # if not loaded yet
        if self.rootblob is None:
            head = self.metasync.get_head_value()
            # create an empty rootblob
            if head is None or head == "":
                self.rootblob = BlobDir2(self)
            # load from disk
            else:
                self.rootblob = BlobDir2(self, head)
        return self.rootblob

    # return local file path of hv
    def get_path(self, hv):
        return self.metasync.get_local_obj_path(hv)

    # a factory
    def get_blob(self, hv, thv):
        assert thv in [BlobDir2.thv, BlobFile2.thv, BlobChunk2.thv, MBlobDir2.thv, MBlobFile2.thv]

        if thv == BlobDir2.thv:
            blob = BlobDir2(self, hv)
        elif thv == BlobFile2.thv:
            blob = BlobFile2(self, hv)
        elif thv == BlobChunk2.thv:
            blob = BlobChunk2(self, hv)
        elif thv == MBlobDir2.thv:
            blob = MBlobDir2(self, hv)

        return blob

    # store given a single blob to disk
    def store(self, blob):
        pn = self.get_path(blob.hv)
        self.added.add(blob.hv)
        with open(pn, "w") as fd:
            fd.write(blob.dump())

    # retrieve newly added blobs
    def get_added_blobs(self):
        ret = list(self.added)
        self.added = set([])
        return ret

    # iterate with (name, blob)
    def walk(self):
        root = self.get_root_blob()
        for (name, blob) in root.walk():
            yield ("/" + name, blob)

    # load BlobFile from a path
    def load_file(self, pn, unit=BLOB_UNIT):
        if not os.path.isfile(pn):
            return None

        blob = BlobFile2(self, mtime=os.path.getmtime(pn))
        for (offset, bytes) in util.each_chunk2(pn, unit):
            chunk = BlobChunk2(self, chunk=bytes)
            blob.add(offset, chunk)
        return blob

    # load BlobDir from a path (generate if non-existent)
    def load_dir(self, pn, generate=True, dirty=False, merge=False):

        if not os.path.isdir(pn):
            return None

        blob = self.get_root_blob()
        abspath = os.path.abspath(pn)
        relpath = self.metasync.get_relative_path(abspath)
        if(relpath == "."): 
            return blob
        for (_, name) in util.iter_path_crumb(relpath):
            # create one if not exist
            if not name in blob:
                if generate:
                    if merge:
                        blob.add(name, MBlobDir2(self))
                    else:
                        blob.add(name, BlobDir2(self))
                else:
                    raise NotTrackedException(name)
            elif dirty:
                blob.add(name, blob[name])
            # fetch child
            blob = blob[name]

        return blob

    def list(self):
        return os.listdir(self.metasync.path_objs)


class LazyBlob(object):

    # de/serialization of blobs
    def _eval_header(self, line):
        raise Exception("Not implemented")
    def _repr_header(self):
        raise Exception("Not implemented")
    def _eval_entry(self, line):
        raise Exception("Not implemented")
    def _repr_entry(self, name, blob):
        raise Exception("Not implemented")

    # invoked whenever hv is recomputed
    def _updated(self):
        pass

    def __init__(self, blobstore, hv=None):
        self.bs = blobstore

        # initial hv (to be loaded from disk)
        self._hv = hv
        # is _hv dirty? any changes meanwhile? (clean up by hv())
        self._dirty = False
        # name directory of BlobFile/Dir
        self._entries = None
        # sorted listed of keys (names)
        self._sorted = None
        # dirty blobs in entries (clean up by store())
        self._dirties = {}

        # empty blobdir
        if self._hv is None:
            self._entries = {}

    def _load(self):
        if self._entries is not None:
            return

        self._entries = {}
        pn = self.bs.get_path(self.hv)

        firstline = True
        for line in open(pn):
            # processing header line
            if firstline:
                self._eval_header(line)
                firstline = False
                continue
            # processing entries
            self._entries.update(self._eval_entry(line))

        # strong assert!
        assert self.hv == util.sha1(self.dump())

    @property
    def hv(self):
        # dirty, recompute the hv
        if self._dirty or self._hv is None:
            self._hv = util.sha1(self.dump())
            self._dirty = False
            self._updated()

        return self._hv

    @property
    def entries(self):
        self._load()
        return self._entries

    def add(self, name, blob, dirty=True):
        assert isinstance(blob, BlobFile2) \
            or isinstance(blob, BlobDir2)  \
            or isinstance(blob, BlobChunk2) \
            or isinstance(blob, MBlobDir2) \
            or isinstance(blob, MBlobFile2)

        self._load()
        self._entries[name] = blob

        if(dirty):
            self._dirty = True
            self._sorted = None
            self._dirties[name] = blob

    def rm(self, name):
        self._load()

        # remove only if we have such a file
        if name in self._entries:
            del self._entries[name]
            self._dirty = True
            self._sorted = None

            # if the one in dirties
            if name in self._dirties:
                del self._dirties[name]
        else:
            dbg.dbg("we don't have such a file")

    def dump(self):
        rtn = [self._repr_header()]
        if self._sorted is None:
            self._sorted = sorted(self.entries.keys())
        for name in self._sorted:
            blob = self.entries[name]
            rtn.append(self._repr_entry(name, blob))
        return "\n".join(rtn)

    def store(self):
        self.bs.store(self)
        for blob in self._dirties.values():
            blob.store()
        self._dirties = {}

    # support 'name in blob'
    def __contains__(self, item):
        return item in self.entries

    def __getitem__(self, name):
        return self.entries[name]

class BlobDir2(LazyBlob):
    thv = "D"

    #
    # <disk format>
    # [(type, name, hv)\n]+
    #
    def __init__(self, blobstore, hv=None):
        super(BlobDir2, self).__init__(blobstore, hv)

    def _eval_header(self, line):
        pass

    def _repr_header(self):
        return "None"

    def _eval_entry(self, line):
        (thv, name, hv) = eval(line)
        return {name: self.bs.get_blob(hv, thv)}

    def _repr_entry(self, name, blob):
        return repr((blob.thv, name, blob.hv))

    # directory specific operations
    def walk(self):
        # depth first walking
        for (name, blob) in self.entries.iteritems():
            yield (name, blob)
            # walk items in subdirectory
            if isinstance(blob, BlobDir2):
                for (child_name, child_blob) in blob.walk():
                    yield (os.path.join(name, child_name), child_blob)

    def diff(self, other):
        # currently takes only added
        mylst = {} 
        for path, blob in self.walk():
            mylst[path] = blob
        otherlst = {}
        for path, blob in other.walk():
            otherlst[path] = blob

        added = {}
        for path in mylst:
            blob = mylst[path]
            if(not otherlst.has_key(path) and blob.thv == "F"):
                added[path] = mylst[path]
        return added

class BlobFile2(LazyBlob):
    thv = "F"

    #
    # <disk format>
    # (unit, size)
    # [blob]*
    #
    def __init__(self, blobstore, hv=None, unit=BLOB_UNIT, mtime=None):
        super(BlobFile2, self).__init__(blobstore, hv)

        #
        # NOTE. cached size info
        #  it will be updated whenever add/rm of chunks
        #
        self._size = None
        self.unit = unit
        #self.mtime = mtime
        #XXX mtime confuses hashing. let's think more on how to handle..
        self.mtime = None

    def _eval_header(self, line):
        (unit, size, mtime) = eval(line)
        self.unit = unit
        self._size = size
        self.mtime = mtime

    def _repr_header(self):
        return repr((self.unit, self.size, self.mtime))

    def _eval_entry(self, line):
        (offset, hv) = eval(line)
        return {offset: self.bs.get_blob(hv, BlobChunk2.thv)}

    def _repr_entry(self, offset, blob):
        return repr((offset, blob.hv))

    def _updated(self):
        total = 0
        for chunk in self.entries.values():
            total += chunk.size
        self._size = total

    @property
    def size(self):
        # if entries changed, or size is not updated yet
        if self._dirty or self._size is None:
            self._updated()
        return self._size

    def read(self):
        # concat bytes from all chunks
        if self._sorted is None:
            self._sorted = sorted(self.entries.keys())
        # TODO. use cStringIO
        ret = cStringIO.StringIO()
        for offset in self._sorted:
            ret.write(self.entries[offset].chunk)
            #ret += self.entries[offset].chunk
        return ret

class BlobChunk2:
    thv = "C"
    def __init__(self, blobstore, hv=None, chunk=None):
        self.bs     = blobstore
        self._chunk = chunk
        self._hv    = hv

        # should provide one of hv or chunk
        assert self._chunk is not None \
            or self._hv is not None

    @property
    def hv(self):
        if self._hv is None:
            self._hv = util.sha1(self.chunk)
        return self._hv

    @property
    def chunk(self):
        if self._chunk is None:
            pn = self.bs.get_path(self._hv)
            with open(pn, "rb") as fd:
                self._chunk = fd.read()
        return self._chunk

    @property
    def size(self):
        return len(self.chunk)

    def store(self):
        self.bs.store(self)

    def dump(self):
        return self.chunk

class MBlobFile2(LazyBlob):
    thv = "m"
    def __init__(self, chunk, offset, size):
        self.chunk = chunk
        self.offset = offset
        self.size = size

    @property
    def hv(self):
        return self.chunk.hv

    def read(self):
        return self.chunk.chunk[self.offset:self.offset+self.size]

    def store(self):
        pass

class MBlobDir2(BlobDir2):
    thv = "M"
    def __init__(self, blobstore, hv=None):
        super(MBlobDir2, self).__init__(blobstore, hv)
        self.chunk = BlobChunk2(blobstore, chunk="")
        self._fileIO = None
        self._files = {}
        self._offset = 0

    def _eval_header(self, line):
        pass

    def _repr_header(self):
        return "None"

    def _repr_entry(self, name, blob):
        if(blob.thv == "m"):
            return repr((blob.thv, name, blob.hv, blob.offset, blob.size))
        else:
            return repr((blob.thv, name, blob.hv, 0, 0))

    def _eval_entry(self, line):
        (thv, name, hv, offset, size) = eval(line)
        if(thv == "m"):
            self.chunk = BlobChunk2(self.bs, hv=hv)
            return {name: MBlobFile2(self.chunk, offset, size)}
        else: 
            return {name: self.bs.get_blob(hv, thv)}

    @property
    def fileIO(self):
        if(self._fileIO is None):
            self._fileIO = cStringIO.StringIO()
        return self._fileIO

    def add_file(self, name, pn):
        with open(pn) as f:
            read = f.read()
            size = len(read)
            self.fileIO.write(read)
            self._files[name] = (self._offset, size)
            self._offset += size

    def done_adding(self):
        self.chunk.chunk = self.fileIO.getvalue() 
        self.chunk.store()
        self.fileIO.close()
        self._fileIO = None
        for name in self._files:
            entry = self._files[name]
            self.add(name, MBlobFile2(self.chunk, entry[0], entry[1]))
            
    # def store(self):
    #     self.bs.store(self)
    #     for blob in self._dirties.values():
    #         blob.store()
    #     self._dirties = {}
