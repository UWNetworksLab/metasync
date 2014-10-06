import os
import io
import sys
import time
import errno
import hashlib
import uuid
import ConfigParser

def median(lst):
    sortlst = sorted(lst)
    size = len(sortlst)
    if size % 2 == 1:
        return sortlst[(size - 1) / 2]
    else:
        lower = sortlst[size / 2 - 1]
        upper = sortlst[size / 2]
        return float(lower + upper) / 2.0

def current_sec():
  return time.mktime(time.localtime())

# only for stroage api
def format_path(path):
    if not path:
        return path
        
    if path[0] == ".":
        path = path[1:]
    return unicode('/' + path.strip('/'))

# convert time to seconds in metadata
def convert_time(timestr):
    from dateutil import tz
    from dateutil.parser import parse
    remotetime = parse(timestr)
    localtime = remotetime.astimezone(tz.tzlocal())

    return time.mktime(localtime.timetuple())

# when break, call pdb
def install_pdb():
    def info(type, value, tb):
        if hasattr(sys, 'ps1') or not sys.stderr.isatty():
            # You are in interactive mode or don't have a tty-like
            # device, so call the default hook
            sys.__execthook__(type, value, tb)
        else:
            import traceback
            import pdb
            # You are not in interactive mode; print the exception
            traceback.print_exception(type, value, tb)
            print
            # ... then star the debugger in post-mortem mode
            pdb.pm()

    sys.excepthook = info

def sha1(content):
    s = hashlib.sha1()
    s.update(content)
    return s.hexdigest()

def empty_file(path):
    with open(path, "w"):
        pass

def read_file(path, size=None):
    with open(path, "r") as fd:
        if size is None:
            return fd.read()
        else:
            return fd.read(size)
    return None

def write_file(path, content):
    with open(path, "w") as fd:
        fd.write(content)
        return True
    return False

def append_file(path, content):
    with open(path, "a") as fd:
        fd.write(content)
        return True
    return False

def create_random_file(path, size):
    if not os.path.exists(path):
        with open("/dev/urandom") as ifd:
            with open(path, "w") as ofd:
                unit = 4096
                while size > 0:
                    buf = ifd.read(min(unit, size))
                    ofd.write(buf)
                    size -= len(buf)
    return path

def mkdirs(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            return True
        return False
    return True

# deprecated: don't compute sha1 ahead of time
def each_chunk(path, unit):
    with open(path, "rb") as f:
        while True:
            content = f.read(unit)
            hashname = sha1(content)
            yield (hashname, content)
            if len(content) < unit:
                break

# iter over (offset, content)
def each_chunk2(path, unit):
    offset = 0
    with open(path, "rb") as f:
        while True:
            content = f.read(unit)
            yield (offset, content)
            if len(content) < unit:
                break
            offset += unit

# iterate over path crumb
#   e.g.) "a/b/c" -> ["a", "a/b", "a/b/c"]
#        "/a/b/c" -> ["a", "a/b", "a/b/c"]
#       "./a/b/c" -> ["a", "a/b", "a/b/c"]
def iter_path_crumb(pn):
    # massage a bit
    if pn[0] == ".":
        pn = pn[1:]

    pn = pn.strip(os.sep)

    # iterate from root to child
    crumb = ""
    for p in pn.split(os.sep):
        crumb = os.path.join(crumb, p)
        yield (crumb, p)

# return printable or not
def to_printable(c):
    if 33 <= ord(c) <= 125:
        return c
    return "."

def hexdump(blob):
    bytes = map(lambda c: "%02X" % ord(c), blob)
    bytes.extend(["  "] * 0x10)

    line = []
    for offset in range(0, len(bytes)/0x10):
        s = offset * 0x10
        e = s + 0x10

        col_hex = " ".join(bytes[s:e])
        col_str = "".join(map(to_printable, blob[s:e]))

        if col_str != "":
            line.append("%08X: %s %s" % (s, col_hex, col_str))

    return "\n".join(line)

def md5(arg):
    return int(hashlib.md5(arg).hexdigest(), 16)

def gen_uuid():
    return uuid.uuid1()

# config-related
def load_config(pn=None):
    config = new_config()
    if pn is not None:
        with open(pn) as fd:
            config.readfp(fd)
    return config

def loads_config(content):
    config = new_config()
    config.readfp(io.BytesIO(content))
    return config

def new_config():
    return ConfigParser.ConfigParser()
