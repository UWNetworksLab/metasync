import sys
import time
import logging
import dbg
import copy

from watchdog.observers import Observer
#from watchdog.events import FileSystemEventHandler
from watchdog.events import PatternMatchingEventHandler 

from params import *

#CHECK THREAD-SAFE
class MetaSyncDaemon(PatternMatchingEventHandler):
    def __init__(self, metasync):
        super(MetaSyncDaemon, self).__init__(ignore_patterns=[metasync.path_meta+"/*"])
        self.metasync = metasync
        self.queue = set()
        self.dirs = set()
        self.lastupdate = time.time()

    def on_created(self, event):
        if(event.is_directory):
            self.dirs.add(event.src_path)
        else:
            self.queue.add(event.src_path)
        self.lastupdate = time.time()

    def on_deleted(self, event):
        self.lastupdate = time.time()
        #self.queue.add(event.src_path)

    def on_modified(self, event):
        if(event.is_directory): return
        self.queue.add(event.src_path)
        self.lastupdate = time.time()

    def on_moved(self, event):
        # XXX. not sure about its semantics, src/dst?
        # src_path => dest_path
        dbg.dbg(str(event))
        self.lastupdate = time.time()
        #self.queue.add(event.src_path)

    def get_all_files(self):
        rtn = copy.copy(self.queue)
        rtn2 = copy.copy(self.dirs)
        self.queue = set()
        self.dirs = set()
        return rtn, rtn2

def start(metasync, args, opts):
    if not metasync.check_sanity():
        dbg.err("Not a metasync directory")
        exit(1)

    daemon = MetaSyncDaemon(metasync)

    # invoke observer
    if(not opts.nocheckin):
        observer = Observer()
        observer.schedule(daemon,
                          metasync.path_root,
                          recursive=True)
        observer.start()

    # stupid poll - change to event-driven.
    try:
        while True:
            time.sleep(SYNC_WAIT)
            if(time.time()-daemon.lastupdate < 0.5): continue

            # batching -> TODO. commit
            files, dirs = daemon.get_all_files()
            files = list(files)
            for d in dirs:
                files = filter(lambda x:not x.startswith(d), files)
            for d in dirs:
                files.append(d)

            # TODO: can we do it together?
            if(len(files) > 0):
                leftover = metasync.cmd_checkin(files, upload_only_first=True)
                metasync.cmd_push()
                if(len(leftover) > 0):
                    metasync.bstore_sync_left(leftover)
                #dbg.info("%s" % files)

            #STUPID pull --- check fetch
            metasync.cmd_fetch()
            metasync.cmd_update()


    except KeyboardInterrupt:
        observer.stop()
    observer.join()

