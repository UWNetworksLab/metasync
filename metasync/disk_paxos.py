#!/usr/bin/env python

import time
import random
import traceback

import services
from threading import Thread
from Queue import Queue
from params import PAXOS_PNUM_INC

import dbg
import util
from error import ItemDoesNotExist

class Worker(Thread):
  def __init__(self, storage, results):
    Thread.__init__(self)
    self.storage = storage
    self.tasks = Queue(10)
    self.results = results
    self.daemon = True
    self.start()

  def create(self, path):
    if not self.storage.exists(path):
      self.storage.put(path, '')

  def append(self, path, content):
    self.storage.append(path, content)

  def readBlockList(self, path):
    logs, clock = self.storage.get_logs(path, None)
    blocks = []
    for log in logs:
      if len(log['message']) > 0:
        blocks.append(log['message'])
    return blocks

  def readBatch(self, pathlist):
    ret = []
    for path in pathlist:
      try:
        content = self.storage.get(path).strip(' \0')
        if len(content) > 0:
          ret.append(content)
      except ItemDoesNotExist:
        pass
    return ret

  def set(self, path, content):
    self.storage.update(path, content)

  def run(self):
    while True:
      ind, funcname, sync, args, kargs = self.tasks.get()
      if ind == -1:
        self.tasks.task_done()
        break
      try:
        func = getattr(self, funcname)
        ret = func(*args, **kargs)
      except Exception:
        traceback.print_exc()
      if sync: self.results.put((ind, ret))
      self.tasks.task_done()

  def join(self):
    self.tasks.put( (-1, None, None, None, None) )
    super(Worker, self).join()

class PaxosThreadPool(object):
  def __init__(self, storages):
    self.index = 0
    self.ndisk = len(storages)
    self.results = Queue(self.ndisk * 10)
    self.workers = []
    for storage in storages:
      worker = Worker(storage, self.results)
      self.workers.append(worker)

  # wait: num | 'majority' | 'all'
  def submit(self, funcname, wait, *args, **kargs):
    #dbg.paxos_time("%s %s" % (funcname, self.cmdId))
    if type(wait) is int:
      waitcount = wait
    elif wait == 'majority':
      waitcount = self.ndisk / 2 + 1
    elif wait == 'all':
      waitcount = self.ndisk

    sync = True if waitcount > 0 else False
    for worker in self.workers:
      # print '%s: %s %s(%s) %s' % (self.index, services.slug(worker.storage), funcname, args, sync)
      worker.tasks.put((self.index, funcname, sync, args, kargs))

    results = None
    if waitcount > 0:
      count = 0
      results = []
      while count < waitcount:
        beg = time.time()
        ind, ret = self.results.get()
        end = time.time()
        # dbg.paxos_time("submit %s %s %s" % (index, funcname, end-beg))
        beg = time.time()
        if ind == self.index:
          count += 1
          results.append(ret)
        self.results.task_done()
        end = time.time()

    self.index += 1
    return results

  def join(self):
    for worker in self.workers:
      worker.join()

class Proposer(object):

  # block is the file path of this client
  # blocklist contains all file blocks (include this client)
  def __init__(self, clientid, storages, block, blockList):
    self.clientid = clientid
    self.block = block
    self.blockList = blockList[:]
    self.blockList.remove(self.block)
    
    self.pnum = None
    self.pval = None
    self.threadpool = PaxosThreadPool(storages)

    # self._init_block()

  """
  def _get_block_list(self):
    results = self.threadpool.submit('readBlockList', 'majority', self.blockListFile)
    blocks = []
    for disk in results:
      if disk is not None:
        blocks = list(set(blocks) | set(disk))
    return blocks

  def _init_block(self):
    self.threadpool.submit('create', 0, self.block)
    blocks = self._get_block_list()
    if self.block not in blocks:
      self.threadpool.submit('append', 0, self.blockListFile, self.block)
  """

  def _init_pnum(self):
    self.pnum = random.randint(0, 30)

  def _debug_time(self, msg):
    cur = time.time()
    dbg.paxos_time("%s: %s" % (msg, cur-self.starttime))

  def propose(self, value):
    # user should first call check_locked
    self.starttime = time.time()
    random.seed(time.time())
    exp_backup = 0.5

    # retrieve the block list and remove my block
    # blocks = self._get_block_list()
    # blocks.remove(self.block)

    # init pval & pnum
    self.pnum = None
    self.pval = None

    while True:
      val = self.propose_once(value)
      if val != None:
        self._debug_time("done")
        return val
      else:
        self._debug_time("another round")
        self.pnum += PAXOS_PNUM_INC
        time.sleep(exp_backup+random.random()) # sleep 1 second, wait for others propose
        exp_backup *= 2

  def propose_once(self, value):

    if self.pnum is None:
      self._init_pnum()
    self.pval = None

    # send prepare messages
    msg = '%s,%s,%s' % (self.clientid, self.pnum, self.pval)
    self.threadpool.submit('set', 0, self.block, msg)
    # dbg.dbg('set %s' % msg)

    # read majority blocks
    results = self.threadpool.submit('readBatch', 'majority', self.blockList)
    
    # check if any value is committed
    for disk in results:
      for block in disk:
        block = block.strip()
        if block.endswith('#'):
          clientid, pnum, pval = block.rstrip('#').split(',')
          return pval

    # check if promised by majority and find the candidate proposal
    candidate = None
    for disk in results:
      for block in disk:
        clientid, pnum, pval = block.split(',')
        pnum = eval(pnum)
        # check whether to abandon this round
        # if both proposal have the same pnum, client with bigger id wins
        if pnum > self.pnum or (pnum == self.pnum and clientid > self.clientid):
          return None
        if pval != 'None':
          if candidate is None or pnum > candidate[1] or (pnum == candidate[1] and clientid > candidate[0]):
            candidate = (clientid, pnum, pval)

    # set the proposal value
    if candidate is not None:
      self.pval = candidate[2]
    else:
      self.pval = value

    # send accept messages
    msg = '%s,%s,%s' % (self.clientid, self.pnum, self.pval)
    self.threadpool.submit('set', 0, self.block, msg)
    # dbg.dbg('set %s' % msg)

    # read majority blocks
    results = self.threadpool.submit('readBatch', 'majority', self.blockList)

    # check if any value is committed
    for disk in results:
      for block in disk:
        if block.endswith('#'):
          clientid, pnum, pval = block.split(',')
          accepted = pval.rstrip('#')
          return accepted

    # check if accepted by majority
    for disk in results:
      for block in disk:
        clientid, pnum, pval = block.split(',')
        pnum = eval(pnum)
        if pnum > self.pnum or (pnum == self.pnum and clientid > self.clientid):
          return None

    # commit the proposal
    msg = '%s,%s,%s#' % (self.clientid, self.pnum, self.pval)
    self.threadpool.submit('set', 0, self.block, msg)
    # dbg.dbg('set %s' % msg)

    return self.pval

  def join(self):
    self.threadpool.join()

class DiskPaxosWorker(Thread):
  def __init__(self, services, block, blockList):
    Thread.__init__(self)
    self.clientid = str(util.gen_uuid())
    dbg.dbg("Client %s" % self.clientid)
    self.proposer = Proposer(self.clientid, services, block, blockList)
    self.daemon = True
    self.latency = 0
    self.master = False

  def run(self):
    beg = time.time()
    val = self.proposer.propose(self.clientid).strip()
    end = time.time()
    self.latency = max(end - beg, self.latency)
    if val == self.clientid:
        self.master = True
        dbg.dbg("Proposal result: %s" % val)
    # dbg.dbg("%s locked %s: %s" % (self.clientid, self.path, end-beg))
          
  def join(self):
    super(DiskPaxosWorker, self).join()
    self.proposer.join()
    