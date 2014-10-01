#!/usr/bin/env python

import time
import random

import services
from threading import Thread
from Queue import Queue

import dbg
import util
from error import ItemDoesNotExist
from params import MSG_VALID_TIME, LOCK_VALID_TIME

MAX_CLIENTS = 10

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
    # print '%s append %s: %s' % (services.slug(self.storage), path, content)
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
        content = self.storage.get(path)
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
        # print '%s: %s %s(%s)' % (ind, services.slug(self.storage), funcname, args)
        func = getattr(self, funcname)
        ret = func(*args, **kargs)
      except Exception as e:
        print(e)
      if sync: self.results.put((ind, ret))
      self.tasks.task_done()

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
      worker.tasks.put((-1, None, None, None, None))
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
    random.seed()
    exp_backup = 0.5

    # retrieve the block list and remove my block
    # blocks = self._get_block_list()
    # blocks.remove(self.block)

    # init pval & pnum
    self._init_pnum() 
    self.pval = None

    # write the initial proposal
    self.threadpool.submit('set', 0, self.block, '%s,%s' % (self.pnum, self.pval))

    while True:
      val = self.propose_once(value)
      if val != None:
        self._debug_time("done")
        return val
      else:
        self._debug_time("another round")
        self.pnum += MAX_CLIENTS
        time.sleep(exp_backup+random.random()) # sleep 1 second, wait for others propose
        exp_backup *= 2

  def propose_once(self, value):

    # read majority blocks
    results = self.threadpool.submit('readBatch', 'majority', self.blockList)
    
    # first check if any value is accepted
    for disk in results:
      for block in disk:
        block = block.strip()
        if block.endswith('#'):
          pnum, pval = block.split(',')
          accepted = pval.rstrip('#')
          return accepted

    # find the candidate proposal
    candidate = None
    for disk in results:
      for block in disk:
        block = block.strip()
        if len(block) == 0:
          continue
        pnum, pval = block.split(',')
        pnum = eval(pnum)
        if pnum > self.pnum:
          # abandon this round
          return None
        if candidate is None or pnum > candidate[0]:
          candidate = (pnum, pval)

    # set the proposal value
    if candidate is not None and candidate[1] != 'None':
      self.pval = candidate[1]
    else:
      self.pval = value

    # write the promised proposal
    self.threadpool.submit('set', 0, self.block, '%s,%s' % (self.pnum, self.pval))

    # read majority blocks
    results = self.threadpool.submit('readBatch', 'majority', self.blockList)
    # check if accepted
    candidate = None
    for disk in results:
      for block in disk:
        block = block.strip()
        if len(block) == 0:
          continue
        pnum, pval = block.split(',')
        pnum = eval(pnum)
        if pnum > self.pnum:
          # abandon this round
          return None

    # if reach here, the proposal is accepted
    self.threadpool.submit('set', 0, self.block, '%s,%s#' % (self.pnum, self.pval))

    return self.pval

  def join(self):
    self.threadpool.join()

