#!/usr/bin/env python

import time
import random
import traceback

from threading import Thread
from Queue import Queue
from params import PAXOS_PNUM_INC

import dbg
import util
import services

class Acceptor(Thread):
  def __init__(self, clientid, storage, path, results):
    Thread.__init__(self)
    
    self.clientid = clientid
    self.storage = storage
    self.path = path
    self.clock = None

    self.promise = None
    self.accept  = None
    self.commit  = None

    self.tasks = Queue(10)
    self.results = results
    self.daemon = True
    self.stop = False
    self.start()

  def join(self):
    self.stop = False
    self.tasks.put( (-1, None, None, None, None) )
    super(Acceptor, self).join()

  def _commit_msg(self, msg):
    if msg.endswith('#'):
      # commit message
      self.commit = msg.rstrip('#')
    else:
      clientid, pnum, pval = msg.split(',')
      pnum = eval(pnum)
      if pval == 'None':
        # prepare message
        if self.promise == None:
          self.promise = (clientid, pnum)
        elif pnum > self.promise[1] or (pnum == self.promise[1] and clientid > self.promise[0]):
          # if both proposal have the same pnum, client with bigger id wins
          self.promise = (clientid, pnum)
      else:
        # accept message
        if pnum > self.promise[1] or (pnum == self.promise[1] and clientid >= self.promise[0]):
          self.accept = (clientid, pnum, pval)
      
  def update(self):
    logs, new_clock = self.storage.get_logs(self.path, self.clock)
    for msg in logs:
      self._commit_msg(msg)
    self.clock = new_clock

  def send(self, msg):
    self.storage.append(self.path, msg)

  def run(self):
    while True:
      #dbg.dbg('[%s] wait' % (self.storage.__class__.__name__))
      cmdId, funcname, wait, args, kargs = self.tasks.get()
      dbg.dbg('[%s] %s cmd: %s' % (self.clientid, services.slug(self.storage), cmdId))
      self.cmdId = cmdId
      if(cmdId == -1):
        #dbg.dbg("cmdId -1, done")
        self.tasks.task_done()
        break
      try:
        func = getattr(self, funcname)
        func(*args, **kargs)
      except Exception as e:
        traceback.print_exc()
      if wait: self.results.put((cmdId, self))
      self.tasks.task_done()

      if self.stop:
        break
    dbg.dbg("[%s] %s stops" % (self.clientid, services.slug(self.storage)))

class AcceptorPool(object):
  def __init__(self, clientid, storages, path):
    self.cmdId = 0
    self.clientid = clientid
    self.num_acceptors = len(storages)
    self.results = Queue(self.num_acceptors*10)
    self.acceptors = []
    for storage in storages:
      acc = Acceptor(clientid, storage, path, self.results)
      self.acceptors.append(acc)

  def submit(self, funcname, wait, *args, **kargs):
    #dbg.paxos_time("%s %s" % (funcname, self.cmdId))
    for acc in self.acceptors:
      acc.tasks.put((self.cmdId, funcname, wait, args, kargs))
    ret = None
    if wait:
      waitcount = self.num_acceptors / 2 + 1
      count = 0
      ret = []
      while count < waitcount:
        beg = time.time()
        index, acc = self.results.get()
        end = time.time()
        dbg.paxos_time("submit %s %s %s" % (index, funcname, end-beg))
        beg = time.time()
        if index == self.cmdId:
          count += 1
          ret.append(acc)
        self.results.task_done()
        end = time.time()

    self.cmdId += 1
    return ret

  def count(self):
    return self.num_acceptors

  def join(self):
    for acceptor in self.acceptors:
      acceptor.join()

class Proposer(object):

  def __init__(self, clientid, storages, path):
    self.clientid = clientid 
    self.pnum = None
    self.pval = None
    self.acceptorPool = AcceptorPool(clientid, storages, path)

  def _init_pnum(self):
    self.pnum = random.randint(0, 30)

  def _debug_time(self, msg):
    cur = time.time()
    # dbg.paxos_time("[%s] %s: %s" % (self.clientid, msg, cur-self.starttime))
    dbg.dbg("[%s] %s: %s" % (self.clientid, msg, cur-self.starttime))

  def propose(self, value):
    # user should first call check_locked
    self.starttime = time.time()
    random.seed(time.time())
    exp_backup = 0.5

    # init pval & pnum
    self.pnum = None
    self.pval = None

    while True:
      val = self.propose_once(value)
      if val != None:
        self._debug_time("done: %s" % val)
        return val
      else:
        self._debug_time("another round")
        self.pnum += PAXOS_PNUM_INC
        time.sleep(exp_backup+random.random()) # sleep 1 second, wait for others propose
        exp_backup *= 2

  def check_locked(self):
    accList = self.acceptorPool.submit('update', True)
    return (self.check_status(accList) != None)

  def propose_once(self, value):

    if not self.pnum:
      self._init_pnum()
    self.pval = None

    # send prepare messages
    msg = "%s,%s,%s" % (self.clientid, self.pnum, self.pval)
    self.acceptorPool.submit('send', False, msg)
    self._debug_time("sent prepare: %s" % msg)

    # update acceptors and wait for majority
    accList = self.acceptorPool.submit('update', True)
    self._debug_time("get prepare result")

    # check if any value is committed
    for acc in accList:
      if acc.commit is not None:
        return acc.commit

    # check if promised by majority and find the candidate proposal
    candidate = None
    for acc in accList:
      if acc.promise is not None:
        if acc.promise[1] > self.pnum or (acc.promise[1] == self.pnum and acc.promise[0] > self.clientid):
          return None
      if acc.accept is not None:
        if candidate is None or acc.accept[1] > candidate[1] or (acc.accept[1] == candidate[1] and acc.accept[0] > candidate[0]):
          candidate = acc.accept

    # set the proposal value
    if candidate is not None:
      self.pval = candidate[2]
    else:
      self.pval = value

     # send accept messages
    msg = "%s,%s,%s" % (self.clientid, self.pnum, self.pval)
    self.acceptorPool.submit('send', False, msg)
    self._debug_time("sent accept: %s" % msg)

    # update acceptors and wait for majority
    accList = self.acceptorPool.submit('update', True)
    self._debug_time("accept result")

    # check if any value is committed
    for acc in accList:
      if acc.commit is not None:
        return acc.commit

    # check if accepted by majority
    for acc in accList:
      if acc.accept is None or acc.accept[2] != self.pval:
        return None

    # commit the proposal
    msg = "%s#" % self.pval
    self.acceptorPool.submit('send', False, msg)
    self._debug_time("sent commit: %s" % msg)

    return self.pval

  def join(self):
    self.acceptorPool.join()

class PPaxosWorker(Thread):
  def __init__(self, services, path):
    Thread.__init__(self)
    random.seed(time.time())
    self.clientid = str(util.gen_uuid())
    self.proposer = Proposer(self.clientid, services, path)
    self.latency = 0
    self.master = False
    dbg.dbg("Client %s" % self.clientid)

  def run(self):
    beg = time.time()
    val = self.proposer.propose(self.clientid).strip()
    end = time.time()
    self.latency = max(end - beg, self.latency)
    if val == self.clientid:
        self.master = True
        dbg.dbg("Proposal result: %s (%s)" % (val, self.latency))
          
  def join(self):
    super(PPaxosWorker, self).join()
    self.proposer.join()