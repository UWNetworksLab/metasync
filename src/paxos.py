#!/usr/bin/env python

import time
import random

from threading import Thread
from Queue import Queue

import dbg
import util
from params import MSG_VALID_TIME, LOCK_VALID_TIME

MAX_CLIENTS = 10

# class Debug:
#   log = []
#   lock = Lock()

#   @staticmethod
#   def gettime():
#     return time.strftime("%H:%M:%S", time.localtime())

#   @staticmethod
#   def dbg(line):
#     Debug.lock.acquire()
#     Debug.log.append(Debug.gettime() + " " + line)
#     Debug.lock.release()

#   @staticmethod
#   def dump(filename):
#     with open(filename, 'w') as of:
#       for line in Debug.log:
#         of.write("%s\n" % line)

class Message:

  PREPARE = 0
  ACCEPT = 1
  DONE = 3
    
  def __init__(self, timestamp, msg_type, client_id, pnum=None, pval=None):
    self.ts = timestamp
    self.type = msg_type
    self.pnum = pnum
    self.pval = pval
    self.client_id = client_id

def parse_msg(log):
  ts = log['time']
  msg = log['message']
  if msg.startswith('prepare'):
    data = msg[len('prepare '):].split(',')
    return Message(ts, Message.PREPARE, data[0], eval(data[1]))
  elif msg.startswith('accept'):
    data = msg[len('accept '):].split(',')
    return Message(ts, Message.ACCEPT, data[0], eval(data[1]), data[2])
  elif msg.startswith('done'):
    data = msg.split()[1].split(',')
    return Message(ts, Message.DONE, data[0], data[1])
  else: assert False

class Acceptor(Thread):
  def __init__(self, client_id, storage, path, results):
    Thread.__init__(self)
    
    self.client_id = client_id
    self.storage = storage
    self.path = path
    self.clock = None

    self.promised = 0
    self.promised_id = None
    self.promised_ts = None
    self.accepted = None
    self.accepted_id = None
    self.accepted_ts = None

    self.tasks = Queue(10)
    self.results = results
    self.daemon = True
    self.start()

  def reset(self):
    self.promised = 0
    self.promiesd_id = None
    self.promised_ts = None
    self.accepted = None
    self.accepted_id = None
    self.accepted_ts = None

  def join(self):
    self.tasks.put( (-1, None, None, None, None) )
    super(Acceptor, self).join()

  def _commit_msg(self, msg):
    msg = parse_msg(msg)
    if msg.type == Message.PREPARE:
      if msg.pnum > self.promised or (msg.pnum == self.promised and msg.client_id > self.promised_id):
        self.promised = msg.pnum
        self.promised_id = msg.client_id
        self.promised_ts = msg.ts
    elif msg.type == Message.ACCEPT:
      if msg.pnum > self.promised or (msg.pnum == self.promised and msg.client_id >= self.promised_id): # is it correct?
        self.accepted = (msg.pnum, msg.pval)
        self.accepted_id = msg.client_id
        self.accepted_ts = msg.ts
    elif msg.type == Message.DONE:
        self.reset()
      
  def update(self):
    logs, new_clock = self.storage.get_logs(self.path, self.clock)
    #dbg.dbg('[%s %d] %s: %s' % (self.storage.__class__.__name__, self.cmdId,
    #  new_clock, logs))

    for msg in logs:
      self._commit_msg(msg)
    self.clock = new_clock
    current = util.current_sec()
    if self.promised_ts and current-self.promised_ts > MSG_VALID_TIME:
      self.promised = 0
      self.promiesd_id = None
      self.promised_ts = None
    if self.accepted_ts and current-self.accepted_ts > MSG_VALID_TIME:
      self.accepted = None
      self.accepted_id = None
      self.accepted_ts = None

    #dbg.dbg('[%s %d] prom: %s, acc: %s' % (self.storage.__class__.__name__,
    #  self.cmdId, self.promised, self.accepted))

  def send(self, msg):
    #dbg.dbg('[%s %d] send %s' % (self.storage.__class__.__name__, 
    #  self.cmdId, msg))
    self.storage.append(self.path, msg)

  def run(self):
    while True:
      #dbg.dbg('[%s] wait' % (self.storage.__class__.__name__))
      cmdId, funcname, wait, args, kargs = self.tasks.get()
      #dbg.dbg('[%s] (%d) get' % (self.storage.__class__.__name__, cmdId))
      self.cmdId = cmdId
      if(cmdId == -1):
        #dbg.dbg("cmdId -1, done")
        self.tasks.task_done()
        break
      else:
        try:
          func = getattr(self, funcname)
          #dbg.dbg(funcname)
          func(*args, **kargs)
        except Exception as e:
          print e
        if wait: self.results.put((cmdId, self))
        self.tasks.task_done()

class AcceptorPool(object):
  def __init__(self, client_id, storages, path):
    self.cmdId = 0
    self.num_acceptors = len(storages)
    self.results = Queue(self.num_acceptors*10)
    self.acceptors = []
    for storage in storages:
      acc = Acceptor(client_id, storage, path, self.results)
      storage.init_log(path)
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

  def __init__(self, client_id, storages, path):
    self.id = client_id 
    self.pnum = None
    self.pval = None
    self.acceptorPool = AcceptorPool(client_id, storages, path)
    random.seed(time.time())
    #dbg

  def _init_pnum(self):
    self.pnum = random.randint(0, 30)

  def _debug_time(self, msg):
    cur = time.time()
    dbg.paxos_time("%s: %s" % (msg, cur-self.starttime))


  def propose(self):
    # user should first call check_locked
    self.starttime = time.time()
    exp_backup = 0.5
    import random
    random.seed()
    while True:
      val = self.propose_once()
      if val != None:
        self._debug_time("done")
        return val
      else:
        self._debug_time("another round")
        time.sleep(exp_backup+random.random()) # sleep 1 second, wait for others propose
        exp_backup *= 2

  def done(self):
    self.acceptorPool.submit('send', False, 'done %s,%s' % (self.id, self.pnum)) 
    self.pnum = None
    self.pval = None

  def join(self):
    self.acceptorPool.join()

  def check_locked(self):
    accList = self.acceptorPool.submit('update', True)
    return (self.check_status(accList) != None)

  def check_status(self, accList):
    # return locked clientid or accepted val
    cnt = 0
    accepted_vals = {}
    for acc in accList:
      if acc.accepted is not None:
        val = acc.accepted[1]
        if val in accepted_vals:
          accepted_vals[val] += 1
        else:
          accepted_vals[val] = 1

    for val in accepted_vals:
      if accepted_vals[val] > self.acceptorPool.count()/2:
        dbg.dbg('accepted: %s' % val)
        return val 
    return None

  def propose_once(self):

    if not self.pnum:
      self._init_pnum()

    # do not update in the beginning
    # assume call check_locked first before call propose

    # send prepare messages to all acceptors
    msg = "prepare %s,%s" % (self.id, self.pnum)
    self.acceptorPool.submit('send', False, msg)
    self._debug_time("sent prepare")

    # check if promised from majority acceptors
    accList = self.acceptorPool.submit('update', True)
    self._debug_time("get prepare result")

    ret = self.check_status(accList)
    if ret != None:
      return ret

    promised_cnt = 0
    accepted_propsals = []
    for acc in accList:
      if acc.promised == self.pnum:
        promised_cnt += 1
      if acc.accepted:
        accepted_propsals.append(acc.accepted)

    # if promised, then send accept messages
    if promised_cnt > self.acceptorPool.count()/2:
      # if no proposals have been accepted by any acceptors
      # then choose own id for the proposal value
      # otherwise, pick the value from the proposal with the highest number
      highest_proposal = None
      for proposal in accepted_propsals:
        if (not highest_proposal) or proposal[0] > highest_proposal[0]:
          highest_proposal = proposal
      if not highest_proposal:
        self.pval = self.id
      else:
        self.pval = highest_proposal[1]
      
      # send accept messages
      msg = "accept %s,%s,%s" % (self.id, self.pnum, self.pval)
      self.acceptorPool.submit('send', False, msg)
      self._debug_time("sent accept")

      # check if accepted from majority acceptors
      accepted_cnt = 0
      accList = self.acceptorPool.submit('update', True)
      self._debug_time("accept result")
      ret = self.check_status(accList)
      if ret != None:
        return ret
      
    self.pnum += MAX_CLIENTS
    #Debug.dbg('new proposal num %d' % self.pnum)
    return None
