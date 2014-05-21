# config params

KB = 1024
MB = 1024*KB
GB = 1024*MB

# name of meta root dir
META_DIR = ".metasync"

# batching time for daemon
SYNC_WAIT = 3

# blob size
BLOB_UNIT = 32*MB

# params for paxos and lock
# lock valid time
LOCK_VALID_TIME = 300 #5min

# message valid time
MSG_VALID_TIME  = 60  #1min