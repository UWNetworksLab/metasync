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

# Increase of Paxos proposal number 
PAXOS_PNUM_INC = 10

# authentication directory
import os
AUTH_DIR = os.path.join(os.path.expanduser("~"), ".metasync")
