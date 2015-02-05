#!/usr/bin/env python

import os
import sys
import argparse
import tempfile
import util
import shutil
import dbg
import test
import daemon
import glob

from metasyncAPI import MetaSync

def cmd_reconfigure(metasync, args, opts):
    metasync.cmd_reconfigure(args[0], args[1])

def cmd_share(metasync, args, opts):
    "share the repo with someone"

    if(len(args) < 1): 
        dbg.err("Give an email to share your repo.")
        return -1

    target_email = args[0]
    metasync.cmd_share(target_email)

def cmd_init(metasync, args, opts):
    "initialize the repo (e.g., metasync init [namespace])"

    # namespace to avoid conflict
    ns = args[0] if len(args) > 0 else str(util.gen_uuid())

    if not metasync.cmd_init(ns):
        dbg.err("Can't initialize the repository")
        return -1

def cmd_status(metasync, args, opts):
    "show status (e.g., metasync status)"

    # TODO.
    #  - status of each services
    #  - list of files
    #  - status of uptodate or syncing
    if not metasync.cmd_status():
        return -1

def cmd_gc(metasync, args, opts):
    "garbage collect"

    # TODO
    #  - option for depth/expiration date to keep 
    if not metasync.cmd_gc():
        return -1

def cmd_mv(metasync, args, opts):
    "move file (e.g., metasync mv [src] [dst])"

    if len(args) != 2: 
        dbg.err("not enough arguments. e.g., metasync mv [src] [dst]")
        return -1

    if not metasync.cmd_mv(args[0], args[1]):
        return -1



def cmd_clone(metasync, args, opts):
    "clone the repo (e.g., metasync clone [namespace])"
    if(len(args) < 1):
        dbg.err("It requires namespace")
        return -1

    ns = args[0]
    if not metasync.cmd_clone(ns):
        dbg.err("Can't clone the repository")
        return -1

def cmd_checkin(metasync, args, opts):
    "commit a file (e.g., metasync checkin [file])"

    if len(args) == 0:
        dbg.err("Need a file to checkin")
        return -1
    target = []
    for f in args:
        target.extend(glob.glob(f))
    metasync.cmd_checkin(target)

def cmd_fetch(metasync, args, opts):
    "fetch blob stores from the repo (e.g., metasync fetch)"

    metasync.cmd_fetch()

def cmd_diff(metasync, args, opts):
    "find out diff from the current objects"

    metasync.cmd_diff()

def cmd_rm(metasync, args, opts):
    "remove a file (e.g., metasync remove [file])"

    if len(args) == 0:
        dbg.err("Need a file to remove")
        return -1

    for f in args:
        metasync.cmd_rm(f)

def cmd_push(metasync, args, opts):
    "push changes to master"

    if not metasync.cmd_push():
        dbg.err("Can't push")
        return -1

def cmd_update(metasync, args, opts):
    "update fetched changes into local filesystem. (e.g., metasync update)"

    if not metasync.cmd_update():
        dbg.err("Can't update")
        return -1

"""
def cmd_peek(metasync, args, opts):
    "XXX."

    metasync.cmd_peek()
"""

def cmd_daemon(metasync, args, opts):
    "invoke a daemon (and wait) - currently disabled"
    if(opts.debug):
        daemon.start(metasync, args, opts)
    else:
        dbg.err("Currently daemon is supported only for debug mode.")

def cmd_test(metasync, args, opts):
    "quick test (e.g., metasync test {%s})"

    # invoke pdb when failed in testing
    util.install_pdb()

    tmpdir = tempfile.mkdtemp()
    root = os.path.join(tmpdir, "repo")
    util.mkdirs(root)
    metasync = MetaSync(root)

    # opts for sub test routines
    opts.root = root
    opts.tmpdir = tmpdir
    opts.encrypt_key = "testkey" if opts.encrypt else ""

    dbg.info("root: %s" % root)
    dbg.info("args: %s" % args)
    dbg.info("opts: ")
    for (k, v) in vars(opts).iteritems():
        dbg.info("  %-12s = %s" % (k, v))

    alltests = dict(test.get_all_tests())
    if any(case not in alltests for case in args):
        dbg.err("no such a test case: %s" % args)
        alltests["help"](metasync, opts)
        exit(1)

    # print help if no test case is provided
    if len(args) == 0:
        args = ["help"]

    for case in args:
        dbg.info("#R<testing %s#> (%s)" % (case, alltests[case].__doc__))
        alltests[case](metasync, opts)

    # poorman's tree
    def tree(path):
        for root, dirs, files in os.walk(path):
            base = os.path.basename(root)
            idnt = '    ' * (root.replace(path, '').count(os.sep))
            print('%s%s/' % (idnt, base))
            for f in files:
                pn = os.path.join(root, f)
                print('    %s%s [%s]' % (idnt, f, os.stat(pn).st_size))

                # dump some content of blobs
                if opts.dump and "objects" == base:
                    print(util.hexdump(util.read_file(pn, 32*2)))
                    print

    # dump root
    if not opts.notree:
        tree(tmpdir)

    # cleanup tmpdir
    if not opts.keep:
        shutil.rmtree(tmpdir)

# update cmd_test sub args
cmd_test.__doc__ %= "|".join(n for (n,_) in test.get_all_tests())

def get_all_cmds():
    for k, f in globals().items():
        if k.startswith("cmd_"):
            yield (k[4:], f)

def get_cmd(cmd):
    func = "cmd_%s" % cmd
    return globals().get(func, None)

def invoke_cmd(cmd, metasync, args):
    func = get_cmd(cmd)
    return func(metasync, args.args, args)


def main():
    # do all the dirty works on command args
    parser = argparse.ArgumentParser(prog='MetaSync')
    parser.add_argument('--version', action='version', version='%(prog)s 0.2.1')
    parser.add_argument('--quite', action='store_true', default=False)
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--authdir', default=None)
    parser.add_argument('--nthreads', default=2, type=int)
    parser.add_argument('--metasync',
                        help="root of metasync repo",
                        default=".")

    # add subparser for each command
    sub_parser = {}
    sub = parser.add_subparsers(dest="cmd")
    for (k, f) in get_all_cmds():
        p = sub.add_parser(k, help=f.__doc__)
        p.add_argument('args', nargs='*')
        sub_parser[k] = p

    # sub command specific option other than args
    sub_parser["test"].add_argument('--dump',
                                    help="hexdump of /objects",
                                    action="store_true",
                                    default=False)

    sub_parser["test"].add_argument('--keep',
                                    help="keep test dir",
                                    action="store_true",
                                    default=False)

    sub_parser["test"].add_argument('--slow',
                                    help="slow path for testing",
                                    action="store_true",
                                    default=False)

    sub_parser["test"].add_argument('--encrypt',
                                    help="enable encryption",
                                    action="store_true",
                                    default=False)

    sub_parser["test"].add_argument('--notree',
                                    help="do not dump the root tree",
                                    action="store_true",
                                    default=False)

    sub_parser["daemon"].add_argument('--nocheckin',
                                    action="store_true",
                                    default=False)

    # in case of not getting any arg
    if len(sys.argv) < 2:
        sys.argv.append('--help')

    args = parser.parse_args()
    # set quite
    dbg.quiet(["info", "err"])

    if args.debug:
        util.install_pdb()
        dbg.quiet(["info", "err","dbg","time"])
    import services
    services.auth_dir = args.authdir


    # invoke & exit
    exit(invoke_cmd(args.cmd, MetaSync(args.metasync, args), args))
    
if __name__ == '__main__':
    main()
