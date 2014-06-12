import types
import dbg
import os

from dropbox_api  import DropboxAPI
from google_api   import GoogleAPI
from box_api      import BoxAPI
from disk_api     import DiskAPI
from baidu_api    import BaiduAPI
from onedrive_api import OneDriveAPI

all_services = [DropboxAPI, GoogleAPI, BoxAPI, BaiduAPI, OneDriveAPI, DiskAPI]

# factory for service api
def factory(srv):
    srv = srv.strip()
    if srv.startswith("disk@"):
        root = srv.split("@")[1]
        return DiskAPI(root)
    elif srv == "google":
        return GoogleAPI()
    elif srv == "box":
        return BoxAPI()
    elif srv == "dropbox":
        return DropboxAPI()
    elif srv == "baidu":
        return BaiduAPI()
    elif srv == "onedrive":
        return OneDriveAPI()
    dbg.err("No such a provider: %s" % srv)
    raise NameError(srv) 

# convert class name to cute slug
def slug(cls):
    # dance around python's type system
    if type(cls) is types.InstanceType:
        cls = cls.__class__
    # so, extract slug out of classname
    return cls.__name__[:-len("API")].lower()

# return backend info, provided by each class
def backends():
    for srv in all_services:
        yield (slug(srv), srv.__doc__)
