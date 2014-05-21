
import os
import sys

import util
import services

from google_api import GoogleAPI
from box_api import BoxAPI

def dropbox_listdir(srv, path):
    dirs = []
    files = []
    lst = srv.client.metadata(path)['contents']
    for x in lst:
        if x['is_dir']:
            dirs.append(x['path'].split('/')[-1])
        else:
            files.append((x['path'].split('/')[-1], x['modified']))

    return dirs, files

def google_listdir(srv, path):
    dirs = []
    files = []
    page_token = None

    path = util.format_path(path)
    folder_metadata = srv.search(path)
    if folder_metadata:
        folder_id = folder_metadata['id']
    else:
        folder_id = 'root'

    while True:
        try:
            param = {}
            if page_token:
                param['pageToken'] = page_token

            # result only contains file id, no file name/type
            children = srv.service.children().list(folderId=folder_id, **param).execute()

            for child in children.get('items', []):
                request = srv.service.files().get(fileId=child['id'])
                metadata = request.execute()
                if srv._is_folder(metadata):
                    dirs.append(metadata['title'])
                else:
                    files.append((metadata['title'], metadata['createdDate']))

            page_token = children.get('nextPageToken')

            if not page_token:  
                break
        except errors.HttpError, error:
            print 'An error occurred: %s' % error
            break

    return dirs, files

def box_listdir(srv, path):
    if path == '/':
        folder_id = BoxAPI.ROOT_FOLDER_ID
    else:
        folder = srv.search(path)
        folder_id = folder['id']

    dirs = []
    files = []
    fl = srv._listdir(folder_id)

    for f in fl:
        if f['type'] == 'folder':
            dirs.append(f['name'])
        else:
            url = BoxAPI.BASE_URL + '/files/%s' % f['id']
            md = srv._request('GET', url)
    
            files.append((md['name'], md['created_at']))
    return dirs, files
    
    for fn in l:
        url = BoxAPI.BASE_URL + '/files/%s' % file_id
        resp = srv._request('GET', url)
    
        result.append(metadata['name'])
    return result

def onedrive_listdir(srv, path):
    folder = srv._path_to_metadata(path, True)
    folder_id = folder['id']
    
    dirs = []
    files = []    

    metalist = srv._listdir(folder_id)
    for item in metalist:
        if item['type'] == 'folder':
            dirs.append(item['name'])
        else:
            files.append((item['name'], item['created_time']))
    return dirs, files

def walk(srv, path, listdir):
    toexpand = [path]
    while toexpand:
        path = toexpand.pop(0)
        dirs, files = listdir(srv, path)
        yield path, dirs, files
        toexpand.extend([path+'/'+d for d in dirs])

cls = sys.argv[1].lower()
path = util.format_path(sys.argv[2])

srv = services.factory(cls)
method = globals().get(cls + '_listdir', None)

beg = None
beg_file = None
end = None
end_file = None
for root, dirs, files in walk(srv, path, method):
    #print root, dirs, files
    for f in files:
        #print f
        ts = util.convert_time(f[1])
        if beg == None or ts < beg:
            beg = ts
            beg_file = f
        if end == None or ts > end:
            end = ts
            end_file = f
#print beg, end
print beg_file[0], beg_file[1]
print end_file[0], end_file[1]
print end - beg
