#!/usr/bin/env python

import os
import json
import time

import httplib, urllib, urlparse

import requests
from threading import Lock
from cStringIO import StringIO

import dbg
import util
from base import *
from error import *

CLIENT_ID = '7nli0hgyk877dk66mcsunydz98ex0zl2'
CLINET_SECRET = 'G32oUY9TVTkbCRGBCpJSpxO3g0eri4BP'

EXCEPTION_MAP = {
  httplib.UNAUTHORIZED: Unauthorized,
  httplib.BAD_REQUEST: BadRequest,
  httplib.CONFLICT: ItemAlreadyExists,
  httplib.NOT_FOUND: ItemDoesNotExist
}

AUTH_FILE = os.path.join(os.path.dirname(__file__), 'box.auth')

class OAuth2(object):

  OAUTH2_URL = 'https://www.box.com/api/oauth2'
  REDIRECT_URI = 'https://www.box.com/'

  @staticmethod
  def request_token():
    dbg.info('Request access token from Box')
    code = OAuth2._authorize()
    token = OAuth2._token_request('authorization_code', code=code)
    dbg.info('Authentication successful')
    return token

  @staticmethod
  def refresh_token(refresh_token):
    dbg.info('Refresh access token from Box')
    if not refresh_token:
      raise Exception('Refresh token is null')
    token = OAuth2._token_request('refresh_token', refresh_token=refresh_token)
    dbg.info('Refresh successful')
    return token

  @staticmethod
  def _authorize():
    import getpass, time
    from selenium import webdriver 
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    url = OAuth2.OAUTH2_URL + '/authorize?response_type=code&client_id=%s&redirect_uri=%s' % (CLIENT_ID, OAuth2.REDIRECT_URI)
    # print 'Open auth url:', url
    import tempfile
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'))
    browser.get(url)
    try:
      wait = WebDriverWait(browser, 30)
      email = wait.until(EC.element_to_be_clickable((By.ID, "login")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    email.send_keys(raw_input("Enter your Box email:"))
    pwd = browser.find_element_by_id("password")
    pwd.send_keys(getpass.getpass("Enter your Box password:"))
    pwd.send_keys(Keys.RETURN)

    try:
      wait = WebDriverWait(browser, 30)
      btn = wait.until(EC.element_to_be_clickable((By.ID, "consent_accept_button")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    btn.click()

    try:
      wait = WebDriverWait(browser, 30)
      wait.until(EC.title_contains("Box"))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")

    code = browser.current_url.split("=")[-1]
    return code

  @staticmethod
  def _token_request(grant_type, **kwargs):
    """
    Args:
      - grant_type: 'authorization_code', 'refresh_token'
      - code: string
    """

    url = OAuth2.OAUTH2_URL + '/token'

    host = urlparse.urlparse(url).hostname
    args = {
      'grant_type': grant_type,
      'client_id': CLIENT_ID,
      'client_secret': CLINET_SECRET,
      }
    args.update(kwargs)
    params = urllib.urlencode(args)

    conn = httplib.HTTPSConnection(host)
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
    conn.request('POST', url, params, headers)
    resp = conn.getresponse()

    if resp.status != 200:
      raise TokenRequest(resp.status, resp.reason)

    token = json.loads(resp.read())

    return token

class Token(object):

  def __init__(self):
    self._token = None
    self.load_token()

  def load_token(self):
    # first try to load from file
    try:
      file = open(AUTH_FILE, 'r')
      self._token = json.loads(file.read())
      file.close()
    except IOError:
      token = OAuth2.request_token()
      self.set_token(token)

  def set_token(self, token):
    with open(AUTH_FILE, 'w') as of:
      of.write(json.dumps(token))
    self._token = token

  @property
  def headers(self):
    return {'Authorization': 'Bearer %s' % self._token['access_token']}

  def refresh(self):
    if 'refresh_token' in self._token:
      token = OAuth2.refresh_token(self._token['refresh_token'])
    else:
      dbg.info('No refresh token in the access token')
      token = OAuth2.request_token()

    self.set_token(token)


class BoxMetaData:
  instance = None

  @staticmethod
  def getInstance():
    if BoxMetaData.instance is None:
      BoxMetaData.instance = BoxMetaData()
    return BoxMetaData.instance

  def __init__(self):
    self._filemap = {}
    self._foldermap = {}
    self.lock = Lock()

  def _is_folder(self, metadata):
    return (metadata['type'] == 'folder')

  def path_to_metadata(self, path, isfolder=False):
    if path == '/':
      return None
    if isfolder:
      self.lock.acquire() 
      metadata = self._foldermap.get(path)
      self.lock.release()
    else:
      self.lock.acquire() 
      metadata = self._filemap.get(path)
      self.lock.release()
    return metadata

  def cache_metadata(self, path, metadata):
    if self._is_folder(metadata):
      self.lock.acquire() 
      self._foldermap[path] = metadata
      self.lock.release()
    else:
      self.lock.acquire() 
      self._filemap[path] = metadata
      self.lock.release()

  def decache_metadata(self, path, metadata):
    if self._is_folder(metadata):
      self.lock.acquire() 
      del self._foldermap[path]
      self.lock.release()
    else:
      self.lock.acquire() 
      del self._filemap[path]
      self.lock.release()


class BoxAPI(StorageAPI, AppendOnlyLog):
  "box@auth     : box.com account with auth info"

  BASE_URL = 'https://api.box.com/2.0'
  UPLOAD_URL = 'https://upload.box.com/api/2.0'

  ROOT_FOLDER_ID = '0'

  def __init__(self, token=None):
    if token:
      self.token = token
    else:
      self.token = Token()

  def sid(self):
    return util.md5("box") % 10000

  def copy(self):
    return BoxAPI(self.token)

  def info_storage(self):
    return 10*GB

  def _path_to_metadata(self, path, isfolder=False):
    metadata = BoxMetaData.getInstance().path_to_metadata(path, isfolder)
    if not metadata:
      backoff = 0.5
      metadata = self.search(path)
      while metadata is None:
        time.sleep(backoff)
        metadata = self.search(path)
        backoff *= 2
    return metadata

  def _cache_metadata(self, path, metadata):
    BoxMetaData.getInstance().cache_metadata(path, metadata)

  def _decache_metadata(self, path, metadata):
    BoxMetaData.getInstance().decache_metadata(path, metadata)

  def _check_error(self, response):
    if not response.ok:
      exception = EXCEPTION_MAP.get(response.status_code, APIError)
      raise exception(response.status_code, response.text)

  def _request(self, method, url, params=None, data=None, headers=None, raw=False, try_refresh=True, **kwargs):
    if headers:
      headers = dict(headers)
      headers.update(self.token.headers)
    else:
      headers = self.token.headers
    response = requests.request(method, url, params=params, data=data, headers=headers, **kwargs)

    if response.status_code == httplib.UNAUTHORIZED and try_refresh:
      self.token.refresh()
      return self._request(method, url, params, data, headers, raw, try_refresh=False, **kwargs)
      
    self._check_error(response)
    if raw:
      return response
    else:
      return response.json()

  def _listdir(self, folder_id, offset=0):
    url = BoxAPI.BASE_URL + '/folders/%s/items?limit=1000&offset=%d' % (folder_id,offset)
    resp = self._request('GET', url)
    return resp['entries']

  def listdir(self, path):
    """
    Args:
      path: string

    Returns:
      list of file/dir names
    """
    path = util.format_path(path)

    if path == '/':
      folder_id = BoxAPI.ROOT_FOLDER_ID
    else:
      folder = self.search(path)
      folder_id = folder['id']

    result = []
    offset = 0
    while True:
      metalist = self._listdir(folder_id, offset)
    
      for metadata in metalist:
        self._cache_metadata(os.path.join(path, metadata['name']), metadata)
        result.append(metadata['name'])

      if(len(metalist) < 1000): break
      offset += 1000
      
    return result

  def exists(self, path):
    """
    Args:
      path: string

    Returns:
      exist: boolean
    """
    metadata = self.search(path)
    return (metadata != None)

  def get(self, path):
    """
    Args:
      path: string

    Returns
      content: string
    """
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = BoxAPI.BASE_URL + '/files/%s/content' % file_id
    try:
      resp = self._request('GET', url, raw=True, stream=True)
    except:
      return None
    return resp.raw.read()

  def putdir(self, path):
    """
    Args:
      path: string

    Returns:
      None
    """
    path = util.format_path(path)
    name = os.path.basename(path)
    parent_folder = os.path.dirname(path)

    if parent_folder == '/':
      parent_id = BoxAPI.ROOT_FOLDER_ID
    else:
      parent = self._path_to_metadata(parent_folder, isfolder=True)
      if not parent:
        # if the parent folder doesn't exist, then create one
        self.putdir(parent_folder)
        parent = self._path_to_metadata(parent_folder, isfolder=True)
      parent_id = parent['id']

    url = BoxAPI.BASE_URL + '/folders'
    data = '{"name":"%s", "parent":{"id":"%s"}}' % (name, parent_id)
    resp = self._request('POST', url, data=data)

    self._cache_metadata(path, resp)

  def put(self, path, content):
    """
    Args:
      path: string
      content: string

    Returns:
      None
    """
    path = util.format_path(path)
    name = os.path.basename(path)
    parent_folder = os.path.dirname(path)

    if parent_folder == '/':
      parent_id = BoxAPI.ROOT_FOLDER_ID
    else:
      parent = self._path_to_metadata(parent_folder, isfolder=True)
      if not parent:
        # if the parent folder doesn't exist, then create one
        self.putdir(parent_folder)
        parent = self._path_to_metadata(parent_folder, isfolder=True)
      parent_id = parent['id']

    url = BoxAPI.UPLOAD_URL + '/files/content'
    form = {"parent_id": parent_id}
    strobj = StringIO(content)

    try:
      resp = self._request('POST', url, data=form, files={name: strobj})
    except:
      return False

    metadata = resp['entries'][0]
    self._cache_metadata(path, metadata)
    return True 

  def update(self, path, content):
    """
    Args:
      path: string
      content: string

    Returns:
      None
    """
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']
    filename = metadata['name']

    url = BoxAPI.UPLOAD_URL + '/files/%s/content' % file_id
    strobj = StringIO(content)

    resp = self._request('POST', url, files={'file': strobj})

    metadata = resp['entries'][0]
    self._cache_metadata(path, metadata)
    return True 

  def rm(self, path):
    # remove file only
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = BoxAPI.BASE_URL + '/files/%s' % file_id
    self._request('DELETE', url, raw=True)

  def rmdir(self, path):
    # remove directory only
    path = util.format_path(path)
    metadata = self._path_to_metadata(path, isfolder=True)
    dir_id = metadata['id']

    url = BoxAPI.BASE_URL + '/folders/%s?recursive=true' % dir_id
    self._request('DELETE', url, raw=True)

  def metadata(self, path):
    path = util.format_path(path)
    file_id = self._path_to_metadata(path)['id']
    
    url = BoxAPI.BASE_URL + '/files/%s' % file_id
    _md = self._request('GET', url)
    md = {}
    md['size'] = _md['size']
    md['mtime'] = util.convert_time(_md['modified_at'])

    return md

  def share(self, path, target_email):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    if not self._is_folder(metadata):
      return

    url = BoxAPI.BASE_URL + '/collaborations'
    body = {
      "item": {
        "id": metadata["id"],
        "type": "folder"
      },
      "accessible_by": {
        "login": target_email,      
        "type": "user"
      },
      "role": "editor"
    }
    self._request('POST', url, data=json.dumps(body))

  def delta(self, path=None, cursor=None):
    url = BoxAPI.BASE_URL + '/events'
    params = {}
    if cursor is not None:
      params['stream_position'] = cursor
    else:
      params['stream_position'] = 'now'
    params['stream_type'] = 'changes'

    resp = self._request('GET', url, params=params)
    cursor = resp['next_stream_position']
    changes = []

    for entry in resp['entries']:
      item = {}
      source_obj = entry['source']
      path_obj = entry['source']['path_collection']

      if entry['event_type'] in ['ITEM_CREATE', 'ITEM_UPLOAD', 'ITEM_UNDELETE_VIA_TRASH']:
        item['type'] = source_obj['type']
      else:
        # events we are not interested
        # e.g. ITEM_TRASH, COMMENT_CREATE
        continue

      fp = ''
      for i in range(1, path_obj['total_count']):
        fp += '/' + path_obj['entries'][i]['name']
      fp += '/' + source_obj['name']
      item['path'] = fp

      if fp == path or fp.startswith(path + '/'):
        changes.append(item)

    return cursor, changes

  def poll(self, path=None, cursor=None, timeout=30):
    # timeout has NO effect
    path = util.format_path(path)
    url = BoxAPI.BASE_URL + '/events'
    params = {'stream_type': 'changes'}
    resp = self._request('OPTIONS', url, params=params)

    poll_url = resp['entries'][0]['url']
    if cursor is None:
      cursor, _ = self.delta(path, cursor)
    poll_url += '&stream_position=%s' % cursor

    #beg = time.time()
    resp = self._request('GET', poll_url)
    #end = time.time()

    if resp['message'] == 'new_change':
      return self.delta(path=path, cursor=cursor)
    else:
      return cursor, []

  def search(self, path):
    if path == '/':
      # root folder'
      return None

    pathlist = path.strip('/').split('/')

    folder_id = BoxAPI.ROOT_FOLDER_ID
    abspath = ''

    for name in pathlist:
      files = self._listdir(folder_id)
      metadata = None
      for fd in files:
        if fd['name'] == name:
          metadata = fd
          break
      if not metadata:
        # File doesn't exist
        return None
      abspath = os.path.join(abspath, name)
      self._cache_metadata(abspath, metadata)
      folder_id = metadata['id'] # update parent folder id

    return metadata

  def post_comment(self, path, comment):

    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = BoxAPI.BASE_URL + '/comments'
    data = '{"item": {"type": "file", "id": "%s"}, "message": "%s"}' % (file_id, comment)

    resp = self._request('POST', url, data = data)

  def get_comments(self, path):

    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = BoxAPI.BASE_URL + '/files/%s/comments' % file_id
    resp = self._request('GET', url);
    return resp['entries']

  def init_log(self, path):
    if not self.exists(path):
      self.put(path, '')

  def append(self, path, msg):
    self.post_comment(path, msg)

  def get_logs(self, path, last_clock):

    from params import MSG_VALID_TIME

    comments = self.get_comments(path)
    if not comments:
      return [], None
    comments.reverse()

    new_logs = []
    new_clock = comments[0]['id']
    latest_ts = util.convert_time(comments[0]['created_at'])
    
    for comment in comments:
      if last_clock and comment['id'] == last_clock:
        break
      ts = util.convert_time(comment['created_at'])
      if latest_ts - ts > MSG_VALID_TIME:
        break
      log = {
        'time': ts,
        'message': comment['message']
      }
      new_logs.insert(0, log)

    return new_logs, new_clock
