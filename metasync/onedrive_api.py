import os
import json
import time
import dbg

import webbrowser
import httplib, urllib, urlparse

import requests
from threading import Lock
from cStringIO import StringIO

import dbg
import util
from error import *
from base import *

CLIENT_ID = '000000004411503A'
CLINET_SECRET = 'CJxXEWQfC07ml95277GnoDrr8M3Ksbc0'

EXCEPTION_MAP = {
  httplib.UNAUTHORIZED: Unauthorized,
  httplib.BAD_REQUEST: BadRequest,
  httplib.CONFLICT: ItemAlreadyExists,
  httplib.NOT_FOUND: ItemDoesNotExist
}

AUTH_FILE = os.path.join(os.path.dirname(__file__), 'onedrive.auth')

class OAuth2(object):

  AUTH_RUL = 'https://login.live.com/oauth20_authorize.srf'
  TOKEN_URL = 'https://login.live.com/oauth20_token.srf'
  REDIRECT_URI = 'https://login.live.com/oauth20_desktop.srf'

  @staticmethod
  def request_token():
    dbg.info('Request access token from OneDrive')
    code = OAuth2._authorize()
    token = OAuth2._token_request('authorization_code', code=code)
    dbg.info('Authentication successful')
    return token
  
  @staticmethod
  def refresh_token(refresh_token):
    dbg.info('Refresh access token from OneDrive')
    if not refresh_token:
      raise Exception('Refresh token is null')
    token = OAuth2._token_request('refresh_token', refresh_token=refresh_token)
    dbg.info('Refresh successful')
    return token

  @staticmethod
  def _authorize():
    import getpass
    from selenium import webdriver 
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    params = {
      'response_type': 'code',
      'client_id': CLIENT_ID,
      'redirect_uri': OAuth2.REDIRECT_URI,
      'scope': 'wl.skydrive_update wl.offline_access'
    }
    authorize_url = OAuth2.AUTH_RUL + '?' + urllib.urlencode(params)
    # print 'Open auth url:', authorize_url
    import tempfile
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'))
    browser.get(authorize_url)
    try:
      wait = WebDriverWait(browser, 30)
      email = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@name='login']")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    email.send_keys(raw_input("Enter your OneDrive account:"))
    pwd = browser.find_element_by_xpath("//input[@name='passwd']") 
    pwd.send_keys(getpass.getpass("Enter your password:"))
    pwd.send_keys(Keys.RETURN)

    try:
      wait = WebDriverWait(browser, 30)
      btn = wait.until(EC.element_to_be_clickable((By.ID, "idBtn_Accept")))
    except:
      print(browser.title)
      browser.quit()
      raise Exception("timeout for authorization")
    btn.click()

    try:
      wait = WebDriverWait(browser, 30)
      wait.until(EC.title_is(""))
    except:
      print(browser.title)
      print(browser.current_url)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")

    url = browser.current_url
    #url = raw_input("Copy the url here: ").strip()
    resp = urlparse.urlparse(url)

    code = None
    args = resp.query.split('&')
    for arg in args:
      data = arg.split('=')
      if data[0] == 'code':
        code = data[1]

    if not code:
      raise Exception('User denied authroization')

    return code

  @staticmethod
  def _token_request(grant_type, **kwargs):
    """
    Args:
      - grant_type: 'authorization_code', 'refresh_token'
      - code: string
    """

    url = OAuth2.TOKEN_URL

    host = urlparse.urlparse(url).hostname
    args = {
      'grant_type': grant_type,
      'client_id': CLIENT_ID,
      'client_secret': CLINET_SECRET,
      'redirect_uri': OAuth2.REDIRECT_URI
      }
    args.update(kwargs)
    params = urllib.urlencode(args)

    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    conn = httplib.HTTPSConnection(host)
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
  def access_token(self):
    return self._token['access_token']

  def refresh(self):
    if 'refresh_token' in self._token:
      token = OAuth2.refresh_token(self._token['refresh_token'])
    else:
      dbg.info('No refresh token in the access token')
      token = OAuth2.request_token()

    self.set_token(token)

class OneDriveMetaData:
  instance = None

  @staticmethod
  def getInstance():
    if OneDriveMetaData.instance is None:
      OneDriveMetaData.instance = OneDriveMetaData()
    return OneDriveMetaData.instance

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


class OneDriveAPI(StorageAPI, AppendOnlyLog):
  "onedrive@auth : onedrive account with auth info"
  BASE_URL = 'https://apis.live.net/v5.0'

  def __init__(self, token=None):
    if token:
      self.token = token
    else:
      self.token = Token()
    OneDriveMetaData.getInstance()

  def sid(self):
    return util.md5("onedrive") % 10000

  def copy(self):
    return OneDriveAPI(self.token)

  def info_storage(self):
    return 7*GB

  def _cache_metadata(self, path, metadata):
    OneDriveMetaData.getInstance().cache_metadata(path, metadata)

  def _decache_metadata(self, path, metadata):
    OneDriveMetaData.getInstance().decache_metadata(path, metadata)

  def _path_to_metadata(self, path, isfolder=False):
    metadata = OneDriveMetaData.getInstance().path_to_metadata(path, isfolder)
    if not metadata:
      backoff = 0.5
      while True:
        try:
          metadata = self.search(path)
          break
        except:
          dbg.dbg("onedrive, search backoff")
          time.sleep(backoff)
          backoff*=2
    return metadata

  def _check_error(self, resp):
    if not resp.ok:
      exception = EXCEPTION_MAP.get(resp.status_code, APIError)
      text = resp.text.replace('\r', '\n')
      raise exception(resp.status_code, text)

  def _request(self, method, url, params=None, data=None, headers=None, raw=False, try_refresh=True, **kwargs):

    if not headers:
      if params:
        params['access_token'] = self.token.access_token
      else:
        params = {'access_token': self.token.access_token}

    response = requests.request(method, url, params=params, data=data, headers=headers, **kwargs)

    if response.status_code == httplib.UNAUTHORIZED and try_refresh:
      self.token.refresh()
      return self._request(method, url, params, data, headers, raw, try_refresh=False, **kwargs)

    self._check_error(response)
    if raw:
      return response
    else:
      return response.json()

  def _listdir(self, folder_id):
    url = OneDriveAPI.BASE_URL + '/%s/files' % folder_id
    resp = self._request('GET', url)
    return resp['data']

  def listdir(self, path):
    """
    Args:
      path: string

    Returns:
      list of file names
    """
    path = util.format_path(path)
    folder = self._path_to_metadata(path, True)
    folder_id = folder['id']

    metalist = self._listdir(folder_id)
    result = []
    for metadata in metalist:
      self._cache_metadata(path + '/' + metadata['name'], metadata)
      result.append(metadata['name'])
    return result

  def exists(self, path):
    """
    Args:
      path: string

    Returns:
      exist: boolean
    """
    path = util.format_path(path)
    metadata = self.search(path)
    return (metadata != None)

  def get(self, path):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = OneDriveAPI.BASE_URL + '/%s/content' % file_id
    resp = self._request('GET', url, raw=True, stream=True)

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

    parent = self._path_to_metadata(parent_folder, isfolder=True)
    if not parent:
      # if the parent folder doesn't exist, then create one
      self.putdir(parent_folder)
      parent = self._path_to_metadata(parent_folder, isfolder=True)

    parent_id = parent['id']
    url = OneDriveAPI.BASE_URL + '/%s' % parent_id
    headers = {
      "Authorization": "Bearer " + self.token.access_token,
      "Content-Type": "application/json"
    }
    data = '{"name": "%s"}' % name
    resp = self._request('POST', url, headers=headers, data=data)
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

    parent = self._path_to_metadata(parent_folder, isfolder=True)
    if not parent:
      # if the parent folder doesn't exist, then create one
      self.putdir(parent_folder)
      parent = self._path_to_metadata(parent_folder, isfolder=True)

    parent_id = parent['id']
    url = OneDriveAPI.BASE_URL + '/%s/files/%s' % (parent_id, name)
    strobj = StringIO(content)
    try:
      metadata = self._request('PUT', url, data=strobj)
    except:
      return False
    metadata[u'type'] = u'file'
    self._cache_metadata(path, metadata)
    return True

  def update(self, path, content):
    return self.put(path, content)

  def rm(self, path):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = OneDriveAPI.BASE_URL + '/%s' % file_id
    self._request('DELETE', url, raw=True)

  def rmdir(self, path):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path, isfolder=True)
    file_id = metadata['id']

    url = OneDriveAPI.BASE_URL + '/%s' % file_id
    self._request('DELETE', url, raw=True)

  def metadata(self, path):
    path = util.format_path(path)
    _md = self.search(path)
    md = {}
    md['size'] = _md['size']
    md['mtime'] = util.convert_time(_md['updated_time'])
    return md

  def search(self, path):

    metacache = OneDriveMetaData.getInstance()
    if not '/' in metacache._foldermap:
      url = OneDriveAPI.BASE_URL + '/me/skydrive'
      resp = self._request('GET', url)
      metacache._foldermap['/'] = resp

    if path == '/':
      return metacache._foldermap['/']

    pathlist = path.strip('/').split('/')

    folder_id = metacache._foldermap['/']['id']
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
      abspath = abspath + '/' + name
      self._cache_metadata(abspath, metadata)
      folder_id = metadata['id'] # update parent folder id

    return metadata

  # IMPORTANT: only shared file can be commentted
  def post_comment(self, path, comment):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    url = OneDriveAPI.BASE_URL + '/%s/comments' % file_id
    headers = {
      "Authorization": "Bearer " + self.token.access_token,
      "Content-Type": "application/json"
    }
    data = '{"message": "%s"}' % comment
    resp = self._request('POST', url, headers=headers, data=data)

  def get_comments(self, path, length=5, offset=0):
    beg = time.time()
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    params = {
      'limit': length,
      'offset': offset
    }
    url = OneDriveAPI.BASE_URL + '/%s/comments' % (file_id)
    resp = self._request('GET', url, params)
    end = time.time()
    dbg.paxos_time("get_comments %s", end-beg)
    return resp['data']

  def init_log(self, path):
    path = '/Public' + util.format_path(path)
    if not self.exists(path):
      self.put(path, '')

  def reset_log(self, path):
    path = '/Public' + util.format_path(path)
    if self.exists(path):
      self.rm(path)

  def append(self, path, msg):
    beg = time.time()
    path = '/Public' + util.format_path(path)
    self.post_comment(path, msg)
    end = time.time()
    dbg.paxos_time("append %s", end-beg)


  def get_logs(self, path, last_clock):

    from params import MSG_VALID_TIME
    beg = time.time()

    path = '/Public' + util.format_path(path)
    length = 5
    offset = 0

    comments = self.get_comments(path, length, offset)
    if not comments:
      return [], None
    
    new_logs = []
    new_clock = comments[0]['id']
    latest_ts = util.convert_time(comments[0]['created_time'])
    ends = False
    while True:
      for comment in comments:
        if last_clock and comment['id'] == last_clock:
          ends = True
          break
        ts = util.convert_time(comment['created_time'])
        if latest_ts - ts > MSG_VALID_TIME:
          ends = True
          break
        
        log = {
          'time': ts,
          'message': comment['message']
        }
        new_logs.insert(0, log)
      if len(comments) < length: ends = True
      if ends: break
      offset += length
      comments = self.get_comments(path, length, offset)

    end = time.time()
    dbg.paxos_time("get_log %s", end-beg)
    return new_logs, new_clock
