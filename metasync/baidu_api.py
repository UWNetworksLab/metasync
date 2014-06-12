#!/usr/bin/env python

import os
import json
import time
import tempfile

import webbrowser
import httplib, urllib, urlparse

from cStringIO import StringIO
import requests

import dbg
import util
from base import *
from error import *

CLIENT_ID = '2ULSIPWwE3eGTZ24vEfcNy3Q'
CLINET_SECRET = 'WmvMUK7O52lnm5aQGiGfE2oYZzEBtEct'

EXCEPTION_MAP = {
  httplib.BAD_REQUEST: BadRequest,
  httplib.UNAUTHORIZED: Unauthorized,
}

from params import AUTH_DIR
AUTH_FILE = os.path.join(AUTH_DIR, 'baidu.auth')

class OAuth2(object):

  AUTH_URL = 'https://openapi.baidu.com/oauth/2.0/authorize'
  TOKEN_URL = 'https://openapi.baidu.com/oauth/2.0/token'

  @staticmethod
  def request_token():
    dbg.info('Request access token from Baidu')
    code = OAuth2._authorize()
    token = OAuth2._token_request('authorization_code', code=code)
    dbg.info('Authentication successful')
    return token
  
  @staticmethod
  def refresh_token(refresh_token):
    dbg.info('Refresh access token from Baidu')
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

    params = {
      'response_type': 'code',
      'client_id': CLIENT_ID,
      'redirect_uri': 'oob',
      'scope': 'netdisk'
    }
    url = OAuth2.AUTH_URL + '?' + urllib.urlencode(params)
    #print 'Open auth url:', url
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'))
    browser.get(url)
    try:
      wait = WebDriverWait(browser, 30)
      username = wait.until(EC.presence_of_element_located((By.NAME, "userName")))
      username.send_keys(raw_input("Enter your baidu userid:"))
      pwd = browser.find_element_by_name("password")
      pwd.send_keys(getpass.getpass("Enter your baidu password:"))
      btn = browser.find_element_by_id("TANGRAM__3__submit")
      btn.click()
      wait = WebDriverWait(browser, 30)
      verify = wait.until(EC.presence_of_element_located((By.ID, "Verifier")))
      code = verify.get_attribute('value')
      if not code:
        raise Exception('User denied authroization')
    except:
      browser.quit()
      import dbg
      dbg.err("error in processing")
      print 'open auth url: ', url
      webbrowser.open(url)
      code = raw_input("Copy the authroization code: ").strip()

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
      'redirect_uri': 'oob'
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

class BaiduAPI(StorageAPI, AppendOnlyLog):
  "baidu@auth : baidu account with auth info"
  BASE_URL = 'https://pcs.baidu.com/rest/2.0/pcs'
  ROOT_DIR = '/apps/metasync'

  def __init__(self, token=None):
    if token:
      self.token = token
    else:
      self.token = Token()

  def info_storage(self):
    return 2048*GB

  def copy(self):
    return BaiduAPI(self.token)

  def sid(self):
    return util.md5("baidu") % 10000

  def _check_error(self, response):
    if not response.ok:
      err = json.loads(response.text)
      if response.status_code == httplib.BAD_REQUEST and err['error_code'] == 31061:
          exception = ItemAlreadyExists
      elif response.status_code == httplib.NOT_FOUND and err['error_code'] == 31066:
          exception = ItemDoesNotExist
      else:
        exception = EXCEPTION_MAP.get(response.status_code, APIError)
      raise exception(response.status_code, response.text)

  def _request(self, method, url, params=None, data=None, headers=None, raw=False, try_refresh=True, **kwargs):

    if params:
      params = dict(params)
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

  def quota(self):
    url = BaiduAPI.BASE_URL + '/quota'
    params = {
      'method': 'info',
    }

    resp = self._request('GET', url, params=params)
    return resp

  def listdir(self, path):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'list',
      'path': path,
    }

    resp = self._request('GET', url, params=params)
    files = [os.path.basename(x['path']) for x in resp['list']]
    return files

  def get(self, path):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'download',
      'path': path
    }

    resp = self._request('GET', url, params=params, raw=True, stream=True)
    return resp.raw.read()

  def putdir(self, path):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'mkdir',
      'path': path
    }

    resp = self._request('POST', url, params=params)

  def put(self, path, content):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'upload',
      'path': path,
    }
    strobj = StringIO(content)

    resp = self._request('POST', url, params=params, files={'file': strobj})
    return True

  def update(self, path, content):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'upload',
      'path': path,
      'ondup': 'overwrite',
    }
    strobj = StringIO(content)

    resp = self._request('POST', url, params=params, files={'file': strobj})
    return True
    
  def exists(self, path):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'meta',
      'path': path,
    }

    try:
      resp = self._request('GET', url, params=params)
      return True
    except ItemDoesNotExist:
      return False

  def rm(self, path):
    path = BaiduAPI.ROOT_DIR + util.format_path(path)
    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'delete',
      'path': path
    }

    self._request('POST', url, params=params)

  def rmdir(self, path):
    self.rm(path)
  

  def __msg_index(self, fn):
    return eval(fn[3:])

  def init_log(self, path):
    if not self.exists(path):
      self.putdir(path)

  def reset_log(self, path):
    if self.exists(path):
      self.rmdir(path)

  def append(self, path, msg):
    path = util.format_path(path)
    lst = sorted(self.listdir(path))
    if lst:
      index = self.__msg_index(lst[-1]) + 1
    else:
      index = 0
    
    while True:
      fn = 'msg%d' % index
      fpath = path + '/' + fn
      try:
        self.put(fpath, msg)
      except ItemAlreadyExists:
        index += 1
      else:
        break

  def get_logs(self, path, last_clock):

    from params import MSG_VALID_TIME

    path = util.format_path(path)

    url = BaiduAPI.BASE_URL + '/file'
    params = {
      'method': 'list',
      'path': BaiduAPI.ROOT_DIR + path,
    }
    resp = self._request('GET', url, params=params)['list']
    if not resp:
      return [], None

    from operator import itemgetter
    lst = [(os.path.basename(x['path']), x['ctime']) for x in resp]
    lst = sorted(lst, key=itemgetter(1))
    lst.reverse()
    
    new_logs = []
    new_clock = self.__msg_index(lst[0][0])
    lastest_ts = lst[0][1]

    for (fn, ts) in lst:
      if last_clock == None and self.__msg_index(fn) == last_clock:
        break
      if lastest_ts - ts > MSG_VALID_TIME:
        break
      log = {
        'time': ts,
        'message': self.get(path + '/' + fn)
      }
      new_logs.insert(0, log)

    return new_logs, new_clock
