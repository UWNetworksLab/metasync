#!/usr/bin/env python

import os

from cStringIO import StringIO

from dropbox.rest import ErrorResponse
from dropbox.client import DropboxClient, DropboxOAuth2FlowNoRedirect

import dbg
import util
from base import *
from error import *

import getpass
from selenium import webdriver 
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import tempfile

APP_KEY = 'tfz7q0gh7i2zhdo'
APP_SECRET = 'l7suwn3xvynv7wh'

# NOTE.
#  with 'auth' params, multiple dropbox instances can be used
#
class DropboxAPI(StorageAPI, AppendOnlyLog):
  "dropbox@auth : dropbox.com account with auth info"

  def __init__(self):
    from params import AUTH_DIR
    authdir = AUTH_DIR 
    self.auth_file = os.path.join(authdir, 'dropbox.auth')
    try:
      with open(self.auth_file, 'r') as file:
        ACCESS_TOKEN = file.readline().rstrip()
        USER_ID = file.readline().rstrip()
    except IOError:
      ACCESS_TOKEN, USER_ID = self._authorize()

    self.client = DropboxClient(ACCESS_TOKEN)

  def sid(self):
    return util.md5("dropbox") % 10000

  def copy(self):
    return DropboxAPI()


  def _authorize(self):
    dbg.info('Request access token from Dropbox')
    flow = DropboxOAuth2FlowNoRedirect(APP_KEY, APP_SECRET)
    authorize_url = flow.start()
    # print 'Open auth url:', authorize_url
    #browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'))
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'), service_args=['--ignore-ssl-errors=true', '--ssl-protocol=tlsv1'])
    browser.get(authorize_url)
    try:
      wait = WebDriverWait(browser, 30)
      email = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@name='login_email']")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    email.send_keys(raw_input("Enter your Dropbox email:"))
    pwd = browser.find_element_by_xpath("//input[@name='login_password']") 
    pwd.send_keys(getpass.getpass("Enter your Dropbox password:"))
    pwd.send_keys(Keys.RETURN)
    try:
      wait = WebDriverWait(browser, 30)
      btn = wait.until(EC.element_to_be_clickable((By.NAME, "allow_access")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    btn.click()
    try:
      wait = WebDriverWait(browser, 30)
      auth_code = wait.until(EC.presence_of_element_located((By.ID, "auth-code")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    print(browser.title)
    #auth_code = browser.find_element_by_id("auth-code")
    code = auth_code.text

    browser.quit()

    #code = #raw_input("Enter the authorization code here: ").strip()
    access_token, user_id = flow.finish(code)
    with open(self.auth_file, 'w') as file:
      file.write(access_token + "\n")
      file.write(user_id + "\n")

    dbg.info('Authentication successful')

    return (access_token, user_id)

  # return: list of file paths
  def listdir(self, path):
    dic = self.client.metadata(path)
    lst = map(lambda x:x["path"], dic["contents"])
    lst = map(lambda x:x.split("/")[-1], lst)
    return lst

  def exists(self, path):
    try:
      dic = self.client.metadata(path)
      if(dic.has_key("is_deleted") and dic["is_deleted"]): return False
      return True
    except:
      return False

  def get(self, path):
    """Get the file content

    Args:
      path: string

    Returns:
      content: string
    """

    conn = self.client.get_file(path)
    content = conn.read()
    conn.close()
    return content

  def get_file_rev(self, path, rev):
    # get file of a previous version with rev hash_id
    content = None
    try:
      conn = self.client.get_file(path, rev=rev)
      content = conn.read()
      conn.close()
    except ErrorResponse as detail:
      #print "[get_file_rev] File doesn't exist", detail
      return None
    return content

  def put(self, path, content):
    """Upload the file

    Args:
      path: string
      content: string, size <= 4MB

    Returns: None
    """
    from dropbox.rest import ErrorResponse
    strobj = StringIO(content)

    try:
      metadata = self.client.put_file(path, strobj, overwrite=False, autorename=False)
    except ErrorResponse as e:
      if e.status == 409:
        raise ItemAlreadyExists(e.status, e.reason)
      else:
        raise APIError(e.status, e.reason)
    return True

  def putdir(self, path):
    self.client.file_create_folder(path)

  def update(self, path, content):
    """Update the file
    Args and returns same as put
    """
    strobj = StringIO(content)
    metadata = self.client.put_file(path, strobj, overwrite=True)
    return True

  def rm(self, path):
    """Delete the file

    Args:
      path: string
    """
    self.client.file_delete(path)

  def rmdir(self, path):
    self.client.file_delete(path)

  def metadata(self, path):
    # only for file, not dir
    _md = self.client.metadata(path)
    md = {}
    md['size'] = _md['bytes']
    md['mtime'] = util.convert_time(_md['modified'])
    return md

  def delta(self, path=None, cursor=None):
    resp = self.client.delta(cursor=cursor, path_prefix=path)
    cursor = resp['cursor']
    changes = []

    for entry in resp['entries']:
      event = {}
      if entry[1]:
        # we don't care about delete event
        event['path'] = entry[0]
        if entry[1]['is_dir']:
          event['type'] = 'folder'
        else:
          event['type'] = 'file'
        changes.append(event)

    return cursor, changes

  def poll(self, path=None, cursor=None, timeout=30):
    # timeout max 480
    import requests
    import time

    from error import PollError

    beg_time = time.time()
    end_time = beg_time + timeout
    curr_time = beg_time

    url = 'https://api-notify.dropbox.com/1/longpoll_delta'
    params = {}
    changes = []
    if path:
      path = util.format_path(path)

    if not cursor:
      cursor, _ = self.delta(path)
      curr_time = time.time()

    while True:
      params['cursor'] = cursor
      params['timeout'] = max(30, int(end_time - curr_time)) # minimum 30 second

      resp = requests.request('GET', url, params=params)
      obj = resp.json()
      if 'error' in obj:
        raise PollError(resp.status_code, resp.text)

      if obj['changes']:
        cursor, _delta = self.delta(path, cursor)
        changes.extend(_delta)
      
      if changes:
        break
      curr_time = time.time()
      if curr_time > end_time:
        break

    return cursor, changes

  def init_log(self, path):
    if not self.exists(path):
      self.put(path, '')

  def reset_log(self, path):
    if self.exists(path):
      self.rm(path)

  def append(self, path, msg):
    self.update(path, msg)

  def get_logs(self, path, last_clock):

    length = 5
    # latest revision comes first
    revisions = self.client.revisions(path, rev_limit=length)
    if not revisions:
      return [], None

    new_logs = []
    new_clock = revisions[0]['rev']
    end = False # if reach to end

    while True:
      for metadata in revisions:
        if last_clock and metadata['rev'] == last_clock:
          end = True
          break
      if end: break
      if len(revisions) < length: break
      # still have logs unread, double the length
      length *= 2
      revisions = self.client.revisions(path, rev_limit=length)

    # download the content of unseen rev
    for metadata in revisions:
      if last_clock and metadata['rev'] == last_clock:
        break
      if 'is_deleted' in metadata and metadata['is_deleted']: continue
      msg = self.get_file_rev(path, metadata['rev'])
      if len(msg) > 0:
        new_logs.insert(0, msg)

    return new_logs, new_clock

  def __msg_index(self, fn):
    return eval(fn[3:])

  def init_log2(self, path):
    if not self.exists(path):
      self.putdir(path)

  def append2(self, path, msg):
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

  def get_logs2(self, path, last_clock):
    path = util.format_path(path)
    lst = self.listdir(path)
    if not lst:
      return [], None

    srt = {}
    for fn in lst:
      srt[self.__msg_index(fn)] = fn
    lst = [srt[i] for i in sorted(srt.keys(), reverse=True)]
    new_logs = []
    new_clock = self.__msg_index(lst[0])

    for fn in lst:
      if last_clock == None and self.__msg_index(fn) == last_clock: break
      msg = self.get(path + '/' + fn)
      new_logs.insert(0, msg)

    return new_logs, new_clock

  def share(self, path, target_email):
    url = "https://www.dropbox.com/"
    print 'Get access token from Dropbox'
    print 'Open auth url:', url
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'), service_args=['--ignore-ssl-errors=true', '--ssl-protocol=tlsv1'])
    browser.get(url)
    try:
      wait = WebDriverWait(browser, 30)
      btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[@id='sign-in']/a")))
      btn.click()
      email = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[@id='login_email']")))
      email.send_keys(raw_input("Enter your Dropbox email:"))
      pwd = browser.find_element_by_xpath("//input[@id='login_password']") 
      pwd.send_keys(getpass.getpass("Enter your Dropbox password:"))
      pwd.send_keys(Keys.RETURN)
      target_folder = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[text()='%s']" % path)))
      target_folder.click()
      wait.until(EC.title_contains("%s" % path))
      share_btn = browser.find_element_by_xpath("//a[@id='global_share_button']")
      share_btn.click()
      target = wait.until(EC.element_to_be_clickable((By.XPATH, "//form[@class='invite-more-form']//input[@spellcheck][@type='text']")))
      target.send_keys(target_email)
      confirm_btn = browser.find_element_by_xpath("//form[@class='invite-more-form']//input[@type='button'][1]")
      confirm_btn.click()
    except:
      print(browser.title)
      assert False
      # print(browser.current_url)
      # print(browser.page_source)    
      pass
