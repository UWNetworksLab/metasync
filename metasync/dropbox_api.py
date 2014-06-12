#!/usr/bin/env python

import os

from cStringIO import StringIO

from dropbox.rest import ErrorResponse
from dropbox.client import DropboxClient, DropboxOAuth2FlowNoRedirect

import dbg
import util
from base import *

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
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'))
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
    strobj = StringIO(content)
    #metadata = self.client.put_file(path, strobj)
    metadata = self.client.put_file(path, strobj, overwrite=True)
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

  def init_log(self, path):
    if not self.exists(path):
      self.put(path, '')

  def reset_log(self, path):
    if self.exists(path):
      self.rm(path)

  def append(self, path, msg):
    self.update(path, msg)

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


  def get_logs(self, path, last_clock):

    from params import MSG_VALID_TIME

    length = 5
    revisions = self.client.revisions(path, rev_limit=length)
    if not revisions:
      return [], None

    new_logs = []
    new_clock = revisions[0]['rev']
    latest_ts = util.convert_time(revisions[0]['modified'])
    ends = False
    append_revs = []
    while True:
      for metadata in revisions:
        if last_clock and metadata['rev'] == last_clock:
          ends = True
          break
        ts = util.convert_time(metadata['modified'])
        if latest_ts - ts > MSG_VALID_TIME:
          ends = True
          break
          
        if 'is_deleted' in metadata and metadata['is_deleted']:
          continue
        if metadata['rev'] in append_revs:
          continue
        content = self.get_file_rev(path, metadata['rev'])
        if content:
          log = {
            'time': ts,
            'message': content
          }
          new_logs.insert(0, log)
        append_revs.append(metadata['rev'])

      if len(revisions) < length: ends = True
      if ends: break
      length *= 2
      revisions = self.client.revisions(path, rev_limit=length)

    return new_logs, new_clock

  def share(self, path, target_email):
    url = "https://www.dropbox.com/"
    print 'Get access token from Dropbox'
    print 'Open auth url:', url
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'))
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
