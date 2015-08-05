#!/usr/bin/env python

import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'google')))

import httplib2
from apiclient import discovery, errors
from apiclient.http import MediaFileUpload
from apiclient.http import MediaInMemoryUpload
from oauth2client import file, client, tools
from threading import Lock

import dbg
import util
import time
from base import *

# App token file containg the OAuth 2.0 information
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'google_client_secrets.json')

# Authentication file
from params import AUTH_DIR
AUTH_FILE = os.path.join(AUTH_DIR, 'google.auth')

# Specify the authentication scope
# auth/drive has the full access to user's files stored in drive
FLOW = client.flow_from_clientsecrets(CLIENT_SECRETS,
  scope = ['https://www.googleapis.com/auth/drive'],
  message = tools.message_if_missing(CLIENT_SECRETS))

# Flag object specify the authentication url
# Details about Flag refers to oauth2client.tools run_flow()
class Flag:

  def __init__(
    self,
    auth_host_name='localhost',
    auth_host_port=[8080, 8090],
    noauth_local_webserver=False,
    logging_level='ERROR'):
    self.auth_host_name = auth_host_name
    self.auth_host_port = auth_host_port
    self.noauth_local_webserver = noauth_local_webserver
    self.logging_level = logging_level

class GoogleMetaData:
  instance = None

  @staticmethod
  def getInstance():
    if GoogleMetaData.instance is None:
      GoogleMetaData.instance = GoogleMetaData()
    return GoogleMetaData.instance

  def __init__(self):
    self._filemap = {}
    self._foldermap = {}
    self.lock = Lock()

  def _is_folder(self, metadata):
    return (metadata['mimeType'].find('folder') >= 0)

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


class GoogleAPI(StorageAPI, AppendOnlyLog):
  "google@auth  : drive.google.com account with auth info"

  UPLOAD_URL = 'https://www.googleapis.com/upload/drive/v2'

  def __init__(self):
    self.service = self._get_service()
    self._num_retries = 0 # retry times in uploading

  def copy(self):
    return GoogleAPI()

  def info_storage(self):
    return 15*GB

  def sid(self):
    return util.md5("google") % 10000

  def _authorize(self, flow, storage, flags):
    # copied and edited from tools.run_flow()
    import getpass, time
    from selenium import webdriver 
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    dbg.info('Request access token from Google Drive')

    oauth_callback = client.OOB_CALLBACK_URN
    flow.redirect_uri = oauth_callback
    authorize_url = flow.step1_get_authorize_url()
    #print 'Open auth url:', authorize_url
    import tempfile
    browser = webdriver.PhantomJS(service_log_path=os.path.join(tempfile.gettempdir(), 'ghostdriver.log'), service_args=['--ignore-ssl-errors=true', '--ssl-protocol=tlsv1'])
    browser.get(authorize_url)
    try:
      wait = WebDriverWait(browser, 30)
      email = wait.until(EC.element_to_be_clickable((By.ID, "Email")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    email.send_keys(raw_input("Enter your Google Drive email:"))
    btn = browser.find_element_by_id("next")
    btn.click()
    try:
      wait = WebDriverWait(browser, 30)
      email = wait.until(EC.element_to_be_clickable((By.ID, "Passwd")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")

    pwd = browser.find_element_by_id("Passwd")
    pwd.send_keys(getpass.getpass("Enter your Google Drive password:"))
    btn = browser.find_element_by_id("signIn")
    btn.click()
    try:
      wait = WebDriverWait(browser, 30)
      btn = wait.until(EC.element_to_be_clickable((By.ID, "submit_approve_access")))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    btn.click()
    try:
      wait = WebDriverWait(browser, 30)
      wait.until(EC.title_contains("Success code"))
    except:
      print(browser.title)
      print(browser.page_source)
      browser.quit()
      raise Exception("timeout for authorization")
    code = browser.title.split("=")[1]
    browser.quit()

    try:
      credential = flow.step2_exchange(code)
    except client.FlowExchangeError, e:
      sys.exit('Authentication has failed: %s' % e)

    storage.put(credential)
    credential.set_store(storage)

    dbg.info('Authentication successful')

    return credential

  def _get_service(self):
    "Return Google Drive service"

    flags = Flag()
    storage = file.Storage(AUTH_FILE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
      # if credential doesn't exist or exprires
      # need to require the authentication
      credentials = self._authorize(FLOW, storage, flags)

    http = httplib2.Http()
    http = credentials.authorize(http)

    return discovery.build('drive', 'v2', http=http)

  def _refresh_auth(self):
    self.service = self._get_service()

  def _path_to_metadata(self, path, isfolder=False):
    metadata = GoogleMetaData.getInstance().path_to_metadata(path, isfolder)
    if not metadata:
      backoff = 0.5
      while True:
        try:
          metadata = self.search(path)
          break
        except:
          time.sleep(backoff)
          backoff*=2
    return metadata

  def _cache_metadata(self, path, metadata):
    GoogleMetaData.getInstance().cache_metadata(path, metadata)

  def _decache_metadata(self, path, metadata):
    GoogleMetaData.getInstance().decache_metadata(path, metadata)

  #deprecated
  def listdir_old(self, path):
    result = []
    page_token = None

    path = util.format_path(path)
    folder_metadata = self.search(path)
    if folder_metadata:
      folder_id = folder_metadata['id']
    else:
      folder_id = 'root'

    while True:
      try:
        param = {}
        if page_token:
          param['pageToken'] = page_token

        children = self.service.children().list(folderId=folder_id, **param).execute()

        for child in children.get('items', []):
          request = self.service.files().get(fileId=child['id'])
          metadata = request.execute()
          self._cache_metadata(os.path.join(path, metadata['title']), metadata)
          result.append(metadata['title'])

        page_token = children.get('nextPageToken')

        if not page_token:
          break
      except errors.HttpError, error:
        print 'An error occurred: %s' % error
        break

    return result

  def listdir(self, path):
    result = []
    path = util.format_path(path)
    if path == '/':
      folder_id = 'root'
    else:
      folder_id = self._path_to_metadata(path)['id']

    param = {}
    param['q'] = '"%s" in parents and trashed=false' % (folder_id)
    param['maxResults'] = 500
    page_token = None

    while True:
      try:
        if page_token:
          param['pageToken'] = page_token
        files = self.service.files().list(**param).execute()
        result.extend(map(lambda x: x['title'], files['items']))

        page_token = files.get('nextPageToken', None)
        if not page_token:
          break
      except errors.HttpError, error:
        print 'An error occurred: %s' % error
        return None

    return result

  def exists(self, path):
    metadata = self._path_to_metadata(path)
    return (metadata != None)

  def get(self, path):
    """Get the content of the file
    Google Drive use file_id, not the path, to access the file

    Args:
      path: string

    Returns:
      content: string
      None if any error happens
    """
    beg = time.time()
    try:
      path = util.format_path(path)
      #print(path)
      metadata = self._path_to_metadata(path)
      #print(metadata)
      file_id = metadata['id']

      request = self.service.files().get(fileId=file_id)
      drive_file = request.execute()

      download_url = drive_file.get('downloadUrl')
      if download_url:
        resp, content = self.service._http.request(download_url)
        if resp.status == 200:
          end = time.time()
          dbg.dbg_time("get file %s %s" % (path, end-beg))
          return content
        else:
          print 'An erro occurred: %s' % resp
          return None
      else:
        print "The file doesn't have any content stored on Drive."
        return None
    except errors.HttpError, error:
      print 'An error occurred: %s' % error
      return None

  def putdir(self, path):

    uri = GoogleAPI.UPLOAD_URL + '/files?uploadType=multipart&alt=json'

    path = util.format_path(path)
    name = os.path.basename(path)
    folder = os.path.dirname(path)

    parent_id = None
    if folder != '/':
      # if it's not at the root folder, find out the parent id
      parent = self._path_to_metadata(folder, isfolder=True)
      if not parent:
        # if the parent folder doesn't exist, then create one
        self.putdir(folder)
        parent = self._path_to_metadata(folder, isfolder=True)
      parent_id = parent['id']

    body = {
      'title': name,
      'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
      # if not at root folder
      body['parents'] = [{'id': parent_id}]

    try:
      drive_file = self.service.files().insert(body=body).execute()
      self._cache_metadata(path, drive_file)
      return True
    except errors.HttpError, error:
      print 'An error occurred: %s' % error
      return False

  def put(self, path, content):
    """Upload the file

    Args:
      path: string
      content: string, size <= 5MB

    Returns:
      success: boolean

    Raises:
      apiclient.errors.HttpError if the response was not a 2xx.
      httplib2.HttpLib2Error if a transport error has occured.
    """
    uri = GoogleAPI.UPLOAD_URL + '/files?uploadType=multipart&alt=json'
    path = util.format_path(path)
    name = os.path.basename(path)
    folder = os.path.dirname(path)

    parent_id = None
    beg = time.time()
    if folder != '/':
      parent = self._path_to_metadata(folder, isfolder=True)
      if not parent:
        # if the parent folder doesn't exist, then create one
        self.putdir(folder)
        parent = self._path_to_metadata(folder, isfolder=True)
      parent_id = parent['id']
    end = time.time()
    dbg.google_time("get parent: %s" % (end-beg))
    beg = time.time()
    media_body = MediaInMemoryUpload(content)
    body = {
      'title': name,
      'mimeType': 'application/octet-stream'
    }
    if parent_id:
      # if not at root folder
      body['parents'] = [{'id': parent_id}]

    try:
      drive_file = self.service.files().insert(body=body, media_body=media_body).execute()
      self._cache_metadata(path, drive_file)
      end = time.time()
      dbg.google_time("insert finish %s" % (end-beg))
      return True
    except errors.HttpError, error:
      print 'An error occurred: %s' % error
      return False

  def update(self, path, content):
    """Update the file
    Args and returns same as put
    """
    dbg.dbg(path)
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    file_id = metadata['id']

    uri = GoogleAPI.UPLOAD_URL + '/files/%s?uploadType=media' % file_id

    headers = {
      'Content-Type': 'text/plain',
      'Content-Length': len(content),
	 	}

    for retry_num in xrange(self._num_retries + 1):
      resp, data = self.service._http.request(uri, method='PUT',
        body=content, headers=headers)
      if resp.status < 500:
        break

    if resp.status >= 300:
      raise errors.HttpError(resp, data, uri=uri)
    if resp.status == 200:
      drive_file = json.loads(data)
      self._cache_metadata(path, drive_file)
      return True
    else:
      return False

  def rm(self, path):
    """Delete a file

    Args:
      file_id: string

    Returns:
      success: boolean
    """
    try:
      path = util.format_path(path)
      metadata = self._path_to_metadata(path)
      file_id = metadata['id']

      self.service.files().delete(fileId=file_id).execute()
      self._decache_metadata(path, metadata)

    except errors.HttpError, error:
      print 'An error occurred: %s' % error
      return False
    else:
      return True

  def rmdir(self, path):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path, isfolder=True)
    file_id = metadata['id']

    self.service.files().delete(fileId=file_id).execute()
    self._decache_metadata(path, metadata)

  def metadata(self, path):
    path = util.format_path(path)
    _md = self.search(path)
    md = {}
    md['size'] = eval(_md['fileSize'])
    md['mtime'] = util.convert_time(_md['modifiedDate'])
    return md

  def share(self, path, target_email):
    path = util.format_path(path)
    metadata = self._path_to_metadata(path)
    #print(metadata)
    #perm = self.service.permissions().list(fileId=metadata['id']).execute()
    body = {
      "role" : "writer",
      "type" : "user",
      "value": target_email
    }
    self.service.permissions().insert(fileId=metadata['id'], emailMessage=True, body=body).execute()

  def revision(self, file_id):
    # should not use
    try:
      request = self.service.files().get(fileId=file_id)
      drive_file = request.execute()
      print 'rev: ', drive_file['headRevisionId']
    except errors.HttpError as detail:
      print 'An error occurred: %s' % detail
      return None

  def search(self, path):
    """Search a file or folder

    Args:
      path: string

    Returns:
      metadata of the given path
    """

    try:
      if path == '/':
        #print 'search: root folder'
        return None

      pathlist = path.strip('/').split('/')

      abspath = ''
      parent = None

      for name in pathlist:
        abspath += '/' + name

        param = {}
        if parent:
          param['q'] = 'title="%s" and "%s" in parents and trashed=false' % (name, parent['id'])
        else:
          param['q'] = 'title="%s" and trashed=false' % name
        resp = self.service.files().list(**param).execute()
        #print(resp)

        if not resp['items']:
          #print "error in search: %s doesn't exist" % abspath
          return None

        # assume no two files/folders have the same name
        # just retrieve the first result
        parent = resp['items'][0]
        self._cache_metadata(abspath, parent)

      return parent

    except errors.HttpError, error:
      print 'An error occurred: %s' % error
      raise Exception(error)

  def post_comment(self, path, comment):
    try:
      path = util.format_path(path)
      metadata = self._path_to_metadata(path)
      file_id = metadata['id']

      new_comment = {
        'content': comment
      }

      self.service.comments().insert(fileId=file_id, body=new_comment).execute()

    except errors.HttpError, error:
      print 'An error occurred: %s' % error
      return False
    else:
      return True

  def init_log(self, path):
    if not self.exists(path):
      self.put(path, '')

  def reset_log(self, path):
    if self.exists(path):
      self.rm(path)

  # send msg to acceptor file
  def append(self, path, msg):
    self.post_comment(path, msg)

  # get logs from acceptor file
  def get_logs(self, path, last_clock):
    path = util.format_path(path)
    file_id = self._path_to_metadata(path)['id']

    # latest comment comes first
    comments = self.service.comments().list(fileId=file_id, maxResults=5).execute()
    if not comments['items']:
      return [], None
    
    new_logs = []
    new_clock = comments['items'][0]['commentId']
    end = False

    while True:
      for comment in comments['items']:
        if last_clock and comment['commentId'] == last_clock:
          end = True
          break
        new_logs.insert(0, comment['content'])
      if end: break
      if 'nextPageToken' not in comments: break
      # get a new batch (5) comments
      comments = self.service.comments().list(fileId=file_id, maxResults=5, pageToken=comments['nextPageToken']).execute()

    return new_logs, new_clock
