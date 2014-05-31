
class APIError(Exception):
  """Base error for this module."""
  def __init__(self, status_code, msg=None, **kwargs):
    super(APIError, self).__init__(self.__str__)
    self.status_code = status_code
    self.msg = msg
    self.__dict__.update(kwargs)

  def __repr__(self):
    return 'status code [%s]\n    %s' % (self.status_code, self.msg)
  
  __str__ = __repr__

class TokenRequest(APIError):
  pass

class Unauthorized(APIError):
  pass

class BadRequest(APIError):
  pass

class ItemAlreadyExists(APIError):
  pass

class ItemDoesNotExist(APIError):
  pass

class PollError(APIError):
  pass
