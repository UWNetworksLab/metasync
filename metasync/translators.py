from crypto import AESCipher

# TODO. encrypt can be provided via one more translation layer
#
#      # depends on user's preferences, from config
#      self.translator = [_enctpytion, _mac]
#
#      @put(path, blob):
#        for tr in metasync.translators:
#           blob = tr.put(blob)
#        return blob
#
#
class TranslatePipe(object):
    def __init__(self, metasync):
        self.metasync = metasync
        self.config   = metasync.config
    def get(self, blob):
        return blob
    def put(self, blob):
        return blob

class TrEncrypt(TranslatePipe):
    def __init__(self, metasync):
        super(TrEncrypt, self).__init__(metasync)

    def _get_key(self):
        # XXX. is it fixed? or allowed to be changed?
        return self.config.get('core', 'encryptkey')

    def get(self, blob):
        aes = AESCipher(self._get_key())
        return aes.decrypt(blob)

    def put(self, blob):
        aes = AESCipher(self._get_key())
        return aes.encrypt(blob)

class TrSigned(TranslatePipe):
    def __init__(self, metasync):
        super(TrSigned, self).__init__(metasync)
        raise Exception("Not implemented yet")

    def get(self, blob):
        # check integrity
        pass

    def put(self, blob):
        # put message authentication
        pass

