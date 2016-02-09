from axolotl.state.axolotlstore import AxolotlStore
from .myidentitykeystore import MyIdentityKeyStore
from .myprekeystore import MyPreKeyStore
from .mysessionstore import MySessionStore
from .mysignedprekeystore import MySignedPreKeyStore
import MySQLdb
import logging
import sys
logger = logging.getLogger(__name__)
class MyAxolotlStore(AxolotlStore):

    def parse_args(self, conn_args):
        args = {}
        for var in conn_args.split(';'):
            val = var.split('=')[1]
            args[var.split('=')[0]] = int(val) if val.isdigit() else val
        return args

    def __init__(self, conn_stream, phone_number):
        args = self.parse_args(conn_stream)
        self.conn_stream = args
        #conn = MySQLdb.connect(**args)
        #conn.text_factory = bytes
        self.identityKeyStore = MyIdentityKeyStore(args, phone_number)
        self.preKeyStore =  MyPreKeyStore(args, phone_number)
        self.signedPreKeyStore = MySignedPreKeyStore(args, phone_number)
        self.sessionStore = MySessionStore(args, phone_number)

    def getIdentityKeyPair(self):
        return self.identityKeyStore.getIdentityKeyPair()

    def storeLocalData(self, registrationId, identityKeyPair):
        self.identityKeyStore.storeLocalData(registrationId, identityKeyPair)

    def getLocalRegistrationId(self):
        return self.identityKeyStore.getLocalRegistrationId()

    def saveIdentity(self, recepientId, identityKey):
        self.identityKeyStore.saveIdentity(recepientId, identityKey)

    def isTrustedIdentity(self, recepientId, identityKey):
        return self.identityKeyStore.isTrustedIdentity(recepientId, identityKey)

    def loadPreKey(self, preKeyId):
        try:
            return self.preKeyStore.loadPreKey(preKeyId)
        except:
           logger.error("ERROR :: {}".format(sys.exc_info()[1]))
           return None

    def loadPreKeys(self):
        return self.preKeyStore.loadPendingPreKeys()

    def storePreKey(self, preKeyId, preKeyRecord):
        self.preKeyStore.storePreKey(preKeyId, preKeyRecord)

    def containsPreKey(self, preKeyId):
        return self.preKeyStore.containsPreKey(preKeyId)

    def removePreKey(self, preKeyId):
        self.preKeyStore.removePreKey(preKeyId)

    def loadSession(self, recepientId, deviceId):
        return self.sessionStore.loadSession(recepientId, deviceId)

    def getSubDeviceSessions(self, recepientId):
        return self.sessionStore.getSubDeviceSessions(recepientId)

    def storeSession(self, recepientId, deviceId, sessionRecord):
        self.sessionStore.storeSession(recepientId, deviceId, sessionRecord)

    def containsSession(self, recepientId, deviceId):
        return self.sessionStore.containsSession(recepientId, deviceId)

    def deleteSession(self, recepientId, deviceId):
        self.sessionStore.deleteSession(recepientId, deviceId)

    def deleteAllSessions(self, recepientId):
        self.sessionStore.deleteAllSessions(recepientId)

    def loadSignedPreKey(self, signedPreKeyId):
        return self.signedPreKeyStore.loadSignedPreKey(signedPreKeyId)

    def loadSignedPreKeys(self):
        return self.signedPreKeyStore.loadSignedPreKeys()

    def storeSignedPreKey(self, signedPreKeyId, signedPreKeyRecord):
        self.signedPreKeyStore.storeSignedPreKey(signedPreKeyId, signedPreKeyRecord)

    def containsSignedPreKey(self, signedPreKeyId):
        return self.signedPreKeyStore.containsSignedPreKey(signedPreKeyId)

    def removeSignedPreKey(self, signedPreKeyId):
        self.signedPreKeyStore.removeSignedPreKey(signedPreKeyId)
