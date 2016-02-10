from axolotl.state.identitykeystore import IdentityKeyStore
from axolotl.ecc.curve import Curve
from axolotl.identitykey import IdentityKey
from axolotl.util.keyhelper import KeyHelper
from axolotl.identitykeypair import IdentityKeyPair
from axolotl.ecc.djbec import *
import warnings
import MySQLdb

class MyIdentityKeyStore(IdentityKeyStore):


    def get_conn(self):
        conn = MySQLdb.connect(**self.args)
        conn.text_factory = bytes
        return conn

    def __init__(self, args, phoneNumber):
        """
        :type args: Connections args 
        """
        self.args = args
        self.phoneNumber = phoneNumber
        q = """CREATE TABLE IF NOT EXISTS %s_identities (_id INT NOT NULL AUTO_INCREMENT,
                       recipient_id BIGINT UNIQUE,registration_id INT, public_key LONGBLOB, private_key LONGBLOB,
                       next_prekey_id INT, timestamp INT, PRIMARY KEY (_id));""" % phoneNumber
        dbConn = self.get_conn()  
        with warnings.catch_warnings():
            warnings.simplefilter("ignore") 
            c = dbConn.cursor()
            c.execute(q)
            dbConn.commit()
        dbConn.close()
        #identityKeyPairKeys = Curve.generateKeyPair()
        #self.identityKeyPair = IdentityKeyPair(IdentityKey(identityKeyPairKeys.getPublicKey()),
        #                                       identityKeyPairKeys.getPrivateKey())
        # self.localRegistrationId = KeyHelper.generateRegistrationId()

    def getIdentityKeyPair(self):
        q = "SELECT public_key, private_key FROM {}_identities WHERE recipient_id = -1".format(self.phoneNumber)
        dbConn = self.get_conn()
        c = dbConn.cursor()
        c.execute(q)
        result = c.fetchone()
        dbConn.close()
        publicKey, privateKey = result
        return IdentityKeyPair(IdentityKey(DjbECPublicKey(publicKey[1:])), DjbECPrivateKey(privateKey))

    def getLocalRegistrationId(self):
        q = "SELECT registration_id FROM {}_identities WHERE recipient_id = -1".format(self.phoneNumber)
        dbConn = self.get_conn()
        c = dbConn.cursor()
        c.execute(q)
        result = c.fetchone()
        dbConn.close()
        return result[0] if result else None


    def storeLocalData(self, registrationId, identityKeyPair):
        q = """INSERT INTO {}_identities(recipient_id, registration_id, public_key, private_key) VALUES(-1, %s, %s, %s)
             ON DUPLICATE KEY UPDATE registration_id=VALUES(registration_id),public_key=VALUES(public_key),private_key=VALUES(private_key)""".format(self.phoneNumber)
        dbConn = self.get_conn()
        c = dbConn.cursor()
        c.execute(q, (registrationId, identityKeyPair.getPublicKey().getPublicKey().serialize(),
                      identityKeyPair.getPrivateKey().serialize()))

        dbConn.commit()
        dbConn.close()

    def saveIdentity(self, recipientId, identityKey):
        q = "DELETE FROM {}_identities WHERE recipient_id=%s".format(self.phoneNumber)
        dbConn = self.get_conn()
        dbConn.cursor().execute(q, (recipientId,))
        dbConn.commit()
        dbConn.close()
        dbConn = self.get_conn()
        q = "INSERT INTO {}_identities (recipient_id, public_key) VALUES(%s, %s)".format(self.phoneNumber)
        dbConn.cursor().execute(q, (recipientId, identityKey.getPublicKey().serialize()))
        dbConn.commit()
        dbConn.close()

    def isTrustedIdentity(self, recipientId, identityKey):
        q = "SELECT public_key from {}_identities WHERE recipient_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        c = dbConn.cursor()
        c.execute(q, (recipientId,))
        result = c.fetchone()
        dbConn.close()
        if not result:
            return True
        if result[0] == identityKey.getPublicKey().serialize():
            return True
        else:
            logger.info("Removed untrusted key")
            self.saveIdentity(recipientId, identityKey)
            return True
