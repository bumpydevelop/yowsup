from axolotl.state.identitykeystore import IdentityKeyStore
from axolotl.ecc.curve import Curve
from axolotl.identitykey import IdentityKey
from axolotl.util.keyhelper import KeyHelper
from axolotl.identitykeypair import IdentityKeyPair
from axolotl.ecc.djbec import *


class MyIdentityKeyStore(IdentityKeyStore):
    def __init__(self, dbConn, phoneNumber):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn

        self.phoneNumber = phoneNumber
        q = """CREATE TABLE IF NOT EXISTS %s_identities (_id INT NOT NULL AUTO_INCREMENT,
                       recipient_id BIGINT UNIQUE,registration_id INT, public_key BLOB, private_key BLOB,
                       next_prekey_id INT, timestamp INT, PRIMARY KEY (_id));""" % phoneNumber

        dbConn.cursor().execute(q)

        #identityKeyPairKeys = Curve.generateKeyPair()
        #self.identityKeyPair = IdentityKeyPair(IdentityKey(identityKeyPairKeys.getPublicKey()),
        #                                       identityKeyPairKeys.getPrivateKey())
        # self.localRegistrationId = KeyHelper.generateRegistrationId()

    def getIdentityKeyPair(self):
        q = "SELECT public_key, private_key FROM {}_identities WHERE recipient_id = -1".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q)
        result = c.fetchone()

        publicKey, privateKey = result
        return IdentityKeyPair(IdentityKey(DjbECPublicKey(publicKey[1:])), DjbECPrivateKey(privateKey))

    def getLocalRegistrationId(self):
        q = "SELECT registration_id FROM {}_identities WHERE recipient_id = -1".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q)
        result = c.fetchone()
        return result[0] if result else None


    def storeLocalData(self, registrationId, identityKeyPair):
        q = "INSERT INTO {}_identities(recipient_id, registration_id, public_key, private_key) VALUES(-1, %s, %s, %s)".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q, (registrationId, identityKeyPair.getPublicKey().getPublicKey().serialize(),
                      identityKeyPair.getPrivateKey().serialize()))

        self.dbConn.commit()

    def saveIdentity(self, recipientId, identityKey):
        q = "DELETE FROM {}_identities WHERE recipient_id=%s".format(self.phoneNumber)
        self.dbConn.cursor().execute(q, (recipientId,))
        self.dbConn.commit()

        q = "INSERT INTO {}_identities (recipient_id, public_key) VALUES(%s, %s)".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, identityKey.getPublicKey().serialize()))
        self.dbConn.commit()

    def isTrustedIdentity(self, recipientId, identityKey):
        q = "SELECT public_key from {}_identities WHERE recipient_id = %s".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId,))
        result = c.fetchone()
        if not result:
            return True
        if result[0] == identityKey.getPublicKey().serialize():
            return True
        else:
            logger.info("Removed untrusted key")
            self.saveIdentity(recipientId, identityKey)
            return True
