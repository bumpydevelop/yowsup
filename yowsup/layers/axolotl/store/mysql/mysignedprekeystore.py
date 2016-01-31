from axolotl.state.signedprekeystore import SignedPreKeyStore
from axolotl.state.signedprekeyrecord import SignedPreKeyRecord
from axolotl.invalidkeyidexception import InvalidKeyIdException
import logging
logger = logging.getLogger(__name__)


class MySignedPreKeyStore(SignedPreKeyStore):

    def __init__(self, dbConn, phoneNumber):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.phoneNumber = phoneNumber
        q = """ CREATE TABLE IF NOT EXISTS %s_signed_prekeys (_id INT NOT NULL AUTO_INCREMENT,
                       prekey_id INT UNIQUE, timestamp INT, record BLOB, PRIMARY KEY (_id)); """ % phoneNumber
        dbConn.cursor().execute(q)


    def loadSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM {}_signed_prekeys WHERE prekey_id = %s".format(self.phoneNumber)

        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))

        result = cursor.fetchone()
        if not result:
            raise InvalidKeyIdException("No such signedprekeyrecord! {}  for phone {}".format(signedPreKeyId, self.phoneNumber))

        return SignedPreKeyRecord(serialized=result[0])

    def loadSignedPreKeys(self):
        q = "SELECT record FROM {}_signed_prekeys".format(self.phoneNumber)

        cursor = self.dbConn.cursor()
        cursor.execute(q,)
        result = cursor.fetchall()
        results = []
        for row in result:
            results.append(SignedPreKeyRecord(serialized=row[0]))

        return results

    def storeSignedPreKey(self, signedPreKeyId, signedPreKeyRecord):
        #q = "DELETE FROM {}_signed_prekeys WHERE prekey_id = %s"
        #self.dbConn.cursor().execute(q, (signedPreKeyId,))
        #self.dbConn.commit()
        logger.info("storing signed pre key {} for phone number {}".format(signedPreKeyId,self.phoneNumber))
        q = "INSERT INTO {}_signed_prekeys (prekey_id, record) VALUES(%s,%s)".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId, signedPreKeyRecord.serialize()))
        self.dbConn.commit()

    def containsSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM {}_signed_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        return cursor.fetchone() is not None

    def removeSignedPreKey(self, signedPreKeyId):
        q = "DELETE FROM {}_signed_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        self.dbConn.commit()
