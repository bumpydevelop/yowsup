from axolotl.state.signedprekeystore import SignedPreKeyStore
from axolotl.state.signedprekeyrecord import SignedPreKeyRecord
from axolotl.invalidkeyidexception import InvalidKeyIdException
import logging
import warnings 
import MySQLdb

logger = logging.getLogger(__name__)


class MySignedPreKeyStore(SignedPreKeyStore):

    def get_conn(self):
        conn = MySQLdb.connect(**self.args)
        conn.text_factory = bytes
        return conn

    def __init__(self, args, phoneNumber):
        """
        :type dbConn: Connection
        """

        self.args = args
        self.phoneNumber = phoneNumber
        dbConn = self.get_conn()
        q = """ CREATE TABLE IF NOT EXISTS %s_signed_prekeys (_id INT NOT NULL AUTO_INCREMENT,
                       prekey_id INT UNIQUE, timestamp INT, record LONGBLOB, PRIMARY KEY (_id)); """ % phoneNumber
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dbConn.cursor().execute(q)
            dbConn.commit()
        
        dbConn.close()

    def loadSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM {}_signed_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        result = cursor.fetchone()
        dbConn.close()
        if not result:
            raise InvalidKeyIdException("No such signedprekeyrecord! {}  for phone {}".format(signedPreKeyId, self.phoneNumber))

        return SignedPreKeyRecord(serialized=result[0])

    def loadSignedPreKeys(self):
        q = "SELECT record FROM {}_signed_prekeys".format(self.phoneNumber)
        dbConn = self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q,)
        result = cursor.fetchall()
        results = []
        for row in result:
            results.append(SignedPreKeyRecord(serialized=row[0]))
        dbConn.close()
        return results

    def storeSignedPreKey(self, signedPreKeyId, signedPreKeyRecord):
        #q = "DELETE FROM {}_signed_prekeys WHERE prekey_id = %s"
        #self.dbConn.cursor().execute(q, (signedPreKeyId,))
        #self.dbConn.commit()
        logger.info("storing signedprekey {} for phone number {}".format(signedPreKeyId,self.phoneNumber))
        dbConn = self.get_conn()
        q = "INSERT INTO {}_signed_prekeys (prekey_id, record) VALUES(%s,%s)".format(self.phoneNumber)
        cursor = dbConn.cursor()
        cursor.execute(q, (signedPreKeyId, signedPreKeyRecord.serialize()))
        dbConn.commit()
        dbConn.close()

    def containsSignedPreKey(self, signedPreKeyId):
        q = "SELECT record FROM {}_signed_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        result = cursor.fetchone()
        dbConn.close()
        return result is not None

    def removeSignedPreKey(self, signedPreKeyId):
        q = "DELETE FROM {}_signed_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q, (signedPreKeyId,))
        dbConn.commit()
        dbConn.close()
