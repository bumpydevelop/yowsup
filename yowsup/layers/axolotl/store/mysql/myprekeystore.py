from axolotl.state.prekeystore import PreKeyStore
from axolotl.state.prekeyrecord import PreKeyRecord
import warnings
import logging
import MySQLdb

logger = logging.getLogger(__name__)

class MyPreKeyStore(PreKeyStore):

    def get_conn(self):
        conn = MySQLdb.connect(**self.args)
        conn.text_factory = bytes
        return conn

    def __init__(self, args, phoneNumber):
        """
        :type args: Connection args
        """
        self.args = args
        self.phoneNumber = phoneNumber
        
        dbConn =  self.get_conn()
        q = """CREATE TABLE IF NOT EXISTS %s_prekeys (_id INT NOT NULL AUTO_INCREMENT,
               prekey_id INT UNIQUE, sent_to_server BOOLEAN, record LONGBLOB, PRIMARY KEY (_id));""" % phoneNumber
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            c = dbConn.cursor()
            c.execute(q)
            dbConn.commit()
        dbConn.close()

    def loadPreKey(self, preKeyId):
        q = "SELECT record FROM {}_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        dbConn =  self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q, (preKeyId,))

        result = cursor.fetchone()
        dbConn.close()
        if not result:
            raise Exception("No such prekeyRecord! where prekey_id {} for phone {}".format(preKeyId,self.phoneNumber))

        return PreKeyRecord(serialized = result[0])


    def loadPendingPreKeys(self):
        q = "SELECT record FROM {}_prekeys".format(self.phoneNumber)
        dbConn =  self.get_conn()
        dbConn.cursor().execute(q)
        result = cursor.fetchall()
        dbConn.close()
        return [PreKeyRecord(serialized=result[0]) for result in result]

    def storePreKey(self, preKeyId, preKeyRecord):
        #self.removePreKey(preKeyId)
        q = "INSERT INTO {}_prekeys (prekey_id, record) VALUES(%s,%s)".format(self.phoneNumber)
        dbConn =  self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q, (preKeyId, preKeyRecord.serialize()))
        dbConn.commit()
        dbConn.close()

    def containsPreKey(self, preKeyId):
        q = "SELECT record FROM {}_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        dbConn =  self.get_conn()
        cursor = dbConn.cursor()
        result = cursor.execute(q, (preKeyId,))
        dbConn.close()
        return result is not None

    def removePreKey(self, preKeyId):
        q = "DELETE FROM {}_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        dbConn =  self.get_conn()
        cursor = dbConn.cursor()
        cursor.execute(q, (preKeyId,))
        dbConn.commit()
        dbConn.close()
