from axolotl.state.prekeystore import PreKeyStore
from axolotl.state.prekeyrecord import PreKeyRecord
class MyPreKeyStore(PreKeyStore):
    def __init__(self, dbConn, phoneNumber):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.phoneNumber = phoneNumber

        q = """CREATE TABLE IF NOT EXISTS %s_prekeys (_id INT NOT NULL AUTO_INCREMENT,
               prekey_id INT UNIQUE, sent_to_server BOOLEAN, record BLOB, PRIMARY KEY (_id));""" % phoneNumber

        dbConn.cursor().execute(q)

    def loadPreKey(self, preKeyId):
        q = "SELECT record FROM {}_prekeys WHERE prekey_id = %s".format(self.phoneNumber)

        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId,))

        result = cursor.fetchone()
        if not result:
            raise Exception("No such prekeyRecord! where prekey_id {} for phone {}".format(preKeyId,self.phoneNumber))

        return PreKeyRecord(serialized = result[0])

    def loadPendingPreKeys(self):
        q = "SELECT record FROM {}_prekeys".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q)
        result = cursor.fetchall()

        return [PreKeyRecord(serialized=result[0]) for result in result]

    def storePreKey(self, preKeyId, preKeyRecord):
        #self.removePreKey(preKeyId)
        q = "INSERT INTO {}_prekeys (prekey_id, record) VALUES(%s,%s)".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId, preKeyRecord.serialize()))
        self.dbConn.commit()

    def containsPreKey(self, preKeyId):
        q = "SELECT record FROM {}_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId,))
        return cursor.fetchone() is not None

    def removePreKey(self, preKeyId):
        q = "DELETE FROM {}_prekeys WHERE prekey_id = %s".format(self.phoneNumber)
        cursor = self.dbConn.cursor()
        cursor.execute(q, (preKeyId,))
        self.dbConn.commit()
