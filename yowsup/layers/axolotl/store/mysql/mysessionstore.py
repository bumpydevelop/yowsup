from axolotl.state.sessionstore import SessionStore
from axolotl.state.sessionrecord import SessionRecord
import warnings
import  MySQLdb


class MySessionStore(SessionStore):

    
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
        q = """CREATE TABLE IF NOT EXISTS %s_sessions (_id INT NOT NULL AUTO_INCREMENT,
                       recipient_id BIGINT(20) UNIQUE, device_id INT, record LONGBLOB, timestamp INT, PRIMARY KEY (_id));"""% phoneNumber
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            dbConn.cursor().execute(q)
            dbConn.commit()
        dbConn.close()

    def loadSession(self, recipientId, deviceId):
        result = None
        dbConn = self.get_conn()
        q = "SELECT record FROM {}_sessions WHERE recipient_id = %s AND device_id = %s".format(self.phoneNumber)
        try:
            c = dbConn.cursor()
            c.execute(q, (recipientId, deviceId))
            result = c.fetchone()
            dbConn.close()
        except:
            pass
        if result:
            return SessionRecord(serialized=result[0])
        else:
            return SessionRecord()

    def getSubDeviceSessions(self, recipientId):
        q = "SELECT device_id from {}_sessions WHERE recipient_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        c = dbConn.cursor()
        c.execute(q, (recipientId,))
        result = c.fetchall()
        dbConn.close()
        deviceIds = [r[0] for r in result]
        return deviceIds

    def storeSession(self, recipientId, deviceId, sessionRecord):
        self.deleteSession(recipientId, deviceId)
        dbConn = self.get_conn()
        q = """INSERT INTO {}_sessions(recipient_id, device_id, record) VALUES(%s,%s,%s)
                ON DUPLICATE KEY UPDATE device_id=VALUES(device_id), record=VALUES(record)""".format(self.phoneNumber)
        c = dbConn.cursor()
        c.execute(q, (recipientId, deviceId, sessionRecord.serialize()))
        dbConn.commit()
        dbConn.close()

    def containsSession(self, recipientId, deviceId):
        q = "SELECT record FROM {}_sessions WHERE recipient_id = %s AND device_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        c = dbConn.cursor()
        c.execute(q, (recipientId, deviceId))
        result = c.fetchone()
        dbConn.close()
        return result is not None

    def deleteSession(self, recipientId, deviceId):
        q = "DELETE FROM {}_sessions WHERE recipient_id = %s AND device_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        dbConn.cursor().execute(q, (recipientId, deviceId))
        dbConn.commit()
        dbConn.close()

    def deleteAllSessions(self, recipientId):
        q = "DELETE FROM {}_sessions WHERE recipient_id = %s".format(self.phoneNumber)
        dbConn = self.get_conn()
        dbConn.cursor().execute(q, (recipientId,))
        dbConn.commit()
        dbConn.close()
