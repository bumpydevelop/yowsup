from axolotl.state.sessionstore import SessionStore
from axolotl.state.sessionrecord import SessionRecord
class MySessionStore(SessionStore):
    def __init__(self, dbConn, phoneNumber):
        """
        :type dbConn: Connection
        """
        self.dbConn = dbConn
        self.phoneNumber = phoneNumber
        q = """CREATE TABLE IF NOT EXISTS %s_sessions (_id INT NOT NULL AUTO_INCREMENT,
                       recipient_id BIGINT(20) UNIQUE, device_id INT, record BLOB, timestamp INT, PRIMARY KEY (_id));"""% phoneNumber
        dbConn.cursor().execute(q)

    def loadSession(self, recipientId, deviceId):
        result = None
        q = "SELECT record FROM {}_sessions WHERE recipient_id = %s AND device_id = %s".format(self.phoneNumber)
        try:
            c = self.dbConn.cursor()
            c.execute(q, (recipientId, deviceId))
            result = c.fetchone()
        except:
            pass
        if result:
            return SessionRecord(serialized=result[0])
        else:
            return SessionRecord()

    def getSubDeviceSessions(self, recipientId):
        q = "SELECT device_id from {}_sessions WHERE recipient_id = %s".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId,))
        result = c.fetchall()

        deviceIds = [r[0] for r in result]
        return deviceIds

    def storeSession(self, recipientId, deviceId, sessionRecord):
        self.deleteSession(recipientId, deviceId)

        q = "INSERT INTO {}_sessions(recipient_id, device_id, record) VALUES(%s,%s,%s)".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, deviceId, sessionRecord.serialize()))
        self.dbConn.commit()

    def containsSession(self, recipientId, deviceId):
        q = "SELECT record FROM {}_sessions WHERE recipient_id = %s AND device_id = %s".format(self.phoneNumber)
        c = self.dbConn.cursor()
        c.execute(q, (recipientId, deviceId))
        result = c.fetchone()

        return result is not None

    def deleteSession(self, recipientId, deviceId):
        q = "DELETE FROM {}_sessions WHERE recipient_id = %s AND device_id = %s".format(self.phoneNumber)
        self.dbConn.cursor().execute(q, (recipientId, deviceId))
        self.dbConn.commit()

    def deleteAllSessions(self, recipientId):
        q = "DELETE FROM {}_sessions WHERE recipient_id = %s".format(self.phoneNumber)
        self.dbConn.cursor().execute(q, (recipientId,))
        self.dbConn.commit()
