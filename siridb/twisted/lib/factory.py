import time
import logging
from twisted.internet.protocol import ClientFactory  # @UnresolvedImport
from twisted.internet import defer  # @UnresolvedImport
from twisted.internet import reactor  # @UnresolvedImport
from twisted.internet import task  # @UnresolvedImport
from twisted.python import log  # @UnresolvedImport
from .protocol import ClientProtocol
from . import protomap


class SiriFactory(ClientFactory):

    protocol = None
    connected = False
    isAvailable = False
    connector = None

    def __init__(self,
                 username,
                 password,
                 dbname,
                 host,
                 port,
                 config,
                 triggerConnect,
                 inactiveTime):
        self._username = username
        self._password = password
        self._dbname = dbname
        self.host = host
        self.port = port
        self.isBackup = config.get('backup', False)
        self.weight = config.get('weight', 1)
        self._triggerConnect = triggerConnect
        self._inactiveTime = inactiveTime

    @defer.inlineCallbacks
    def keepaliveLoop(self, interval=45):
        sleep = interval
        self.lastResp = time.time()
        while True:
            yield task.deferLater(reactor, sleep, lambda: None)
            if not self.connected:
                break
            sleep = \
                max(0, interval - time.time() + self.lastResp) or interval
            if sleep == interval:
                log.msg('Send keep-alive package...', logLevel=logging.DEBUG)
                try:
                    yield self.protocol.sendPackage(
                        protomap.CPROTO_REQ_PING,
                        timeout=15)
                except Exception as e:
                    log.err(e)
                    self.connector.disconnect()
                    break

    def setAuthDeferred(self):
        self._authDeferred = defer.Deferred()
        self._authDeferred.addCallback(self.onAuthenticated)

    def buildProtocol(self, addr):
        self.protocol = ClientProtocol()
        self.protocol.factory = self
        self.onConnectionMade(addr)
        return self.protocol

    def clientConnectionFailed(self, connector, reason):
        self.connected = False
        self.isAvailable = False
        self._authDeferred.errback(reason)

    def clientConnectionLost(self, connector, reason):
        self.connected = False
        self.isAvailable = False
        self.protocol = None
        self._triggerConnect()

    def onConnectionMade(self, addr):
        self.connected = True
        self.keepaliveLoop()

    def onAuthenticated(self, result):
        self.isAvailable = True
        return result

    def setAvailable(self):
        if self.connected:
            self.isAvailable = True

    def setNotAvailable(self):
        if self.isAvailable:
            self.isAvailable = False
            reactor.callLater(self._inactiveTime, self.setAvailable)
