import logging
import qpack
from twisted.internet import reactor, defer  # @UnresolvedImport
from twisted.internet.protocol import Protocol  # @UnresolvedImport
from twisted.internet.error import TimeoutError  # @UnresolvedImport
from twisted.python import log  # @UnresolvedImport
from . import protomap
from .datapackage import DataPackage
from .exceptions import InsertError
from .exceptions import QueryError
from .exceptions import ServerError
from .exceptions import PoolError
from .exceptions import AuthenticationError
from .exceptions import UserAuthError


class ClientProtocol(Protocol):

    _MAP = {
        # SiriDB Client protocol success response types
        protomap.CPROTO_RES_QUERY: lambda f, d: f.callback(d),
        protomap.CPROTO_RES_INSERT: lambda f, d: f.callback(d),
        protomap.CPROTO_RES_ACK: lambda f, d: f.callback(None),
        protomap.CPROTO_RES_AUTH_SUCCESS: lambda f, d: f.callback(True),
        protomap.CPROTO_RES_INFO: lambda f, d: f.callback(d),
        protomap.CPROTO_RES_FILE: lambda f, d: f.callback(d),

        # SiriDB Client protocol error response types
        protomap.CPROTO_ERR_MSG: lambda f, d: f.errback(
            RuntimeError(d.get('error_msg', None))),
        protomap.CPROTO_ERR_QUERY: lambda f, d: f.errback(
            QueryError(d.get('error_msg', None))),
        protomap.CPROTO_ERR_INSERT: lambda f, d: f.errback(
            InsertError(d.get('error_msg', None))),
        protomap.CPROTO_ERR_SERVER: lambda f, d: f.errback(
            ServerError(d.get('error_msg', None))),
        protomap.CPROTO_ERR_POOL: lambda f, d: f.errback(
            PoolError(d.get('error_msg', None))),
        protomap.CPROTO_ERR_USER_ACCESS: lambda f, d: f.errback(
            UserAuthError(d.get('error_msg', None))),
        protomap.CPROTO_ERR: lambda f, d: f.errback(
            RuntimeError(
                'Unexpected error occurred, view siridb log '
                'for more info')),
        protomap.CPROTO_ERR_NOT_AUTHENTICATED: lambda f, d: f.errback(
            AuthenticationError('This connection is not authenticated')),
        protomap.CPROTO_ERR_AUTH_CREDENTIALS: lambda f, d: f.errback(
            AuthenticationError('Invalid credentials')),
        protomap.CPROTO_ERR_AUTH_UNKNOWN_DB: lambda f, d: f.errback(
            AuthenticationError('Unknown database')),
        protomap.CPROTO_ERR_LOADING_DB: lambda f, d: f.errback(
            RuntimeError('Error loading database, '
                'please check the SiriDB log files')),
        protomap.CPROTO_ERR_FILE: lambda f, d: f.errback(
            RuntimeError('Error retreiving file')),
    }

    def __init__(self):
        self._auth = False
        self._pid = 0
        self._requests = {}
        self._bufferedData = bytearray()
        self._dataPackage = None

    def connectionMade(self):
        deferred = self.sendPackage(
            protomap.CPROTO_REQ_AUTH,
            data=(
                self.factory._username,
                self.factory._password,
                self.factory._dbname),
            timeout=10)
        deferred.chainDeferred(self.factory._authDeferred)

    def connectionLost(self, *args, **kwargs):
        return Protocol.connectionLost(self, *args, **kwargs)

    def dataReceived(self, data):
        self._bufferedData.extend(data)
        while self._bufferedData:
            size = len(self._bufferedData)
            if self._dataPackage is None:
                if size < DataPackage.struct_datapackage.size:
                    return None
                self._dataPackage = DataPackage(self._bufferedData)
            if size < self._dataPackage.length:
                return None
            try:
                self._dataPackage.extract_data_from(self._bufferedData)
            except Exception as e:
                log.err(e)
                # empty the bytearray to recover from this error
                del self._bufferedData[:]
            else:
                self._onPackageReceived()
            self._dataPackage = None

    def _onPackageReceived(self):
        try:
            deferred, task = self._requests.pop(self._dataPackage.pid)
        except KeyError:
            log.msg('Package ID not found: {}'.format(self._dataPackage.pid),
                    logLevel=logging.ERROR)
            return None

        task.cancel()

        self.onPackageReceived(
            self._dataPackage.pid,
            self._dataPackage.tipe,
            self._dataPackage.data,
            deferred
        )

    def onPackageReceived(self, pid, tipe, data=None, deferred=None):
        self._MAP.get(tipe, lambda f, d: f.errback(
            TypeError('Client received an unknown package type: {}'
                      .format(tipe))))(deferred, data)

    def timeoutRequest(self, pid):
        self._requests[pid][0].errback(TimeoutError('Request timed out'))
        del self._requests[pid]

    def _sendPackage(self, pid, tipe, data=None):
        if data:
            data = bytes(qpack.packb(data))
            length = len(data)
        else:
            data = b''
            length = 0
        header = DataPackage.struct_datapackage.pack(
            length,
            pid,
            tipe,
            tipe ^ 255)
        self.transport.write(header + data)

    def sendPackage(self, tipe, data=None, timeout=1800):
        self._pid += 1
        self._pid %= 65536  # pid is handled as uint16_t
        deferred = defer.Deferred()
        self._sendPackage(self._pid, tipe, data)
        task = reactor.callLater(timeout, self.timeoutRequest, self._pid)
        self._requests[self._pid] = (deferred, task)
        return deferred
