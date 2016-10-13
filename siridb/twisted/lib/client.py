import logging
import random
import time
from twisted.internet import reactor  # @UnresolvedImport
from twisted.internet import defer  # @UnresolvedImport
from twisted.internet import task  # @UnresolvedImport
from twisted.python import log  # @UnresolvedImport
from twisted.internet.error import ConnectionClosed  # @UnresolvedImport
from .factory import SiriFactory
from . import protomap
from .exceptions import ServerError
from .exceptions import PoolError
from .exceptions import AuthenticationError

# never wait more than x seconds before trying to connect again
DEFAULT_MAX_WAIT_RETRY = 90

# default timeout used while connecting to a SiriDB server
DEFAULT_CONNECT_TIMEOUT = 10

# when a SiriDB server is marked as inactive, wait x seconds before releasing
# the inactive status.
DEFAULT_INACTIVE_TIME = 30


class SiriDBClientTwisted(object):
    '''
        Exception handling:

        - InsertError (can only be raised when using the insert() method)
            Make sure the data is correct because this only happens when
            SiriDB could not process the request. Its likely to fail again
            on a retry.
        - QueryError (can only be raised when using the query() method)
            Make sure the query is correct because this only happens when
            SiriDB could not process the query. Its likely to fail again.
        - PoolError
            SiriDB has no online server for at least one required pool
            Try again later after some reasonable delay.
        - siritwisted.shared.exceptions.AuthenticationError
            Raised when credentials are invalid or insufficient
        - IndexError
            Raised when the database does not exist anymore
        - TypeError
            Raised when an unknown package is received. (might be caused
            by running a different SiriDB version)
        - RuntimeError
            Raised when a general error message is received. This should not
            happen unless a new bug is discovered.
        - OverflowError (can only be raised when using the insert() method)
            Raise when integer values cannot not be packed due to an overflow
            error. (integer values should be signed and not more than 63 bits)
    '''

    def __init__(self,
                 username,
                 password,
                 dbname,
                 hostlist,
                 inactiveTime=DEFAULT_INACTIVE_TIME,
                 maxWaitRetry=DEFAULT_MAX_WAIT_RETRY):
        '''Initialize.
        Arguments:
            username: User with permissions to use the database.
            password: Password for the given username.
            dbname: Name of the database.
            hostlist: List with SiriDB servers. (all servers or a subset of
                      servers can be in this list.)

                      Example:
                      [
                          ('server1.local', 9000, {'weight': 3}),
                          ('server2.local', 9000),
                          ('backup1.local', 9000, {'backup': True})
                      ]

                      Each server should at least has a hostname and port
                      number. Optionally you can provide a dictionary with
                      extra options.

                      Available Options:
                      - weight : Should be a value between 1 and 9. A higher
                                 value gives the server more weight so it will
                                 be more likely chosen. (default 1)
                      - backup : Should be either True or False. When True the
                                 server will be marked as backup server and
                                 will only be chosen if no other server is
                                 available. (default: False)
        Keyword arguments:
            inactiveTime: When a server is temporary not available, for
                          example the server could be paused, we mark the
                          server inactive for x seconds.
            maxWaitRetry: When the reconnect loop starts, we try to reconnect
                          in a seconds, then 2 seconds, 4, 8 and so on until
                          max_wait_retry is reached and then use this value
                          to retry again.
        '''
        hostlist = [list(host[:2]) + [host[2] if len(host) > 2 else {}]
                    for host in hostlist]
        self._factories = set(
            [SiriFactory(username,
                         password,
                         dbname,
                         host,
                         port,
                         config,
                         triggerConnect=self._triggerConnect,
                         inactiveTime=inactiveTime)
             for host, port, config in hostlist])
        self._factory_pool = []
        for factory in self._factories:
            assert 0 < factory.weight < 10, \
                'weight should be value between 1 and 9'
            for _ in range(factory.weight):
                self._factory_pool.append(factory)
        self._maxWaitRetry = maxWaitRetry

    @defer.inlineCallbacks
    def connect(self, timeout=DEFAULT_CONNECT_TIMEOUT):
        self._retryConnect = True
        result = yield self._connect(timeout)
        if not all([success for success, _ in result]):
            for b, r in result:
                if isinstance(r.value, (AuthenticationError, IndexError)):
                    r.raiseException()
            else:
                self._connectLoop()
        defer.returnValue(result)

    def close(self):
        self._retryConnect = False
        for factory in self._factories:
            if factory.connected:
                factory.connector.disconnect()

    @defer.inlineCallbacks
    def insert(self, data, timeout=3600):
        '''Insert data into SiriDB.

        see module doc-string for info on exception handling.
        '''
        while True:
            factory = self._getRandomConnection()

            try:
                result = yield factory.protocol.sendPackage(
                    protomap.CPROTO_REQ_INSERT,
                    data,
                    timeout)
            except (ConnectionClosed, ServerError) as e:
                log.msg('Insert failed with error {!r}, trying another '
                        'server if one is available...'.format(e),
                        logLevel=logging.DEBUG)
                if factory.connected:
                    factory.setNotAvailable()
            else:
                factory.lastResp = time.time()
                defer.returnValue(result)

    @defer.inlineCallbacks
    def query(self, query, timePrecision=None, timeout=3600):
        '''Send a query to SiriDB.

        see module doc-string for info on exception handling.
        '''
        tryUnavailable = True
        while True:
            factory = self._getRandomConnection(tryUnavailable)
            try:
                result = yield factory.protocol.sendPackage(
                        protomap.CPROTO_REQ_QUERY,
                        data=(query, timePrecision),
                        timeout=timeout)
            except (ConnectionClosed, ServerError) as e:
                log.msg('Query failed with error {!r}, trying another '
                        'server if one is available...'.format(e),
                        logLevel=logging.DEBUG)
                if factory.connected:
                    factory.setNotAvailable()
            else:
                factory.lastResp = time.time()
                defer.returnValue(result)

            # only try unavailable once
            tryUnavailable = False

    @defer.inlineCallbacks
    def _connect(self, timeout=DEFAULT_CONNECT_TIMEOUT):
        tasks = []
        for factory in self._factories:
            if not factory.connected:
                factory.setAuthDeferred()
                if factory.connector:
                    factory.connector.connect()
                else:
                    factory.connector = reactor.connectTCP(
                        factory.host,
                        factory.port,
                        factory,
                        timeout=timeout)
                tasks.append(factory._authDeferred)
        if not tasks:
            return
        result = yield defer.DeferredList(tasks, consumeErrors=True)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _connectLoop(self):
        sleep = 1
        try:
            while [factory
                   for factory in self._factories
                   if not factory.connected]:
                log.msg('Reconnecting in {} seconds...'.format(sleep),
                        logLevel=logging.DEBUG)
                yield task.deferLater(reactor, sleep, lambda: None)
                if not self._retryConnect:
                    break
                yield self._connect()
                sleep = min(sleep * 2, self._maxWaitRetry)
        except Exception as e:
            log.err(e)

    def _triggerConnect(self):
        if self._retryConnect:
            self._connectLoop()

    def _getRandomConnection(self, tryUnavailable=False):
        available = [factory
                     for factory in self._factory_pool
                     if factory.isAvailable]

        nonBackups = [factory for factory in available if not factory.isBackup]

        if nonBackups:
            return random.choice(nonBackups)

        if available:
            return random.choice(available)

        if tryUnavailable:

            connections = \
                [factory
                 for factory in self._factory_pool
                 if factory.connected]

            if connections:
                return random.choice(connections)

        raise PoolError('No available connections found')
