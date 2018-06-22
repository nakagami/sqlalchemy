# mssql/pymssql.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: mssql+pymssql
    :name: pymssql
    :dbapi: pymssql
    :connectstring: mssql+pymssql://<username>:<password>@<freetds_name>/?\
charset=utf8
    :url: http://pymssql.org/

pymssql is a Python module that provides a Python DBAPI interface around
`FreeTDS <http://www.freetds.org/>`_.  Compatible builds are available for
Linux, MacOSX and Windows platforms.

Modern versions of this driver work very well with SQL Server and
FreeTDS from Linux and is highly recommended.

"""
from .base import MSDialect, MSIdentifierPreparer
from ... import types as sqltypes, util, processors
import re


class _MSNumeric_minitds(sqltypes.Numeric):
    def result_processor(self, dialect, type_):
        if not self.asdecimal:
            return processors.to_float
        else:
            return sqltypes.Numeric.result_processor(self, dialect, type_)


class MSIdentifierPreparer_minitds(MSIdentifierPreparer):

    def __init__(self, dialect):
        super(MSIdentifierPreparer_minitds, self).__init__(dialect)
        # pymssql has the very unusual behavior that it uses pyformat
        # yet does not require that percent signs be doubled
        self._double_percents = False


class MSDialect_minitds(MSDialect):
    supports_native_decimal = True
    driver = 'minitds'

    preparer = MSIdentifierPreparer_minitds

    colspecs = util.update_copy(
        MSDialect.colspecs,
        {
            sqltypes.Numeric: _MSNumeric_minitds,
            sqltypes.Float: sqltypes.Float,
        }
    )

    @classmethod
    def dbapi(cls):
        module = __import__('minitds')

        module.Binary = lambda x: x if hasattr(x, 'decode') else str(x)

        return module

    def _get_server_version_info(self, connection):
        vers = connection.scalar("select @@version")
        m = re.match(
            r"Microsoft .*? - (\d+).(\d+).(\d+).(\d+)", vers)
        if m:
            return tuple(int(x) for x in m.group(1, 2, 3, 4))
        else:
            return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        port = opts.pop('port', None)
        if port and 'host' in opts:
            opts['host'] = "%s:%s" % (opts['host'], port)
        opts['use_ssl'] = False
        return [[], opts]

    def is_disconnect(self, e, connection, cursor):
        for msg in (
            "Adaptive Server connection timed out",
            "Net-Lib error during Connection reset by peer",
            "message 20003",  # connection timeout
            "Error 10054",
            "Not connected to any MS SQL server",
            "Connection is closed",
            "message 20006",  # Write to the server failed
            "message 20017",  # Unexpected EOF from the server
        ):
            if msg in str(e):
                return True
        else:
            return False

    def set_isolation_level(self, connection, level):
        import sys
        if level == 'AUTOCOMMIT':
            connection.set_autocommit(True)
        else:
            connection.set_autocommit(False)
            super(MSDialect_minitds, self).set_isolation_level(connection,
                                                               level)


dialect = MSDialect_minitds
