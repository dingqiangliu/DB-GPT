"""Base class for Vertica dialect."""

# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
# Copyright (c) 2017 StartApp Inc.
# Copyright (c) 2015 Locus Energy
# Copyright (c) 2013 James Casbon
# Copyright (c) 2010 Bo Shi

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import logging
import re
from textwrap import dedent
from typing import Any, Optional

from sqlalchemy import exc, sql, util
from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, INTERVAL
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import String
from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BLOB,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    FLOAT,
    INTEGER,
    NUMERIC,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    VARBINARY,
    VARCHAR,
)

logger: logging.Logger = logging.getLogger(__name__)

ischema_names = {
    "INT": INTEGER,
    "INTEGER": INTEGER,
    "INT8": INTEGER,
    "BIGINT": BIGINT,
    "SMALLINT": SMALLINT,
    "TINYINT": SMALLINT,
    "CHAR": CHAR,
    "VARCHAR": VARCHAR,
    "VARCHAR2": VARCHAR,
    "TEXT": VARCHAR,
    "NUMERIC": NUMERIC,
    "DECIMAL": NUMERIC,
    "NUMBER": NUMERIC,
    "MONEY": NUMERIC,
    "FLOAT": FLOAT,
    "FLOAT8": FLOAT,
    "REAL": REAL,
    "DOUBLE": DOUBLE_PRECISION,
    "TIMESTAMP": TIMESTAMP,
    "TIMESTAMP WITH TIMEZONE": TIMESTAMP(timezone=True),
    "TIMESTAMPTZ": TIMESTAMP(timezone=True),
    "TIME": TIME,
    "TIME WITH TIMEZONE": TIME(timezone=True),
    "TIMETZ": TIME(timezone=True),
    "INTERVAL": INTERVAL,
    "INTERVAL HOUR TO SECOND": INTERVAL,
    "INTERVAL HOUR TO MINUTE": INTERVAL,
    "INTERVAL DAY TO SECOND": INTERVAL,
    "INTERVAL YEAR TO MONTH": INTERVAL,
    "DOUBLE PRECISION": DOUBLE_PRECISION,
    "DATE": DATE,
    "DATETIME": DATETIME,
    "SMALLDATETIME": DATETIME,
    "BINARY": BINARY,
    "VARBINARY": VARBINARY,
    "RAW": BLOB,
    "BYTEA": BYTEA,
    "BOOLEAN": BOOLEAN,
    "LONG VARBINARY": BLOB,
    "LONG VARCHAR": VARCHAR,
    "GEOMETRY": BLOB,
    "GEOGRAPHY": BLOB,
}


class UUID(String):
    """The SQL UUID type."""

    __visit_name__ = "UUID"


class TIMESTAMP_WITH_PRECISION(TIMESTAMP):
    """The SQL TIMESTAMP With Precision type.

    Since Vertica supports precision values for timestamp this allows ingestion
    of timestamp fields with precision values.
    PS: THIS DATA IS CURRENTLY UNUSED, IT JUST FIXES INGESTION PROBLEMS
    TODO: Should research the possibility of reflecting the precision in the schema

    """

    __visit_name__ = "TIMESTAMP"

    def __init__(self, timezone=False, precision=None):
        """Construct a new :class:`_types.TIMESTAMP_WITH_PRECISION`.

        :param timezone: boolean.  Indicates that the TIMESTAMP type should
         enable timezone support, if available on the target database.
         On a per-dialect basis is similar to "TIMESTAMP WITH TIMEZONE".
         If the target database does not support timezones, this flag is
         ignored.
        :param precision: integer.  Indicates the PRECISION field when provided


        """
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


def TIMESTAMP_WITH_TIMEZONE(*args, **kwargs):
    """Convert to timestamp with timezone."""
    kwargs["timezone"] = True
    return TIMESTAMP_WITH_PRECISION(*args, **kwargs)


def TIME_WITH_TIMEZONE(*args, **kwargs):
    """Convert to time with timezone."""
    kwargs["timezone"] = True
    return TIME(*args, **kwargs)


class VerticaDDLCompiler(PGDDLCompiler):
    """DDL compiler for Vertica."""

    def get_column_specification(self, column, **kwargs):
        """Return column specificaiton."""
        colspec = self.preparer.format_column(column)
        # noinspection PyUnusedLocal
        # noinspection PyProtectedMember
        if column.primary_key and column is column.table._autoincrement_column:
            colspec += " AUTO_INCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec


class VerticaInspector(reflection.Inspector):
    """Reflection inspector for Vertica."""

    dialect: VerticaDialect

    def get_all_columns(self, table, schema: Optional[str] = None, **kw: Any):
        r"""Return all table columns names within a particular schema."""
        return self.dialect.get_all_columns(
            self.bind, table, schema, info_cache=self.info_cache, **kw
        )

    def get_table_comment(self, table_name: str, schema: Optional[str] = None, **kw):
        """Return comment of a table in a schema."""
        return self.dialect.get_table_comment(
            self.bind, table_name, schema, info_cache=self.info_cache, **kw
        )

    def get_view_columns(
        self, view: Optional[str] = None, schema: Optional[str] = None, **kw: Any
    ):
        r"""Return all view columns names within a particular schema."""
        return self.dialect.get_view_columns(
            self.bind, view, schema, info_cache=self.info_cache, **kw
        )

    def get_view_comment(
        self, view: Optional[str] = None, schema: Optional[str] = None, **kw
    ):
        r"""Return view comments within a particular schema."""
        return self.dialect.get_view_comment(
            self.bind, view, schema, info_cache=self.info_cache, **kw
        )


# noinspection PyArgumentList,PyAbstractClass


class VerticaDialect(default.DefaultDialect):
    """Vertica dialect."""

    name = "vertica"
    ischema_names = ischema_names
    ddl_compiler = VerticaDDLCompiler
    inspector = VerticaInspector

    def __init__(self, json_serializer=None, json_deserializer=None, **kwargs):
        """Init object."""
        default.DefaultDialect.__init__(self, **kwargs)

        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer

    def initialize(self, connection):
        """Init dialect."""
        super().initialize(connection)

    def _get_default_schema_name(self, connection):
        return connection.scalar(sql.text("SELECT current_schema()"))

    def _get_server_version_info(self, connection):
        v = connection.scalar(sql.text("SELECT version()"))
        m = re.match(r".*Vertica Analytic Database v(\d+)\.(\d+)\.(\d)+.*", v)
        if not m:
            raise AssertionError(
                "Could not determine version from string '%(ver)s'" % {"ver": v}
            )
        return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

    def create_connect_args(self, url):
        """Create args of connection."""
        opts = url.translate_connect_args(username="user")
        opts.update(url.query)
        return [], opts

    def has_table(self, connection, table_name, schema=None):
        """Check availability of a table."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        has_table_sql = sql.text(
            dedent(
                """
                SELECT EXISTS (
                SELECT table_name
                FROM v_catalog.all_tables
                WHERE lower(table_name) = '%(table)s'
                AND lower(schema_name) = '%(schema)s')
                """
                % {"schema": schema.lower(), "table": table_name.lower()}
            )
        )

        c = connection.execute(has_table_sql)
        return bool(c.scalar())

    def has_sequence(self, connection, sequence_name, schema=None):
        """Check availability of a sequence."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        has_seq_sql = sql.text(
            dedent(
                """
                SELECT EXISTS (
                SELECT sequence_name
                FROM v_catalog.sequences
                WHERE lower(sequence_name) = '%(sequence)s'
                AND lower(sequence_schema) = '%(schema)s')
                """
                % {"schema": schema.lower(), "sequence": sequence_name.lower()}
            )
        )

        c = connection.execute(has_seq_sql)
        return bool(c.scalar())

    def has_type(self, connection, type_name):
        """Check availability of a type."""
        has_type_sql = sql.text(
            dedent(
                """
                SELECT EXISTS (
                SELECT type_name
                FROM v_catalog.types
                WHERE lower(type_name) = '%(type)s')
                """
                % {"type": type_name.lower()}
            )
        )

        c = connection.execute(has_type_sql)
        return bool(c.scalar())

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        """Return names of all schemas."""
        get_schemas_sql = sql.text(
            dedent(
                """
                SELECT schema_name
                FROM v_catalog.schemata
                """
            )
        )

        c = connection.execute(get_schemas_sql)
        return [row[0] for row in c if not row[0].startswith("v_")]

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        """Return comment of a table in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        sct = sql.text(
            dedent(
                """
                SELECT nvl(comment, table_name) as column_comment
                FROM v_catalog.tables t
                  LEFT JOIN v_internal.vs_comments c ON t.table_id = c.objectoid
                WHERE lower(table_name) = '%(table)s'
                  AND lower(table_schema) = '%(schema)s'
                """
                % {"schema": schema.lower(), "table": table_name.lower()}
            )
        )
        return {"text": connection.execute(sct).scalar()}

    @reflection.cache
    def _get_table_oid(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_oid_sql = sql.text(
            dedent(
                """
                SELECT table_id
                FROM
                    (SELECT table_id, table_name, table_schema FROM v_catalog.tables
                        UNION
                     SELECT table_id, table_name, table_schema FROM v_catalog.views) A
                WHERE lower(table_name) = '%(table)s'
                  AND lower(table_schema) = '%(schema)s'
                """
                % {"schema": schema.lower(), "table": table_name.lower()}
            )
        )

        c = connection.execute(get_oid_sql)
        table_oid = c.scalar()

        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """Get names of tables in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_tables_sql = sql.text(
            dedent(
                """
                SELECT table_name
                FROM v_catalog.tables
                WHERE lower(table_schema) = '%(schema)s'
                ORDER BY table_schema, table_name
                """
                % {"schema": schema.lower()}
            )
        )

        c = connection.execute(get_tables_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        """Get names of temp tables in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_tables_sql = sql.text(
            dedent(
                """
                SELECT table_name
                FROM v_catalog.tables
                WHERE lower(table_schema) = '%(schema)s'
                  AND IS_TEMP_TABLE
                ORDER BY table_name
                """
                % {"schema": schema.lower()}
            )
        )

        c = connection.execute(get_tables_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """Get names of views in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_views_sql = sql.text(
            dedent(
                """
                SELECT table_name
                FROM v_catalog.views
                WHERE lower(table_schema) = '%(schema)s'
                ORDER BY table_schema, table_name
                """
                % {"schema": schema.lower()}
            )
        )

        c = connection.execute(get_views_sql)
        return [row[0] for row in c]

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """Get definition of a view in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        view_def = sql.text(
            dedent(
                """
                SELECT VIEW_DEFINITION
                FROM V_CATALOG.VIEWS
                WHERE table_name = '%(table)s' AND table_schema = '%(schema)s'
                """
                % {"schema": schema.lower(), "table": view_name.lower()}
            )
        )

        return connection.execute(view_def).scalar()

    # Vertica does not support global temporary views.
    @reflection.cache
    def get_temp_view_names(self, connection, schema=None, **kw):
        """Get names of temp views in a schema."""
        return []

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        """Get unique constrains of a table in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_constraints_sql = sql.text(
            dedent(
                """
                SELECT constraint_name
                  , listagg(column_name using parameters max_length=65000)
                FROM v_catalog.constraint_columns
                WHERE table_name = '%(table)s' AND table_schema = '%(schema)s'
                AND constraint_type IN ('p', 'u')
                GROUP BY 1
                """
                % {"schema": schema.lower(), "table": table_name.lower()}
            )
        )
        c = connection.execute(get_constraints_sql)
        return [
            {"name": name, "column_names": cols.split(",")}
            for name, cols in c.fetchall()
        ]

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        """Get checks of a table in a schema."""
        table_oid = self._get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        constraints_sql = sql.text(
            dedent(
                """
                SELECT constraint_name, column_name
                FROM v_catalog.constraint_columns
                WHERE table_id = %(oid)s
                AND constraint_type = 'c'
                """
                % {"oid": table_oid}
            )
        )

        c = connection.execute(constraints_sql)

        return [{"name": name, "sqltext": col} for name, col in c.fetchall()]

    def normalize_name(self, name):
        """Normalize name."""
        name = name and name.rstrip()
        if name is None:
            return None
        return name.lower()

    def denormalize_name(self, name):
        """Denormalize name."""
        return name

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Get poreignn keys of a table in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_constraints_sql = sql.text(
            dedent(
                """
                SELECT constraint_name
                  , reference_table_schema
                  , reference_table_name
                  , listagg(column_name using parameters max_length=65000)
                  , listagg(reference_column_name using parameters max_length=65000)
                FROM v_catalog.constraint_columns
                WHERE table_name = '%(table)s' AND table_schema = '%(schema)s'
                  AND constraint_type = 'f'
                GROUP BY 1, 2, 3
                """
                % {"schema": schema.lower(), "table": table_name.lower()}
            )
        )
        c = connection.execute(get_constraints_sql)
        return [
            {
                "name": c_name,
                "referred_schema": rt_schema,
                "referred_table": rt_name,
                "constrained_columns": cols.split(","),
                "referred_columns": r_cols.split(","),
            }
            for c_name, rt_schema, rt_name, cols, r_cols in c.fetchall()
        ]

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        """Get indexes of a table in a schema."""
        return []

    # Disable index creation since that's not a thing in Vertica.
    # noinspection PyUnusedLocal
    def visit_create_index(self, create):
        """Disable index creation since that's not a thing in Vertica."""
        return None

    def _get_column_info(  # noqa: C901
        self, name, data_type, default: str, is_nullable, table_name, schema
    ):
        attype: str = re.sub(r"\(.*\)", "", data_type)

        charlen = re.search(r"\(([\d,]+)\)", data_type)
        if charlen:
            charlen = charlen.group(1)  # type: ignore
        args = re.search(r"\((.*)\)", data_type)
        if args and args.group(1):
            args = tuple(re.split(r"\s*,\s*", args.group(1)))  # type: ignore
        else:
            args = ()  # type: ignore
        kwargs = {}

        if attype == "numeric":
            if charlen:
                prec, scale = charlen.split(",")  # type: ignore
                args = (int(prec), int(scale))  # type: ignore
            else:
                args = ()  # type: ignore
        elif attype == "integer":
            args = ()  # type: ignore
        elif attype in ("timestamptz", "timetz"):
            kwargs["timezone"] = True
            #     # if charlen:
            #     #     kwargs["precision"] = int(charlen)  # type: ignore
            args = ()  # type: ignore
        # elif attype in ("timestamp", "time"):
        #     kwargs["timezone"] = False
        #     # if charlen:
        #     #     kwargs["precision"] = int(charlen)  # type: ignore
        #     args = ()  # type: ignore
        # elif attype.startswith("interval"):
        #     field_match = re.match(r"interval (.+)", attype, re.I)
        #     # if charlen:
        #     #     kwargs["precision"] = int(charlen)  # type: ignore
        #     if field_match:
        #         kwargs["fields"] = field_match.group(1)  # type: ignore
        #     attype = "interval"
        #     args = ()  # type: ignore
        elif attype == "date":
            args = ()  # type: ignore
        elif charlen:
            args = (int(charlen),)  # type: ignore

        coltype = self.ischema_names.get(attype.upper(), None)

        self.ischema_names["UUID"] = UUID
        self.ischema_names["TIMESTAMP"] = TIMESTAMP_WITH_PRECISION
        self.ischema_names["TIMESTAMPTZ"] = TIMESTAMP_WITH_TIMEZONE
        self.ischema_names["TIMETZ"] = TIME_WITH_TIMEZONE

        if coltype:
            coltype = coltype(*args, **kwargs)  # type: ignore
        else:
            util.warn("Did not recognize type '%s' of column '%s'" % (attype, name))
            coltype = sqltypes.NULLTYPE
        # adjust the default value
        autoincrement = False
        if default is not None:
            match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
            if match is not None:
                if issubclass(coltype._type_affinity, sqltypes.Integer):  # type: ignore
                    autoincrement = True

                # the default is related to a Sequence
                sch = schema
                if "." not in match.group(2) and sch is not None:
                    # unconditionally quote the schema name.  this could
                    # later be enhanced to obey quoting rules /
                    # "quote schema"
                    default = (
                        match.group(1)
                        + ('"%s"' % sch)
                        + "."
                        + match.group(2)
                        + match.group(3)
                    )

        column_info = dict(
            name=name,
            type=coltype,
            nullable=is_nullable,
            default=default,
            autoincrement=autoincrement,
            table_name=table_name,
            comment=str(default),
        )
        return column_info

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Get primary keye of a table in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        spk = sql.text(
            dedent(
                """
                SELECT constraint_name
                  , listagg(column_name using parameters max_length=65000)
                FROM v_catalog.primary_keys
                WHERE lower(table_schema) = '%(schema)s'
                  AND lower(table_name) = '%(table)s'
                GROUP BY 1
                """
                % {"schema": schema.lower(), "table": table_name.lower()}
            )
        )

        for row in connection.execute(spk):
            cname = row[0]
            columns = row[1]
            return {"name": cname, "constrained_columns": columns.split(",")}

        return None

    def get_all_columns(self, connection, table, schema=None, **kw):
        """Get all columns of a table in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        s = sql.text(
            dedent(
                """
                SELECT column_name, data_type, '' as column_default
                  , true as is_nullable, lower(table_name) as table_name
                FROM v_catalog.columns
                WHERE lower(table_schema) = '%(schema)s'
                  AND lower(table_name) = '%(table)s'
                UNION ALL
                SELECT column_name, data_type, '' as column_default
                  , true as is_nullable, lower(table_name) as table_name
                FROM v_catalog.view_columns
                WHERE lower(table_schema) = '%(schema)s'
                  AND lower(table_name) = '%(table)s'
                """
                % {"schema": schema.lower(), "table": table.lower()}
            )
        )

        columns = []
        for row in connection.execute(s):
            name = row[0]
            dtype = row[1].lower()
            default = row[2]
            nullable = row[3]
            table_name = row[4].lower()
            column_info = self._get_column_info(
                name, dtype, default, nullable, table_name, schema
            )
            columns.append(column_info)
        return columns

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Get all columns of a table in a schema."""
        return self.get_all_columns(connection, table_name, schema)

    def get_view_columns(self, connection, view, schema=None, **kw):
        """Get columns of views in a schema."""
        if schema is None:
            schema = self._get_default_schema_name(connection)

        s = sql.text(
            dedent(
                """
                SELECT column_name, data_type, '' as column_default
                  , true as is_nullable,lower(table_name) as table_name
                FROM v_catalog.view_columns
                WHERE lower(table_schema) = '%(schema)s'
                  AND lower(table_name) = '%(table)s'
                """
                % {"schema": schema.lower(), "table": view.lower()}
            )
        )

        columns = []

        for row in connection.execute(s):
            name = row[0]
            dtype = row[1].lower()
            default = row[2]
            nullable = row[3]
            table_name = row[4].lower()

            column_info = self._get_column_info(
                name, dtype, default, nullable, table_name, schema
            )
            # print(column_info)
            # column_info.update({'primary_key': primary_key})
            columns.append(column_info)

        return columns

    def get_view_comment(self, connection, view, schema=None, **kw):
        """Get comment of view."""
        sct = sql.text(
            dedent(
                """
                SELECT nvl(comment, table_name) as column_comment
                FROM v_catalog.views t
                  LEFT JOIN v_internal.vs_comments c ON t.table_id = c.objectoid
                WHERE lower(table_name) = '%(table)s'
                  AND lower(table_schema) = '%(schema)s'
                """
                % {"schema": schema.lower(), "table": view.lower()}
            )
        )
        return {"text": connection.execute(sct).scalar()}
