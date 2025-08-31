from __future__ import annotations


from typing import Any, Sequence

from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem, InteractiveCatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from clickhouse_driver.dbapi import connect, Connection
from clickhouse_driver.dbapi.cursor import Cursor
from textual_fastdatatable.backend import AutoBackendType

from harlequin_clickhouse.cli_options import CLICKHOUSE_OPTIONS

# from harlequin_clickhouse.menu_items import ClickhouseDatabaseCatalogItem, ClickhouseTableCatalogItem, ClickhouseColumnCatalogItem
# from harlequin_clickhouse.short_types import get_short_type

def get_short_type(type_name: str) -> str:
    MAPPING = {
        # columns
        "UInt8": "#",
        "UInt16": "#",
        "UInt32": "#",
        "UInt64": "##",
        "UInt128": "##",
        "UInt256": "##",
        "Int8": "#",
        "Int16": "#",
        "Int32": "#",
        "Int64": "##",
        "Int128": "##",
        "Int256": "##",
        "Float32": "#.#",
        "Float64": "#.#",
        "Decimal": "#.#",
        "Boolean": "t/f",
        "String": "s",
        "FixedString": "s",
        "Date": "d",
        "Date32": "d",
        "DateTime": "ts",
        "DateTime64": "ts",
        "JSON": "{}",
        "UUID": "uid",
        "Enum": "e",
        "LowCardinality": "lc",
        "Array": "[]",
        "Map": "{}->{}",
        "SimpleAggregateFunction": "saf",
        "AggregateFunction": "af",
        "Nested": "tbl",
        "Tuple": "()",
        "Nullable": "?",
        "IPv4": "ip",
        "IPv6": "ip",
        "Point": "•",
        "Ring": "○",
        "Polygon": "▽",
        "MultiPolygon": "▽▽",
        "Expression": "expr",
        "Set": "set",
        "Nothing": "nil",
        "Interval": "|-|",
        # databases
        "Atomic": "atm",
        "Lazy": "lz",
        "Replicated": "rep",
        "PostgreSQL": "pg",
        "MySQL": "my",
        "SQLite": "sq",
        "Backup": "bak",
        "MaterializedPostgreSQL": "mpg",
        "DataLakeCatalog": "dlc",
    }
    return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")

class ClickhouseDatabaseCatalogItem(InteractiveCatalogItem):
    def fetch_children(self) -> list[ClickhouseTableCatalogItem]:

        conn: HarlequinClickHouseConnection = self.connection

        def table_fqdn(table: str) -> str:

            return '.'.join([self.qualified_identifier, table])
        
        def table_generator(tables) -> ClickhouseTableCatalogItem:
            for (table, engine) in tables:
                conn.execute(f"SELECT 'tryna add table {table} witn engine {engine}'")
                yield CatalogItem(
                    qualified_identifier=table_fqdn(table),
                    query_name=table_fqdn(table),
                    label=table,
                    type_label=get_short_type(engine),
                    # connection=self.connection,
                )
            conn.execute(f"SELECT '{self.qualified_identifier}: TableCatalog finished being formed'")
        

        
        # with conn.cursor() as cur, conn.cursor() as debug_cursor:
        conn.execute(f"SELECT '{self.qualified_identifier}: TableCatalog started being formed'")
        tables = conn.execute(
            f"SELECT name, engine FROM system.tables WHERE database = '{self.qualified_identifier}'"
        ).fetchall()


        conn.execute(f"SELECT 'Tables fetched for {self.qualified_identifier}'")
        """
        table_items: list[ClickhouseTableCatalogItem] = []
        for (table, engine) in tables:
            try:
                conn.execute(f"SELECT 'tryna add table {table} witn engine {engine}'")
                #table_item = ClickhouseTableCatalogItem(
                table_item = 
                table_items.append(table_item)
                conn.execute(f"SELECT 'added table {table} witn engine {engine}'")
            except Exception as e:
                conn.execute(f"SELECT 'cant get tables: error {str(e)}'")
                raise e
        #
        """
        return table_generator(tables)


class ClickhouseTableCatalogItem(InteractiveCatalogItem):
    def fetch_children(self):

        def column_fqdn(column):

            return '.'.join([self.qualified_identifier, column])
        
        conn: Connection = self.connection
        with conn.cursor() as cur:
            columns = cur.execute(
                f"SELECT name, type FROM system.columns WHERE CONCAT(database, '.', table) = '{self.qualified_identifier}'"
            ).fetchall()

            cur.execute(f"Columns fetched for {self.qualified_identifier}")
            # for (column, data_type) in columns:
            column_items: list[ClickhouseColumnCatalogItem] = [
                ClickhouseColumnCatalogItem(
                    qualified_identifier=column_fqdn(column),
                    query_name=column_fqdn(column),
                    label=column,
                    type_label=get_short_type(data_type),
                    connection=self.connection,
                )
                for (column, data_type) in columns
            ]
        return column_items

        


class ClickhouseColumnCatalogItem(InteractiveCatalogItem):
    pass
        


class HarlequinClickHouseCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        # names = self.cur.column_names
        # types = self.cur.column_types
        # return list(zip(names, types))
        return self.cur.columns_with_types

    def set_limit(self, limit: int) -> HarlequinClickHouseCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
    
    def fetchone(self) -> tuple | None:
        yield from self.cur.fetch_one()



class HarlequinClickHouseConnection(HarlequinConnection):


    def __init__(
        self,
        conn_str: Sequence[str],
        *args: Any,
        init_message: str = "Welcome to ClickHouse with harlequin",
        options: dict[str, Any],
    ) -> None:
        self.init_message = init_message
        self.conn_str = conn_str
        try:
            if len(conn_str) == 1:
                self.conn = connect(conn_str, **options)
            else:
                self.conn = connect(**options)
            cur = self.conn.cursor()
            cur.execute("SELECT 1")
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title="Harlequin could not connect to your ClickHouse with `clickhouse_driver`.",
            ) from e

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.conn.cursor()
            cur.execute(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur.description:
                return HarlequinClickHouseCursor(cur)
            else:
                return None

    def get_catalog_deprecated(self) -> Catalog:
        # This is a small hack to overcome the fact that clickhouse doesn't have the concept of schemas
        databases = self._list_databases()
        database_items: list[CatalogItem] = []
        for (db,) in databases:
            relations = self._list_relations_in_database(db)
            rel_items: list[CatalogItem] = []
            for rel, rel_type in relations:
                cols = self._list_columns_in_relation(db, rel)
                col_items = [
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{rel}"."{col}"',
                        query_name=f'"{col}"',
                        label=col,
                        type_label=self._get_short_type(col_type),
                    )
                    for col, col_type in cols
                ]
                rel_items.append(
                    CatalogItem(
                        qualified_identifier=f'"{db}"."{rel}"',
                        query_name=f'"{db}"."{rel}"',
                        label=rel,
                        type_label="v" if rel_type == "VIEW" else "t",
                        children=col_items,
                    )
                )
            database_items.append(
                CatalogItem(
                    qualified_identifier=f'"{db}"',
                    query_name=f'"{db}"',
                    label=db,
                    type_label="s",
                    children=rel_items,
                )
            )
        return Catalog(items=database_items)
    
    def get_catalog(self) -> Catalog:
        try:
            self.conn.cursor().execute(f"SELECT 'LD started'")
            databases = self._list_databases()
            self.conn.cursor().execute(f"SELECT 'LD finished'")
            database_items: list[ClickhouseDatabaseCatalogItem] = []
            self.conn.cursor().execute(f"SELECT 'DI initiated'")

            for (db, engine) in self._list_databases():
                self.conn.cursor().execute(f"SELECT '{db}', '{engine}'")
                database_items.append(
                    ClickhouseDatabaseCatalogItem(
                            qualified_identifier=db,
                            query_name=db,
                            label=db,
                            type_label=get_short_type(engine),
                            connection=self,
                        )
                )
            self.conn.cursor().execute(f"SELECT 'CatalogFormed'")
            return Catalog(items=database_items)
        except Exception as e:
            self.conn.cursor().execute(f"SELECT '{e}'")

    def get_completions(self) -> list[HarlequinCompletion]:
        extra_keywords = ["foo", "bar", "baz"]
        return [
            HarlequinCompletion(
                label=item,
                type_label="kw",
                value=item,
                priority=1000,
                context=None,
            )
            for item in extra_keywords
        ]

    def _list_databases(self) -> list[tuple[str]]:
        conn: Connection = self.conn
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    name, engine
                FROM system.databases
                where name not in
                    ('INFORMATION_SCHEMA', 'system', 'information_schema');
            """
            )
            results: list[tuple[str, str]] = cur.fetchall()
            #cur.execute(str(results))
        return results
    


class HarlequinClickHouseAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = CLICKHOUSE_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> HarlequinClickHouseConnection:
        conn = HarlequinClickHouseConnection(
            conn_str=self.conn_str,
            options=self.options,
        )
        return conn
