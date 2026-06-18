from dataclasses import dataclass, field
from typing import Dict, List
import duckdb 
from pathlib import Path


@dataclass 
class DuckLakeEngine:
    conn:duckdb.DuckDBPyConnection
    catalog_name:str
    sqlite_path:str = field(default_factory=lambda: str((Path().home()/'data/metadata.sqlite').resolve()))
    data_path:str = field(default_factory=lambda: str((Path().home()/'data/ducklake').resolve()))
    configs: dict = field(default_factory=lambda: {"extensions": ['ducklake', 'sqlite','parquet'],"pragmas":{"wal_autocheckpoint_entries":"2000"}})

    def use_db(self,use:bool=True):
        if use:
            self.conn.execute(f"USE {self.catalog_name}")

            

    def connect_and_mount(self):

        for ext in self.configs['extensions']:
            self.conn.execute(f"INSTALL {ext}")
            self.conn.execute(f"LOAD {ext}")

        try: 
            self.conn.execute(f"""
                ATTACH 'ducklake:{self.sqlite_path}' AS {self.catalog_name} (
                    DATA_PATH '{self.data_path}',
                    META_TYPE 'sqlite',
                    META_JOURNAL_MODE 'WAL',
                    META_BUSY_TIMEOUT 500,
                    OVERRIDE_DATA_PATH true
                );""")
            if self.configs.get('pragmas',{}):
                for k,v in self.configs.get('pragmas').items(): # type: ignore
                    self.conn.execute(f"PRAGMA {k} = {v};")
        except duckdb.CatalogException:
            pass

    def execute(self, query: str):
        return self.conn.execute(query)
    
    @property
    def info_schema(self):
        return self.conn.sql(f"""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog='{self.catalog_name}'""")
    

class StreamSchema:
    TYPE_MAP = {
        "String": "VARCHAR", "double": "DOUBLE", "long": "BIGINT",
        "int": "INTEGER", "char": "VARCHAR", "boolean": "BOOLEAN",
        }
        
    def __init__(self, service:str, field_map:dict):
        self.service = service
        self.raw_fields = field_map.get(service,[])
        self.columns = self._parse_fields()

    def _clean_name(self, name:str) -> str:
        return name.lower().replace(" ", "_").replace("/","_")

    def _parse_fields(self) -> List[Dict[str,str]]:
        parsed = []
        for f in self.raw_fields:
            clean_name = self._clean_name(f.get("fieldName"))
            if clean_name == 'symbol':
                continue
            parsed.append({
                "name": clean_name,
                "type": self.TYPE_MAP.get(f.get("type"), "VARCHAR"),
                "path": f.get("fieldId")
            })
        return parsed


@dataclass
class DuckBuilder:
    engine:DuckLakeEngine
    schema:StreamSchema

    def _build_columns(self) -> str:
        cols = [f"""{col.get("name"):<40}{col.get("type")}""" for col in self.schema.columns]
        return ",\n            ".join(cols)


    def _build_extractions(self) -> str:
        clauses = [
            f"TRY_CAST(payload->>'$.{col['path']}' AS {col['type']}) AS raw_{col['name']}"
            for col in self.schema.columns
        ]
        return ",\n            ".join(clauses)
        
    def _build_forward_fills(self) -> str:
            clauses = [
                f"LAST_VALUE(raw_{col['name']} IGNORE NULLS) OVER ("
                f"PARTITION BY symbol ORDER BY msg_timestamp "
                f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {col['name']}"
                for col in self.schema.columns
            ]
            return ",\n            ".join(clauses)
    
    def _core_select(self, base_table: str) -> str:

            qry = f"""
                     SELECT
                         md5(concat_ws('_', message_id, symbol, (payload->>'$.timestamp'))) as record_id,
                         message_id,
                         symbol,
                         (payload->>'$.service') AS service,
                         epoch_ms((payload->>'$.timestamp')::BIGINT) AS msg_timestamp,
                         {self._build_extractions()}
                     FROM {base_table}
                     WHERE service = '{self.schema.service}'
                     ORDER BY symbol,msg_timestamp
             """
            return qry


    def create_base_table(self, table_name: str, tmp:bool=False):
        if tmp:
            _tmp= "TEMPORARY"
        else:
            _tmp=''
        query = f"""
            CREATE {_tmp} TABLE IF NOT EXISTS {table_name} (
                record_id                                   VARCHAR,
                message_id                               VARCHAR,
                symbol                                   VARCHAR,
                service                                  VARCHAR,
                msg_timestamp                            TIMESTAMP,
                {self._build_columns()}
            )
        """
        self.engine.execute(query)

    def create_materialized_view(self, base_table: str, view_name: str, dry_run:bool=True):
        query = f"""
            CREATE OR REPLACE MATERIALIZED VIEW {view_name} AS
            {self._core_select(base_table)}
        """
        if not dry_run:
            self.engine.execute(query)
        else:
            return query

    def create_view(self, base_table: str, view_name: str, dry_run:bool = True):
        query = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            {self._core_select(base_table)}
        """
        if not dry_run:
            self.engine.execute(query)
        else:
            return query

    def insert_from_stream(self, base_tbl: str, target_tbl: str, dry_run: bool = True):
            # We wrap the core select statement into a CTE to allow for an optimized NOT EXISTS check 
            query = f"""
                INSERT INTO {target_tbl}
                WITH incoming_data AS (
                    {self._core_select(base_tbl)}
                )
                SELECT * FROM incoming_data
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM {target_tbl} AS existing
                    WHERE existing.record_id = incoming_data.record_id
                )
                ORDER BY symbol, msg_timestamp;
            """
            if not dry_run:
                self.engine.execute(query)
            else:
                return query


