from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from collections import UserDict
import duckdb 
from pathlib import Path
from typing import Any, Dict, List
import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


BASE_DIR = Path('~').expanduser().resolve(True)/"schwabdev"
SQLITE_PATH = str((BASE_DIR/"data/metadata.sqlite").resolve())
DATA_PATH = str((BASE_DIR/"data/ducklake").resolve())


class DuckConfigs(UserDict):
    """A clean dictionary wrapper that gives you dot-notation for global configs."""
    def __getattr__(self, key: str) -> Any:
        if key in self.data:
            return self.data[key]
        raise AttributeError(f"'NamedDict' object has no attribute '{key}'")

    def __setattr__(self, key: str, value: Any):
        if key == "data":
            super().__setattr__(key, value)
        else:
            self.data[key] = value



class DuckEngine:
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        
        # Pure, vanilla Python dictionary
        self.configs = DuckConfigs({
            "extensions": ["parquet", "ducklake", "sqlite"],
            "pragmas": {
                "wal_autocheckpoint_entries": "2000"
                },
            }
        )

    def apply_configs(self) -> None:
        for ext in self.configs.extensions:
            self.conn.execute(f"INSTALL {ext};")
            self.conn.execute(f"LOAD {ext};")

        for k,v in self.configs.pragmas.items():
            self.conn.execute(f"PRAGMA {k} = {v};")

    def use_db(self,catalog:str) -> None:
        self.conn.execute(f"USE {catalog}")  

    
    def info_schema(self,catalog:str):
        return self.conn.sql(f"""
            SELECT table_catalog, table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_catalog='{catalog}'""")

    
    def attach_duck_lake(self, read_only: bool = False) -> None:
        """Example of Duck Lake Params needed for DuckEngine.configs

            {
            catalog: {
                name: ...,
                sqlite_path: ...,
                data_path: ... 
                }

        """
        cfg = self._get_valid_config("catalog", ("name", "sqlite_path", "data_path"))
        if not cfg:
            return

        name, sqlite_p, data_p = cfg["name"], cfg["sqlite_path"], cfg["data_path"]

        # Formulate DuckLake custom extension connection string
        attach_sql = f"""
            ATTACH 'ducklake:{sqlite_p}' AS "{name}" (
                DATA_PATH '{data_p}'
                ,META_TYPE 'sqlite'
                ,META_JOURNAL_MODE 'WAL'
                ,META_BUSY_TIMEOUT 500
                ,OVERRIDE_DATA_PATH true
                ,READ_ONLY {str(read_only).lower()} 
            );
        """
        self._run_attachment_pipeline(name, attach_sql, env_label="Data Lake")


    def attach_local_db(self, read_only: bool = False) -> None:
        """Orchestrates validation and mounting for a persistent local database file."""
        cfg = self._get_valid_config("local_db", ("alias", "db_path"))
        if not cfg:
            return

        alias, db_path = cfg["alias"], cfg["db_path"]

        # Ensure parent storage directories physically exist on the machine
        path = Path(db_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Formulate native DuckDB file attachment string
        attach_sql = f"ATTACH '{path}' AS \"{alias}\" (READ_ONLY {str(read_only).upper()});"
        self._run_attachment_pipeline(alias, attach_sql, env_label="Local Storage")


    def _run_attachment_pipeline(self, name: str, attach_sql: str, env_label: str) -> None:
        """Shared template workflow that controls the lifecycle of an attachment action."""
        if self._is_catalog_attached(name):
            logger.info("%s namespace '%s' is already active. Skipping.", env_label, name)
            return

        if self._execute_attach(name, attach_sql):
            self._apply_pragmas()
            self._log_mount_success(name, env_label)


    def _get_valid_config(self, block_name: str, required_keys: Tuple[str, ...]) -> Optional[Dict[str, str]]:
        """Generic Validator: Fetches and ensures a target configuration dictionary is sound."""
        # Handles both NamedDict attribute access or dictionary get access uniformly
        cfg_block = getattr(self.configs, block_name, None) if hasattr(self.configs, block_name) else self.configs.get(block_name)
        
        if not cfg_block or not all(k in cfg_block for k in required_keys):
            logger.error(
                "Missing required parameters in engine configs['%s']! Expected keys: %s",
                block_name, required_keys
            )
            return None
        return cfg_block


    def _is_catalog_attached(self, name: str) -> bool:
        """Generic Inspector: Probes the running session for active database namespaces."""
        check_qry = f"SELECT 1 FROM duckdb_databases() WHERE database_name = '{name}';"
        try:
            result = self.conn.execute(check_qry).df()
            return not result.empty
        except Exception:
            return False


    def _execute_attach(self, name: str, attach_sql: str) -> bool:
        """Generic Attacher: Executes the target database mounting SQL query strings."""
        try:
            self.conn.execute(attach_sql)
            return True
        except Exception:
            logger.exception("Failed to execute database attachment for workspace namespace: '%s'", name)
            return False


    def _apply_pragmas(self) -> None:
        """Generic Engine Tuner: Sweeps global connection pragmas into the session context."""
        pragmas = getattr(self.configs, "pragmas", None) if hasattr(self.configs, "pragmas") else self.configs.get("pragmas")
        if pragmas and isinstance(pragmas, dict):
            for k, v in pragmas.items():
                self.conn.execute(f"PRAGMA {k} = {v};")


    def _log_mount_success(self, name: str, env_label: str) -> None:
            """Generic Reporter: Inspects and outputs formatted operational metadata reports."""
            query = f"""
                SELECT database_name as name, path, type, readonly, encrypted, options 
                FROM duckdb_databases() 
                WHERE database_name = '{name}';
            """
            records = self.conn.execute(query).df().to_dict(orient='records')
            if not records:
                return

            log_block = [
                f"Successfully mounted {env_label}: '{name}'",
                "—" * 65
            ]
            for key, val in records[0].items():
                if not isinstance(val, dict):
                    log_block.append(f"  {key:<12} : {val}")
                else:
                    log_block.append(f"  {key:<12} :")
                    for sub_key, sub_val in val.items():
                        log_block.append(f"    ↳ {sub_key:<24} = {sub_val}")
            log_block.append("—" * 65)
            
            logger.info("\n" + "\n".join(log_block))
        

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
    engine:DuckEngine
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
        self.engine.conn.execute(query)

    def create_materialized_view(self, base_table: str, view_name: str, dry_run:bool=True):
        query = f"""
            CREATE OR REPLACE MATERIALIZED VIEW {view_name} AS
            {self._core_select(base_table)}
        """
        if not dry_run:
            self.engine.conn.execute(query)
        else:
            return query

    def create_view(self, base_table: str, view_name: str, dry_run:bool = True):
        query = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            {self._core_select(base_table)}
        """
        if not dry_run:
            self.engine.conn.execute(query)
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
                self.engine.conn.execute(query)
            else:
                return query


