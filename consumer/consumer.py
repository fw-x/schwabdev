from dataclasses import dataclass, field
import logging
import sys
import time
import duckdb
import orjson
import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from utils.sqltools import DuckLakeEngine
from pathlib import Path
import pyarrow as pa


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logging.getLogger("redis").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


@dataclass
class RedisStreamSource:
    client: redis.Redis
    stream: str
    batch_count: int = 10_000
    block_ms:int =2000
    last_id:str = "0-0"

    def read_batch(self):
        try:
            response = self.client.xread(
                {self.stream: self.last_id},
                count=self.batch_count,
                block=self.block_ms
            )
            if not response:
                return None
            
            messages = response[0][1] # type: ignore
            if messages:
                last_msg_id = messages[-1][0]
                self.last_id = last_msg_id.decode('utf-8') if isinstance(last_msg_id, bytes) else last_msg_id
            return messages, self.last_id
        
        except redis.RedisError as e:
            logger.error(f"Redis connection dropped: {e}")
            return None


class SchwabPayloadTransformer:
    # Pre-allocate schema out of the function block for memory reuse efficiency
    STREAM_SCHEMA = pa.schema([
        ('message_id', pa.string()),
        ('timestamp', pa.timestamp('ms')), 
        ('symbol', pa.string()),
        ('payload', pa.string())           
    ])

    @classmethod
    def process_batch(cls, raw_messages: list) -> pa.Table:
        """Transforms raw buffered elements directly from Redis into a unified Arrow Table."""
        message_ids = []
        timestamps = []
        symbols = []
        payloads = []

        for msg_id_bytes, data_dict in raw_messages:
            msg_id = msg_id_bytes.decode('utf-8')
            symbol = data_dict.get(b'symbol', b'UNKNOWN').decode('utf-8')
            payload_bytes = data_dict.get(b'payload', b'{}')

            try:
                # Fast parsing directly on raw byte chunks via orjson
                parsed_json = orjson.loads(payload_bytes)
                timestamp = parsed_json.get('timestamp', None)
            except orjson.JSONDecodeError:
                timestamp = None

            message_ids.append(msg_id)
            timestamps.append(timestamp)
            symbols.append(symbol)
            payloads.append(payload_bytes.decode('utf-8'))

        return pa.Table.from_pydict({
            'message_id': message_ids,
            'timestamp': timestamps,
            'symbol': symbols,
            'payload': payloads
        }, schema=cls.STREAM_SCHEMA)



@dataclass
class DuckDBRepository:
    engine: DuckLakeEngine
    table_name: str

    def init_table(self):
        """Initializes the targeted cold storage table."""
        self.engine.conn.execute(f"""
        CREATE TABLE IF NOT EXISTS stream_tbl_new (
            message_id VARCHAR,
            service VARCHAR,
            timestamp TIMESTAMP,
            symbol VARCHAR,
            payload JSON,
            part_root VARCHAR,
            part_date VARCHAR
        );
                                 
        ALTER TABLE stream_tbl_new SET PARTITIONED BY (service, part_root, part_date);
                                 
        """)

    def save_arrow_table(self, arrow_table: pa.Table):
        """Streams the pre-allocated Arrow Table memory blocks into the storage cache."""
        if arrow_table is None or len(arrow_table) == 0:
            return
            
        # The engine queries the variable name 'arrow_table' from local scope natively
        self.engine.conn.execute(f"""
            INSERT INTO {self.table_name} 
            SELECT 
                message_id,
                (payload->>'$.service') as service,
                timestamp, 
                symbol, 
                payload::JSON,
                TRIM(SUBSTR(symbol,0,6)) as part_root,
                STRFTIME(epoch_ms((payload->>'$.timestamp')::BIGINT), '%Y_%m_%d') as part_date
            FROM arrow_table;
        """)

class FileCheckpointManager:
    dir = Path().home() / 'data'
    file_path = dir / "redis.chk"
    dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_last_id(cls) -> str:
        if not cls.file_path.exists():
            return "0-0"
        try:
            last_id = cls.file_path.read_text().strip()
            return last_id if last_id else "0-0"
        except Exception as e:
            logger.error(f"Failed to read checkpoint file: {e}")
            return "0-0"
        
    @classmethod
    def save_id(cls, last_id: str):
        try:
            temp_file = cls.file_path.with_suffix(".tmp")
            temp_file.write_text(str(last_id))
            temp_file.replace(cls.file_path)
        except Exception as e:
            logger.error(f"Failed to write checkpoint file: {e}")

@dataclass
class SchwabStreamPipeline:
    source: RedisStreamSource
    transformer: SchwabPayloadTransformer
    repository: DuckDBRepository
    checkpoint_manager: FileCheckpointManager

    # Using deque as a faster buffer allocation layer
    _buffer: list = field(default_factory=list)
    _last_flush_time: float = field(default_factory=time.time)

    _staged_checkpoint_id: str | None = None  
    _flushed_checkpoint_id: str | None = None

    def __post_init__(self): 
        self.BATCH_SIZE_LIMIT: int = self.source.batch_count       
        self.TIME_LIMIT_SECONDS: int = 30

    def run(self):
        # Initialize storage layout structures
        self.repository.init_table()
        
        self._flushed_checkpoint_id = self.checkpoint_manager.get_last_id()
        self.source.last_id = self._flushed_checkpoint_id
        logger.info(f"Resuming stream from file checkpoint: {self._flushed_checkpoint_id}")

        while True:
            try:
                raw_batch = self.source.read_batch()

                if not raw_batch or not raw_batch[0]:
                    # Market is quiet. Perform a lightweight memory flush if rows remain
                    if self._buffer:
                        logger.info("Market quiet interval reached. Executing timed flush...")
                        self._flush_to_storage()
                    
                    # Run background catalog consolidation if interval has passed
                    if (time.time() - self._last_flush_time > self.TIME_LIMIT_SECONDS):
                        self._execute_heavy_maintenance()
                    
                    time.sleep(0.1)
                    continue

                raw_messages, batch_last_id = raw_batch
                
                # Append raw elements directly to our deque collection block
                self._buffer.extend(raw_messages)
                
                self.source.last_id = batch_last_id
                self._staged_checkpoint_id = batch_last_id 

                if len(self._buffer) >= self.BATCH_SIZE_LIMIT:
                    logger.info(f"Buffer hit threshold ({len(self._buffer)} rows). Flushing...")
                    self._flush_to_storage()

            except KeyboardInterrupt:
                logger.info("Pipeline received interrupt signal. Performing final safety flush...")
                if self._buffer:
                    self._flush_to_storage()
                sys.exit()
            except Exception as e:
                logger.error(f"Error in pipeline loop: {e}", exc_info=True)
                time.sleep(1)

    def _flush_to_storage(self):
        """Atomically handles parsing and ingestion into local db structures."""
        try:
            start_time = time.time()
            row_count = len(self._buffer)
            
            arrow_table = self.transformer.process_batch(self._buffer)

            self.repository.engine.conn.execute("BEGIN TRANSACTION;")
            self.repository.save_arrow_table(arrow_table)
            self.repository.engine.conn.execute("COMMIT;")
            self._flushed_checkpoint_id = self._staged_checkpoint_id

            if self._flushed_checkpoint_id:
                self.checkpoint_manager.save_id(self._flushed_checkpoint_id)
                
            self._buffer.clear()
            logger.info(f"Appended {row_count} rows to cold storage in {time.time() - start_time:.4f}s")
            
        except Exception as e:
            logger.error(f"Failed to flush memory buffer to storage: {e}", exc_info=True)

    def _execute_heavy_maintenance(self):
        """Consolidates buffered inline catalog rows into real parquet structures on disk."""
        try:
            logger.info("Executing scheduled DuckLake compaction and WAL checkpoint optimization...")
            m_start = time.time()
            
            self.repository.engine.conn.execute(
                f"CALL ducklake_flush_inlined_data('{self.repository.engine.catalog_name}', table_name => '{self.repository.table_name}');"
            )
            self.repository.engine.conn.execute("FORCE CHECKPOINT;")
            self._last_flush_time = time.time()
            logger.info(f"DuckLake maintenance completed in {time.time() - m_start:.2f} seconds.")
        except Exception as e:
            logger.error(f"Maintenance operation failed: {e}")




def main():
    RETRY_STRATEGY = Retry(ExponentialBackoff(cap=2, base=0.1), 3)
    REDIS_CLIENT = redis.Redis(
        host="redis",
        port=6379,
        retry=RETRY_STRATEGY,
        retry_on_timeout=True
    )
    
    logger.info("Initializing producer components...")
    
    engine = DuckLakeEngine(
        conn=duckdb.connect(config={
            "home_directory": f"{Path().home() / 'data'}"
        }),
        catalog_name="schwab_lake"
    )
    engine.connect_and_mount()
    logger.info("DuckLake engine connected and mounted.")
    engine.use_db()
    logger.info("Using catalog: %s", engine.catalog_name)

    # Performance Core: Scale up row limits so 5k records don't trigger forced disk flushes instantly
    engine.conn.execute("SET ducklake_default_data_inlining_row_limit = 100_000;")

    source = RedisStreamSource(client=REDIS_CLIENT, stream="SCHWAB")
    transformer = SchwabPayloadTransformer()
    options_repo = DuckDBRepository(engine=engine, table_name="stream_tbl_new")
    checkpoint_manager = FileCheckpointManager()

    pipeline = SchwabStreamPipeline(
        source=source, 
        transformer=transformer, 
        repository=options_repo,
        checkpoint_manager=checkpoint_manager
    )

    pipeline.run()

if __name__ == "__main__":
    main()

 