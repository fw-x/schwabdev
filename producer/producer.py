from redis.retry import Retry
from redis.backoff import ExponentialBackoff
import redis
import logging
import schwabdev
import orjson
from config.config import Settings
import threading
import sys
from utils.utils import OptionChainBuilder


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RETRY_STRATEGY = Retry(ExponentialBackoff(cap=2,base=0.1),3)
REDIS_CLIENT = redis.Redis(
      host="redis",
      port=6379,
      retry=RETRY_STRATEGY,
      retry_on_timeout=True
      )

settings = Settings()   # type: ignore
STREAM_NAME = "SCHWAB"


def schwab_stream_subscriber(streamer:schwabdev.Stream, request):

    service_method_map = {
        "LEVELONE_EQUITIES":streamer.level_one_equities,
        "LEVELONE_OPTIONS":streamer.level_one_options,
        "LEVELONE_FUTURES":streamer.level_one_futures,
        "LEVELONE_FUTURES_OPTIONS":streamer.level_one_futures_options
    }

    for req in request:
        service = req.get("service")
        tickers = req.get("keys", [])
        
        if not tickers:
            continue

        stream_method = service_method_map.get(service)

        if stream_method:
            total_fields = len(schwabdev.stream_fields.get(service, {}))
            field_str = ",".join(str(i) for i in range(total_fields))

            streamer.send(stream_method(tickers,field_str))
            logger.info(f"Subscribed to {service} for tickers: {tickers}")
        else:
            logger.warning(f"Unknown or Unsupported service type {service}")



class SchwabHandler:

    def __init__(self, redis_client:redis.Redis):
          self.redis_client = redis_client

    def stream_handler(self, raw_payload:str):
        try:
            payload = orjson.loads(raw_payload)
            data_list = payload.get('data')
            
            if not data_list:
                return
            pipe = self.redis_client.pipeline(transaction=False)
            cmd_queue = 0


            for msg in data_list:
                msg_headers = {
                    'service':msg.get('service'),
                    'timestamp':msg.get('timestamp'),
                    'command':msg.get('command')
                    }
                
                for item in msg.get('content',[]):
                    flattened = {**msg_headers,**item}
                    symbol = flattened.get('key')
                    

                    if not symbol:
                        continue
            
                    pipe.xadd(
                        STREAM_NAME,
                        {
                            "symbol":symbol,
                            "payload":orjson.dumps(flattened)
                        }
                    )
                    cmd_queue += 1
            if cmd_queue > 0:
                            pipe.execute()

        except orjson.JSONDecodeError:
                    logger.error("Failed to decode raw_payload string into JSON.")
        except redis.RedisError as re:
                    logger.error(f"Redis error encountered: {re}")
        except Exception as e:
                    logger.error(f"Unexpected error in stream_flattener: {e}", exc_info=True)

if __name__ == '__main__':

    logger.info("Starting Producer...")

    # TODO: Need to Paramertize the Below to make it more dynamic
    spy_chain = OptionChainBuilder(strike_step=1).option_list_generator('SPY',759.57)
    spx_chain = OptionChainBuilder(strike_step=5).option_list_generator('SPXW',7609.78)

    client = schwabdev.Client(
          app_key=settings.schwab_key, 
          app_secret=settings.schwab_secrete,
          tokens_db=settings.token_db 
        )
    # client.update_tokens(True, True)
    streamer = schwabdev.Stream(client)

    req = [{
        "service": "LEVELONE_OPTIONS",
        "keys": spx_chain + spy_chain
            }]
    

    schwab_handler = SchwabHandler(REDIS_CLIENT).stream_handler
    schwab_stream_subscriber(streamer,req)
    streamer.start(schwab_handler)

    shutdown_event = threading.Event()

    try:
        print("Stream running... Press Ctrl+C to stop.")
        shutdown_event.wait()  # Efficiently blocks main thread without burning CPU
    except KeyboardInterrupt:
        streamer.stop() 
        print("\nStopping Stream...")
        sys.exit(0)

