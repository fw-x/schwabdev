import schwabdev
from datetime import datetime, timedelta
import json
from typing import Iterable, Union
import itertools


class OptionChainBuilder:

    def __init__(self, strike_step: int = 5, lbound: float = 0.95, ubound: float = 1.05):
        self.strike_step = strike_step
        self.lbound = lbound
        self.ubound = ubound

    def _root_padder(self, symbol: str) -> str:
        return symbol.upper().ljust(6)
    
    def _round_to_step(self, x: float, direction: str = 'floor') -> int:
        # Internal math safely uses the instance defaults
        if direction == 'ceil':
            return int((x + self.strike_step - 0.001) // self.strike_step) * self.strike_step
        return int(x // self.strike_step) * self.strike_step
    
    def _strike_range(self, spot: float) -> range:
        _start = self._round_to_step(spot * self.lbound, 'floor')
        _end = self._round_to_step(spot * self.ubound, 'ceil')
        return range(_start, _end + self.strike_step, self.strike_step)
    
    def _strike_padder(self, spot: float) -> list[str]:
        return [str(strike * 1000).zfill(8) for strike in self._strike_range(spot)]

    def _normalize_iterable(self, data: Union[str, Iterable[str]], arg_name: str) -> list[str]:
            if isinstance(data, str):
                return [item.strip() for item in data.split(',')]
            if isinstance(data, (list, tuple, set)):
                return [str(item).strip() for item in data]
            raise TypeError(f"'{arg_name}' must be a comma-separated string, list, or tuple.")
        
    def _expiry_date_calc(self, days_to_expiration: int) -> str:
            now = datetime.now()
            dte = now + timedelta(days=days_to_expiration)
            return dte.strftime('%y%m%d')

    def option_list_generator(self, root: str, spot: float, sides: str = 'C,P', dte: int = 0) -> list[str]:
            fmtd_root = [self._root_padder(root)]
            expiry = [self._expiry_date_calc(dte)] 
            fmtd_sides = self._normalize_iterable(sides, "sides")
            fmtd_strikes = self._strike_padder(spot)

            return [''.join(pairs) for pairs in itertools.product(fmtd_root, expiry, fmtd_sides, fmtd_strikes)]



def translate_data(response) -> list[str]:
    """
    Translate field numbers to field names

    Returns:
        list[str]: list of field names
    """
    for item in response.get("data", []):
        if isinstance(item, dict):
            service = item.get("service", None)
            timestamp = item.get("timestamp", None)
            content = item.get("content", None)
            if timestamp:
                item["timestamp"] = datetime.strftime(datetime.fromtimestamp(timestamp / 1000.0),'%Y_%m_%d, %H:%M:%S.%f')

            if service and content and service.startswith("LEVELONE_"):
                if isinstance(content, list):
                    for quote in content:
                        for field, _ in quote.copy().items():
                            if field.isdigit():
                                new_field = translate_field(service, field)
                                quote[new_field] = quote.pop(field)                           
                                      
    return response
    
    
def translate_field(service: str, field: str|int) -> str:
    """
    Translate field number to field name

    Args:
        field (str|int): field number
    Returns:
        str: field name
    """
    mapping = schwabdev.stream_fields.get(service.upper(), None)
    if mapping is None:
        return format_field_str(str(field))
    try:
        if isinstance(mapping, dict):
            return format_field_str(mapping.get(field, str(field)))
        elif isinstance(mapping, list):
            index = int(field)
            if 0 <= index < len(mapping):
                return format_field_str(mapping[index])
            else:
                return format_field_str(str(field))
        else:
            return format_field_str(str(field))
    except Exception:
        return format_field_str(str(field))
    
def format_field_str(field:str) ->str:
    return field.replace(' ','_').strip().upper()




def serialize_for_redis(record: dict) -> tuple[str, dict, str]:
        """
        Prepares a flattened record for Redis operations.
        Returns: (redis_key, hash_mapping, json_pubsub_payload)
        """
        symbol = record["symbol"]
        redis_key = f"ticker:{symbol}"
        
        # Redis hashes store strings, so ensure all values are stringified
        # (redis-py's hset mapping handles basic types well, but good to be explicit)
        hash_mapping = {k: str(v) for k, v in record.items()}
        
        # Turn the full payload into a string for the message broker / PubSub
        pubsub_payload = json.dumps(record)
        
        return redis_key, hash_mapping, pubsub_payload
