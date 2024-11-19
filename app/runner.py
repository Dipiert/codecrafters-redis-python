import logging
import os 
from .constants import Constants

from .storage_manager import StorageManager

logging.basicConfig(level=logging.DEBUG)

class Runner:
    
    sm = StorageManager()
    
    def run_echo(self, to_echo):
        logging.debug("Running ECHO")
        return [to_echo]
    
    def run_ping(self):
        logging.debug("Running PING")
        return "+PONG"
    
    def run_set(self, to_set):
        expires_in = None        
        
        if len(to_set) > 2 and to_set[2].upper() == 'PX':
            expires_in = int(to_set[3])
            
        logging.debug("Running SET: %s", to_set)
        self.sm.store(to_set[0], to_set[1], expires_in)
        return "+OK"
    
    def run_get(self, to_get):
        logging.debug("Getting %s", to_get)
        return [self.sm.get(to_get)]
    
    def run_config_get(self, to_get):
        print("Running CONFIG GET")
        logging.debug("Running CONFIG GET %s", to_get)
        return [to_get, self.sm.config.get(to_get)]
        
    def run_keys_all(self):
        keys = []
        dir = self.sm.config.get("dir", '.')
        filename = self.sm.config.get("dbfilename", 'dump.rdb')
        file_path = os.path.join(dir, filename)
        with open(file_path, "rb") as stream:
            logging.debug("Retrieving keys from RDB file in %s", file_path)
            f = stream.read()
            print(f)
            
            kv_ht_size_idx = f.find(Constants.RDB_KV_HASH_TABLE_SIZE_START.value)
            keys_len = f[kv_ht_size_idx + 1:kv_ht_size_idx + 2][0]
            expirations_len = f[kv_ht_size_idx + 2:kv_ht_size_idx + 3][0]
            
            logging.debug("Number of keys stored: %s", keys_len)
            logging.debug("Number of expirations stored: %s", expirations_len)
            
            logging.debug("Value's type and encoding, 0 means 'string': %s", f[kv_ht_size_idx + 3 : kv_ht_size_idx + 4][0]) 
            
            for k in range(keys_len):
                key_size = f[kv_ht_size_idx + 4 + k : kv_ht_size_idx + 5 + k][0]
                
                # working only with string values as of now:             
                key = f[kv_ht_size_idx + 5 + k : kv_ht_size_idx + 5 + k + key_size].decode('utf-8')
                
                logging.debug("Key size: %s", key_size)
                logging.debug("Key: %s", key)
                keys.append(key)
                
        return keys

