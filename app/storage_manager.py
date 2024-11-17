import logging
import datetime

logging.basicConfig(level=logging.DEBUG)

class StorageManager:
    
    def __init__(self):
        self.storage = {}
        self.expirations = {}
        self.config = {}
    
    def store(self, k, v, expires_in):
        to_store = {k: v}
        self.storage.update(to_store)
        logging.debug("Stored %s", to_store)
        if expires_in is not None:
            expiration_time = datetime.datetime.now() + datetime.timedelta(milliseconds=expires_in)
            logging.debug("Setting expiration time for '%s' at %s", k, expiration_time)
            self.expirations[k] = expiration_time
             
    def get(self, to_get):
        value = self.storage.get(to_get)
        if value:
            expiration = self.expirations.get(to_get)
            if expiration:
                if expiration > datetime.datetime.now():
                    return value
                else:
                    logging.debug("Key '%s' has expired at %s", to_get, expiration)
                    return None
            else:
                logging.debug("Expiration time wasn't set for '%s'", to_get)  
                return value
            
    def set_config(self, config):
        self.config.update(config)
        logging.debug("Redis config: %s", config)