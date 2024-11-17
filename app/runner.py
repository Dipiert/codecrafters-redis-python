import logging
from app.storage_manager import StorageManager

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