import asyncio 
import logging
from enum import Enum
import datetime

# Local tests:
# ECHO
# (printf '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n';) | nc localhost 6379 # ECHO
# SET
# (printf '*3\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n';) | nc localhost 6379 # SET
# SET w/ expiry
# (printf '*5\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n';) | nc localhost 6379 
# in other terminal -> sleep 0.2 && (printf '*2\r\n$3\r\nGET\r\n$10\r\nstrawberry\r\n';) | nc localhost 6379 

logging.basicConfig(level=logging.DEBUG)
 
  
class Constants(Enum):
    
    SEPARATOR = '\r\n' 

class StorageManager:
    
    def __init__(self):
        self.storage = {}
        self.expirations = {}
    
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
    

class Runner:
    
    sm = StorageManager()
    
    def run_echo(self, to_echo):
        logging.debug("Running ECHO")
        return to_echo
    
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
        return self.sm.get(to_get)
        
        
class RESPParser:
        
    runner = Runner()
    
    def handle_command(self, cmd):
        logging.debug("Raw command: %s", cmd)
        cmd = cmd.decode("utf-8")
        logging.debug("CMD as string: %s", cmd)
        cmd = cmd.split(Constants.SEPARATOR.value)
        if cmd[0][0] == '*':            
            resp = self._handle_array(cmd)
                        
        resp += Constants.SEPARATOR.value
        return resp.encode('utf-8')
    
    def _bulk_string(self, string):
        if string is None:
            return '$-1' # null bulk string
        return '$' + str(len(string)) + Constants.SEPARATOR.value + string

    def _handle_array(self, cmd):
        logging.debug("Handling array %s", cmd)
        array_len = int(cmd[0][1]) * 2 + 1 # * 2 as every command is preceded by the amount of bytes, +1 for the Constants.SEPARATOR suffix
        logging.debug("As per CMD, array length is: %s", array_len)
        to_run = []
        for i in range(2, array_len, 2):
            to_run.append(cmd[i])
        logging.debug("CMD to run %s", to_run)
        action = to_run[0].upper()
        if action == 'ECHO':
            echo = self.runner.run_echo(to_run[1])
            return self._bulk_string(echo)
        if action == 'PING':
            return self.runner.run_ping()
        if action == 'SET':
            return self.runner.run_set(to_run[1:])
        if action == 'GET':
            got = self.runner.run_get(to_run[1])
            return self._bulk_string(got)           
            
async def redis_server(reader, writer):
    parser = RESPParser()
    while True:
        cmd = await reader.read(100)        
        if not cmd:
            break
        resp = parser.handle_command(cmd)
        writer.write(resp)
        await writer.drain()
    writer.close()

async def main(host, port):
    server = await asyncio.start_server(redis_server, host, port)
    await server.serve_forever()
    
if __name__ == '__main__':
    asyncio.run(main('localhost', 6379))