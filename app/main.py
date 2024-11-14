import asyncio 
import logging
from enum import Enum

# (printf '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n';) | nc localhost 6379 # ECHO
# (printf '*3\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n';) | nc localhost 6379 # SET


logging.basicConfig(level=logging.DEBUG)
 
class Constants(Enum):
    
    SEPARATOR = '\r\n'
    
class StorageManager:
    
    def __init__(self):
        self.storage = {}
    
    def store(self, to_store):
        self.storage.update(to_store)
        logging.debug("Stored %s", to_store)
        
    def get(self, to_get):
        return self.storage.get(to_get)
    

class Runner:
    
    sm = StorageManager()
    
    def run_echo(self, to_echo):
        logging.debug("Running ECHO")
        return to_echo
    
    def run_ping(self):
        logging.debug("Running PING")
        return "+PONG"
    
    def run_set(self, to_set):
        logging.debug("Running SET")
        self.sm.store({to_set[0]: to_set[1]})
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
        return '$' + str(len(string)) + Constants.SEPARATOR.value + string

    def _handle_array(self, cmd):
        logging.debug("Handling array %s", cmd)
        array_len = int(cmd[0][1]) * 2 + 1 # * 2 as every command is preceded by the amount of bytes, +1 for the self.TERMINATOR suffix
        logging.debug("As per CMD, array length is: %s", array_len)
        to_run = []
        for i in range(2, array_len, 2):
            to_run.append(cmd[i])
        logging.debug("CMD to run %s", to_run)
        if to_run[0] == 'ECHO':
            echo = self.runner.run_echo(to_run[1])
            return self._bulk_string(echo)
        if to_run[0] == 'PING':
            return self.runner.run_ping()
        if to_run[0] == 'SET':
            return self.runner.run_set(to_run[1:])
        if to_run[0] == 'GET':
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