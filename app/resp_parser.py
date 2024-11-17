import logging

from app.runner import Runner
from app.constants import Constants

logging.basicConfig(level=logging.DEBUG)

class RESPParser:
        
    runner = Runner()
    
    def handle_command(self, cmd):
        resp = ""
        logging.debug("Raw command: %s", cmd)
        cmd = cmd.decode("utf-8")
        logging.debug("CMD as string: %s", cmd)
        cmd = cmd.split(Constants.SEPARATOR.value)
        
        if cmd[0][0] == Constants.ARRAY_FIRST_BYTE.value:            
            resp = self._handle_array(cmd)
        
        if resp:                     
            resp += Constants.SEPARATOR.value
            
        return resp.encode('utf-8')
    
    def _bulk_string(self, list_to_bulk):
        res = []
        
        logging.debug("Bulk string-ing: %s", list_to_bulk)
        
        if list_to_bulk == [None]:
            return '$-1' # null bulk string
        
        if len(list_to_bulk) > 1:
            res.append(Constants.ARRAY_FIRST_BYTE.value + str(len(list_to_bulk)))
        
        for i in list_to_bulk:
            res.append('$' + str(len(i)) + Constants.SEPARATOR.value + i)
        
        return f'{Constants.SEPARATOR.value}'.join(res)
        
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
        if action == 'CONFIG':
            if to_run[1] == 'GET':
                got = self.runner.run_config_get(to_run[2])
                return self._bulk_string(got)
        
        logging.warning("Unknown command: %s", to_run)
        return ""