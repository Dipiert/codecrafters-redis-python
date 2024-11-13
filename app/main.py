import asyncio 
import logging

logging.basicConfig(level=logging.DEBUG)

class RESPParser:
        
    SEPARATOR = "\r\n"
    
    def handle_command(self, cmd):
        cmd = cmd.decode("utf-8")
        logging.debug("CMD as string: %s", cmd)
        cmd = cmd.split(self.SEPARATOR)
        if cmd[0][0] == '*':            
            resp = self._handle_array(cmd)
            
        return resp.encode('utf-8')
    
    def _handle_echo(self, to_echo):
        return '$' + str(len(to_echo)) + self.SEPARATOR + to_echo + self.SEPARATOR
        
    def _handle_ping(self):
        return "+PONG" + self.SEPARATOR 
    
    def _handle_array(self, cmd):
        logging.debug("Handling array %s", cmd)
        array_len = int(cmd[0][1])
        logging.debug("As per CMD, array length is: %s", array_len)
        # array_len + 3 because: we don't care about the 2 first elements (array length and length of the first actual cmd)
        # and also range() is exclusive and we care about the last element
        # step is 2 so we skip the byte lengths
        to_run = []
        for i in range(2, array_len+3, 2): 
            to_run.append(cmd[i])
        if to_run[0] == 'ECHO':
            return self._handle_echo(to_run[1])
        if to_run[0] == 'PING':
            return self._handle_ping() 

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