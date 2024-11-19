import asyncio 
import logging
import argparse
from functools import partial

from app.resp_parser import RESPParser

# Local tests:
# ECHO
# Terminal 1: (printf '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n';) | nc localhost 6379 # ECHO
# SET / GET
# Terminal 1: (printf '*3\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$5\r\napple\r\n';) | nc localhost 6379 # SET
# Terminal 2: (printf '*2\r\n$3\r\nGET\r\n$10\r\nstrawberry\r\n';) | nc localhost 6379
# SET w/ expiry
# Terminal 1: (printf '*5\r\n$3\r\nSET\r\n$10\r\nstrawberry\r\n$4\r\npear\r\n$2\r\npx\r\n$3\r\n100\r\n';) | nc localhost 6379 
# Terminal 2: sleep 0.2 && (printf '*2\r\n$3\r\nGET\r\n$10\r\nstrawberry\r\n';) | nc localhost 6379 
# CONFIG GET
# Terminal 1: (printf '*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n';) | nc localhost 6379
# KEYS *
# Terminal 1: (in project directory) redis-server
# Terminal 2: redis-cli set orange mango
# Terminal 2: redis-cli save
# Terminal 1: Ctrl + C (see <dbfilename>.rdb created in <dir>)
# Terminal 1: (printf '*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n' ;) | nc localhost 6379

            
async def redis_server(reader, writer, dir=None, dbfilename=None):
    parser = RESPParser()
    parser.runner.sm.set_config({"dir": dir, "dbfilename": dbfilename})
    
    while True:
        cmd = await reader.read(100)        
        if not cmd:
            break
        resp = parser.handle_command(cmd)
        writer.write(resp)
        await writer.drain()
        
    writer.close()

async def main(host, port, dir, dbfilename):
    logging.info("Running server at %s:%s", host, port)
    server = await asyncio.start_server(partial(redis_server, dir=dir, dbfilename=dbfilename), host, port)
    await server.serve_forever()
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--dir')
    arg_parser.add_argument('--dbfilename')
    
    args = arg_parser.parse_args()
    if args.dir:
        logging.debug("Setting dir arg as %s", args.dir), 
    if args.dbfilename:
        logging.debug("Setting dbfilename arg as %s", args.dbfilename)
        
    asyncio.run(main('localhost', 6379, args.dir, args.dbfilename))