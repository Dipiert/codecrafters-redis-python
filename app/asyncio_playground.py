import asyncio 

async def redis_server(reader, writer):
    while True:
        data = reader.read(100)
        if not data:
            break
        writer.write(b"+PONG\r\n")
        await writer.drain()
    writer.close()

async def main():
    server = await asyncio.start_server(redis_server, 'localhost', 6376)
    await server.serve_forever()
    
if __name__ == '__main__':
    main()