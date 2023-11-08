'''
Websocket client library
'''
import websockets
import ssl
import json
from GSSLibs import log
import logging
import asyncio

logger = logging.getLogger(__file__)

class LoggerAdapter(logging.LoggerAdapter):
    """Add connection ID and client IP address to websockets logs."""
    def process(self, msg, kwargs):
        try:
            websocket = kwargs["extra"]["websocket"]
        except KeyError:
            return msg, kwargs
        return f"{websocket.id} {msg}", kwargs

async def on_message(message):
    resp = json.loads(message)
    for webIds_values in resp['Items']:
        for value in webIds_values['Items']:
            log.info('{:<50s}'.format(f"Timestamp: {value['Timestamp']}"))
            log.info('{:<50s}'.format(f"Name: {webIds_values['Name']}"))
            log.info('{:<50s}'.format(f"Value: {value['Value']}"))
    
async def websocket_client(url, headers, message_callback=on_message): 
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with websockets.connect(url, ssl=ssl_context, logger=None, extra_headers=headers) as websocket:
        async for message in websocket:
            _ = asyncio.create_task(message_callback(message))
            

   