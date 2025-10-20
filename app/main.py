import socket  # noqa: F401
import threading
import argparse
from app.resp import resp_parser, resp_encoder, simple_string_encoder, error_encoder, array_encoder
from app.utils import getter, setter, rpush, lrange, lpush, llen, lpop, blpop, type_getter_lists, increment
from app.utils2 import xadd, type_getter_streams, xrange, xread, blocks_xread
from app.classes import Master, Slave

blocked = {}
blocked_xread = {}
queue = []

# def cmd_executor(decoded_data, connection, config, queued, executing):
#     # PING
#     if queued and decoded_data[0] != "EXEC" and decoded_data[0] != "DISCARD":
#         # append to last queue 
#         queue[len(queue)-1].append(decoded_data)
#         response = simple_string_encoder("QUEUED")
#         # if queued:
#         #     return response, queued
#         connection.sendall(response)
#         return [], queued
#     elif decoded_data[0] == "PING":
#         response = simple_string_encoder("PONG")
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [],queued
#     # ECHO
#     elif decoded_data[0].upper() == "ECHO" and len(decoded_data) > 1:
#         response = resp_encoder(decoded_data[1])
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # GET
#     elif decoded_data[0].upper() == "GET":
#         response = resp_encoder(getter(decoded_data[1]))
#         print(f"GET response: {response}")
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [],queued
#     # SET
#     elif decoded_data[0].upper() == "SET" and len(decoded_data) > 2:
#         print(f"SET data: {decoded_data}")
#         setter(decoded_data[1:])
#         response = "+OK\r\n".encode()
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
    
#     ############################## LISTS ##############################

#     # RPUSH
#     elif decoded_data[0].upper() == "RPUSH" and len(decoded_data) > 2:
#         # For simplicity, we treat RPUSH similar to SET in this implementation
#         size = rpush(decoded_data[1:], blocked)
#         response = resp_encoder(size)
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # LRANGE
#     elif decoded_data[0].upper() == "LRANGE" and len(decoded_data) > 3:
#         response = resp_encoder(lrange(decoded_data[1:]))
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # LPUSH
#     elif decoded_data[0].upper() == "LPUSH":
#         size = lpush(decoded_data[1:])
#         response = resp_encoder(size)
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # LLEN
#     elif decoded_data[0].upper() == "LLEN" and len(decoded_data) > 1:
#         size = llen(decoded_data[1])
#         response = resp_encoder(size)
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # LPOP
#     elif decoded_data[0].upper() == "LPOP" and len(decoded_data) > 1:
#         response = resp_encoder(lpop(decoded_data[1:]))
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [],queued
#     # BLPOP
#     elif decoded_data[0].upper() == "BLPOP" and len(decoded_data) > 2:
#         response = blpop(decoded_data[1:], connection, blocked)
#         if response is None:
#             return [], queued
#         else:
#             response = resp_encoder(response)
#             if executing:
#                 return response, queued
#             connection.sendall(response)
#         return [], queued
    
#     ############################## STREAMS ##############################

#     # TYPE
#     elif decoded_data[0].upper() == "TYPE" and len(decoded_data) > 1:
#         response = type_getter_lists(decoded_data[1])
#         print(f"TYPE response: {response}")
#         if response == "none": 
#             response2 = simple_string_encoder(type_getter_streams(decoded_data[1]))
#             if executing:
#                 return response2, queued
#             connection.sendall(response2)
#         else:
#             response = simple_string_encoder(response)
#             if executing:
#                 return response, queued
#             connection.sendall(response)
#         return [], queued

#     # XADD 
#     elif decoded_data[0].upper() == "XADD" and len(decoded_data) > 4:      
#         result = xadd(decoded_data[1:], blocked_xread)
#         print(f"XADD result: {result}")
#         if result[0] == "id":
#             response = resp_encoder(result[1])
#             if executing:
#                 return response, queued
#             connection.sendall(response)
#         else:   
#             response = error_encoder(result[1])
#             if executing:
#                 return response, queued
#             connection.sendall(response)
#         return [], queued
#     # XRANGE
#     elif decoded_data[0].upper() == "XRANGE" and len(decoded_data) >= 4:
#         # print(f"XRANGE called with data: {decoded_data}")
#         result = xrange(decoded_data[1:])
#         response = resp_encoder(result)
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # XREAD STREAMS
#     elif decoded_data[0].upper() == "XREAD" and len(decoded_data) >= 4:
#         if decoded_data[1].upper() == "BLOCK":
#             result = blocks_xread(decoded_data[2:], connection, blocked_xread)
#             if result is None:
#                 return [],queued
#             # response = resp_encoder(result)
#         else: 
#             result = xread(decoded_data[2:])
#             response = resp_encoder(result)
#             if executing:
#                 return response, queued
#             connection.sendall(response)
        
#         return [], queued
    
#     ########################## TRANSACTION ############################

#     # INCR
#     elif decoded_data[0].upper() == "INCR" and len(decoded_data) > 1:
#         response = increment(decoded_data[1])
#         if response == -1:
#             response = error_encoder("ERR value is not an integer or out of range")
#         else:
#             response = resp_encoder(response)
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued
#     # MULTI
#     elif decoded_data[0].upper() == 'MULTI':
#         # queue.append(decoded_data)
#         queued = True
#         queue.append([])
#         response = simple_string_encoder("OK")
#         connection.sendall(response)
#         return [], queued
#     # EXEC
#     elif decoded_data[0].upper() == 'EXEC':
#         if queued == True:
#             queued = False
#             print(f"EXEC queue: {queue}")
#             if len(queue) == 0:
#                 response = resp_encoder([])
#                 # print(f"EXEC response: {response}")
#                 connection.sendall(response)
#                 return [], queued
#                 # continue
#             else:
#                 executing = True
#                 result = []
#                 q = queue.pop(0)
#                 for cmd in q:
#                     print(f"EXEC cmd: {cmd}")
#                     output, queued = cmd_executor(cmd, connection, queued, executing)
#                     result.append(output)
#                     print(f"EXEC result: {result}")
#                 # queue.clear()
#                 executing = False
#                 response = array_encoder(result)
#                 connection.sendall(response)
#                 return [], queued
#         else:
#             response = error_encoder("ERR EXEC without MULTI")
#             connection.sendall(response)
#             return [],queued
#     # DISCARD
#     elif decoded_data[0].upper() == 'DISCARD':
#         if queued == True:
#             queued = False
#             queue.clear()
#             response = simple_string_encoder("OK")
#             connection.sendall(response)
#             return [], queued
#         else:
#             response = error_encoder("ERR DISCARD without MULTI")
#             connection.sendall(response)
#             return [],queued

#     ############################### REPLCATION ##############################

#     # INFO
#     elif decoded_data[0].upper() == "INFO":
#         response = resp_encoder(config['role'])
#         if executing:
#             return response, queued
#         connection.sendall(response)
#         return [], queued

#     # ERR
#     else:
#         response = error_encoder("ERR")
#         connection.sendall(response)
#         return [], queued

# def handle_client(connection, config):
#     queued = False
#     executing = False
#     with connection:
#         while True:
#             data = connection.recv(1024)
#             if not data:
#                 break
#             print(f"Received data: {data}")

#             decoded_data = resp_parser(data)
#             _, queued = cmd_executor(decoded_data, connection, config, queued, executing)

def main(args):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    if args.replicaof:
        runner = Slave(args)
    else:
        runner = Master(args)     

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", type=str, default="")
    args = parser.parse_args()
    main(args)
