import asyncio
from kaspr.app import app
from kaspr import KasprApp
import faust
from faust import current_event

# print("RUNNING......")
# topics = app.topic("topic-one", "topic-two", value_serializer="json")

# @app.agent(topics)
# async def processor(stream: faust.StreamT):
#     async for event in stream.events():
#         print(event)

# input_topic = app.topic("kms-input", value_serializer="json")
# repartitioned_topic = app.topic("kms-input-repartitioned")

# heartbeats = app.channel()

# async def sink_1(value):
#     event = current_event()
#     print(f"sink_1: {value} / event: {event}")
    
# async def sink_2(value):
#     event = current_event()
#     await asyncio.sleep(15)
#     print(f"sink_2: {value} / event: {event}")

# @app.agent(input_topic, sink=[sink_1, sink_2, repartitioned_topic])
# async def process_A(stream: faust.StreamT):
#     async for event in stream.events():

#         event.key = 'Akey'
#         event.value = "Avalue"
#         print(f"A - Processed: {event}")
#         yield event.value


# @app.agent(repartitioned_topic)
# async def process_B(stream: faust.StreamT):
#     async for event in stream.events():
#         print(f"B - Processed: {event}")
#         #yield event        
# app.channel

# # Terminal functions
# # .events()
# # 

# # class EventT(Generic[T], AsyncContextManager):

# #     app: _AppT
# #     key: K
# #     value: V
# #     headers: Mapping
# #     message: Message
# #     acked: bool