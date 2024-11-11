# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE.md file in the project root for full license information.

from typing import Any
import azure.identity.aio

from quart import Blueprint, jsonify, request, Response, render_template, current_app

import asyncio
import json, os

import os
from azure.ai.projects.aio import AIProjectClient
from azure.identity import DefaultAzureCredential

from azure.ai.projects.models import (
    MessageDeltaTextContent,
    MessageDeltaChunk,
    ThreadMessage,
    ThreadRun,
    RunStep,
    FileSearchTool,
    AsyncToolSet,
    FilePurpose,
    AgentStreamEvent
)


bp = Blueprint("chat", __name__, template_folder="templates", static_folder="static")


async def read_config(assistant_name):
    config_path = f"config/{assistant_name}_assistant_config.yaml"
    try:
        # Attempt to read the configuration file
        current_app.logger.info(f"Reading assistant configuration from {config_path}")
        with open(config_path, "r") as file:
            content = file.read()
            return content
    except FileNotFoundError as e:
        current_app.logger.error(f"Configuration file not found at {config_path}: {e}")
        return None
    except Exception as e:
        current_app.logger.error(f"An error occurred: {e}")
        return None

@bp.before_app_serving
async def configure_assistant_client():
    # config = await read_config("file_search")
    # client_args = {}
    ai_client = AIProjectClient.from_connection_string(
        credential=DefaultAzureCredential(),
        conn_str=os.environ["PROJECT_CONNECTION_STRING"],
    )

    print(f"Current dir is {os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'files', 'product_info_1.md'))}")
    
    # files = ["product_info_1.md", "product_info_2.md"]
    # file_ids =[]
    # for file in files:
    #     file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'files', file))
    #     print(f"Uploading file {file_path}")
    #     file_id = await ai_client.agents.upload_file_and_poll(file_path=file_path, purpose=FilePurpose.AGENTS)
    
    # vector_store = await ai_client.agents.create_vector_store(file_ids=file_ids, name="sample_store")

    # file_search_tool = FileSearchTool(vector_store_ids=[vector_store.id])
    
    tool_set = AsyncToolSet()
    # tool_set.add(file_search_tool)
    
    agent = await ai_client.agents.create_agent(
        model="gpt-4-1106-preview", name="my-assistant", instructions="You are helpful assistant", tools = tool_set.definitions, tool_resources=tool_set.resources
    )

    print(f"Created agent, agent ID: {agent.id}")

    
    bp.ai_client = ai_client
    bp.agent = agent
        

@bp.after_app_serving
async def shutdown_assistant_client():
    await bp.ai_client.agents.delete_agent(bp.agent.id)
    await bp.ai_client.close()

@bp.get("/")
async def index():
    return await render_template("index.html")

    
@bp.post("/chat")
async def start_chat():
    thread_id = request.cookies.get('thread_id')
    agent_id = request.cookies.get('agent_id')
    thread = None
    
    if thread_id or agent_id != bp.agent.id:
        # Check if the thread is still active
        try:
            thread = await bp.ai_client.agents.get_thread(thread_id)
        except Exception as e:
            current_app.logger.error(f"Failed to retrieve thread with ID {thread_id}: {e}")
    if thread is None:
        thread = await bp.ai_client.agents.create_thread()    
                    
    user_message = await request.get_json()
    if not hasattr(bp, 'ai_client'):
        return jsonify({"error": "Agent is not initialized"}), 500

    message = await bp.ai_client.agents.create_message(
        thread_id=thread.id, role="user", content=user_message['message']
    )
    print(f"Created message, message ID {message.id}")


    response = jsonify({"thread_id": thread.id, "message": "Processing started"})
    response.set_cookie('thread_id', thread.id)
    response.set_cookie('agent_id', bp.agent.id)
    
    
    return response, 200


@bp.route('/fetch-document', methods=['GET'])
async def fetch_document():
    filename = request.args.get('filename')
    current_app.logger.info(f"Fetching document: {filename}")
    if not filename:
        return jsonify({"error": "Filename is required"}), 400

    # Get the file path from the mapping
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'files', filename))
    
    if not file_path:
        return jsonify({"error": f"No file found for filename: {filename}"}), 404

    # Construct the full path to the file
    # full_path = os.path.join('files', file_path)
    full_path = file_path

    if not os.path.exists(full_path):
        return jsonify({"error": f"File not found: {filename}"}), 404

    try:
        # Read the file content asynchronously using asyncio.to_thread
        data = await asyncio.to_thread(read_file, full_path)
        return Response(data, content_type='text/plain')

    except Exception as e:
        return jsonify({"error": str(e)}), 500

def read_file(path):
    with open(path, 'r') as file:
        return file.read()

@bp.route('/stream', methods=['GET'])
async def stream_responses():
    # Set necessary headers for SSE
    headers = {
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'text/event-stream'
    }

    thread_id = request.cookies.get('thread_id')
    agent_id = request.cookies.get('agent_id')
    if not thread_id:
        return jsonify({"error": "Agent ID is required"}), 400

    current_app.logger.info(f"Stream request received for thread ID: {thread_id}")


    async def event_stream():
        async with await bp.ai_client.agents.create_stream(
            thread_id=thread_id, assistant_id=agent_id
        ) as stream:
            accumulated_text = ""
            
            async for event_type, event_data in stream:

                if isinstance(event_data, MessageDeltaChunk):
                    for content_part in event_data.delta.content:
                        if isinstance(content_part, MessageDeltaTextContent):
                            text_value = content_part.text.value if content_part.text else "No text"
                            accumulated_text += text_value
                            print(f"Text delta received: {text_value}")
                            event_data = json.dumps({'content': text_value, 'type': "message"})
                            yield f"data: {event_data}\n\n"

                elif isinstance(event_data, ThreadMessage):
                    print(f"ThreadMessage created. ID: {event_data.id}, Status: {event_data.status}")
                    if (event_data.status == "completed"):
                        event_data = json.dumps({'content': accumulated_text, 'type': "completed_message"})
                        yield f"data: {event_data}\n\n"

                elif isinstance(event_data, ThreadRun):
                    print(f"ThreadRun status: {event_data.status}")

                elif isinstance(event_data, RunStep):
                    print(f"RunStep type: {event_data.type}, Status: {event_data.status}")

                elif event_type == AgentStreamEvent.ERROR:
                    print(f"An error occurred. Data: {event_data}")

                elif event_type == AgentStreamEvent.DONE:
                    print("Stream completed.")
                    event_data = json.dumps({'type': "stream_end"})
                    yield f"data: {event_data}\n\n"

                else:
                    print(f"Unhandled Event Type: {event_type}, Data: {event_data}")

    return Response(event_stream(), headers=headers)
