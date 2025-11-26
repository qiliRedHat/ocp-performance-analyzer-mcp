#!/usr/bin/env python3
"""
OpenShift Node Analyzer MCP Client with FastAPI and LangGraph Integration
Provides AI-powered chat interface for interacting with node analysis MCP tools
"""

import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp
import traceback
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel, Field

# Add project root to sys.path for elt package imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# MCP and LangGraph imports
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# LangChain and LangGraph imports
from langchain.tools import BaseTool
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from dotenv import load_dotenv

# Fallback-safe import for HTML conversion utility
try:
    from elt.utils.analyzer_elt_json2table import convert_json_to_html_table
except ImportError:
    def convert_json_to_html_table(json_data):
        try:
            return json.dumps(json_data, indent=2) if not isinstance(json_data, str) else json_data
        except Exception:
            return str(json_data)

import warnings
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class _SuppressInvalidHttp(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "Invalid HTTP request received" not in msg

logging.getLogger("uvicorn").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.error").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.access").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.protocols.http.h11_impl").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.protocols.http.httptools_impl").addFilter(_SuppressInvalidHttp())

class ChatRequest(BaseModel):
    message: str = Field(..., description="User message to process")
    conversation_id: str = Field(default="default", description="Conversation identifier")
    system_prompt: Optional[str] = Field(default=None, description="Optional system prompt override")

class SystemPromptRequest(BaseModel):
    system_prompt: str = Field(..., description="System prompt to set")
    conversation_id: str = Field(default="default", description="Conversation identifier")

class HealthResponse(BaseModel):
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Check timestamp")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")

class MCPTool(BaseTool):
    """LangChain tool wrapper for MCP tools"""

    name: str
    description: str
    mcp_client: 'MCPClient'

    def __init__(self, name: str, description: str, mcp_client: 'MCPClient'):
        super().__init__(name=name, description=description, mcp_client=mcp_client)

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use async version")

    async def _arun(self, **kwargs) -> str:
        try:
            # Handle both direct kwargs and nested kwargs
            params = kwargs.get('kwargs', kwargs) or {}
            if isinstance(params, dict):
                # Remove None values to use tool defaults
                params = {k: v for k, v in params.items() if v is not None}

            # Wrap parameters in 'request' key as expected by MCP server
            wrapped_params = {"request": params} if params else {}

            result = await self.mcp_client.call_tool(self.name, wrapped_params)

            # If result is already a string (HTML or formatted text), return directly
            # Otherwise, JSON dump it for proper serialization
            if isinstance(result, str):
                return result
            else:
                return json.dumps(result, indent=2)
        except Exception as e:
            logger.error(f"Error calling MCP tool {self.name}: {e}")
            return f"Error: {str(e)}"

class MCPClient:
    """MCP Client for interacting with the node analyzer server"""

    def __init__(self, mcp_server_url: str = "http://localhost:8004"):
        self.mcp_server_url = mcp_server_url
        self.session = None
        self.available_tools: List[Dict[str, Any]] = []
        self.langchain_tools: List[MCPTool] = []

    async def connect(self):
        try:
            url = f"{self.mcp_server_url}/mcp"
            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    session_id = get_session_id()
                    logger.info(f"Session ID: {session_id}")
                    tools_result = await session.list_tools()
                    logger.info(f"Discovered {len(tools_result.tools)} MCP tools")

                    # Rebuild tool list idempotently to avoid duplicate growth across reconnects
                    new_available_tools: List[Dict[str, Any]] = []
                    seen_tool_names = set()

                    for tool in tools_result.tools:
                        name = getattr(tool, "name", None)
                        if not name or name in seen_tool_names:
                            continue
                        seen_tool_names.add(name)

                        logger.info(f"Tool discovered: {name}")
                        input_schema = {}
                        try:
                            if tool.inputSchema:
                                if isinstance(tool.inputSchema, dict):
                                    input_schema = tool.inputSchema
                                elif hasattr(tool.inputSchema, "model_dump"):
                                    input_schema = tool.inputSchema.model_dump()
                                elif hasattr(tool.inputSchema, "dict"):
                                    input_schema = tool.inputSchema.dict()
                        except Exception:
                            input_schema = {}

                        new_available_tools.append({
                            "name": name,
                            "description": getattr(tool, "description", ""),
                            "input_schema": input_schema
                        })

                    # Replace existing tools rather than appending to prevent growth
                    self.available_tools = new_available_tools
                    self._create_langchain_tools()
                    logger.info(f"Loaded {len(self.available_tools)} MCP tools")
            return True

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
            return False

    def _create_langchain_tools(self):
        self.langchain_tools = []
        for tool_info in self.available_tools:
            langchain_tool = MCPTool(
                name=tool_info["name"],
                description=tool_info["description"],
                mcp_client=self
            )
            self.langchain_tools.append(langchain_tool)

    async def call_tool(self, tool_name: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        try:
            url = f"{self.mcp_server_url}/mcp"
            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                try:
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        if get_session_id:
                            logger.info(f"Session ID in call_tool: {get_session_id()}")
                        logger.info(f"Calling tool {tool_name} with params {params}")

                        # Check if tool has parameters by examining input schema
                        tool_info = next((t for t in self.available_tools if t["name"] == tool_name), None)
                        has_params = False
                        if tool_info and tool_info.get("input_schema"):
                            schema = tool_info["input_schema"]
                            # Check if schema has properties (parameters)
                            if isinstance(schema, dict):
                                properties = schema.get("properties", {})
                                has_params = bool(properties and len(properties) > 0)

                        # UNIFIED PARAMETER HANDLING: Same as network and ovnk clients
                        # For tools with no parameters, send empty dict {}
                        # For tools with parameters, wrap in 'request' key if not already wrapped
                        if params is None or params == {}:
                            if has_params:
                                # Tool expects parameters but none provided, send empty request wrapper
                                request_params = {"request": {}}
                            else:
                                # Tool has no parameters, send empty dict
                                request_params = {}
                        elif "request" not in params:
                            request_params = {"request": params}
                        else:
                            request_params = params

                        result = await session.call_tool(tool_name, request_params)

                        # Enhanced error handling for JSON parsing
                        if not result.content:
                            logger.warning(f"Tool {tool_name} returned empty content")
                            return {"error": "Empty response from tool", "tool": tool_name}

                        if len(result.content) == 0:
                            logger.warning(f"Tool {tool_name} returned no content items")
                            return {"error": "No content items in response", "tool": tool_name}

                        content_text = result.content[0].text
                        if not content_text or content_text.strip() == "":
                            logger.warning(f"Tool {tool_name} returned empty text content")
                            return {"error": "Empty text content from tool", "tool": tool_name}

                        content_text = content_text.strip()

                        try:
                            if content_text[:1] in ['{', '[']:
                                json_data = json.loads(content_text)
                                logger.info(f"Parsed JSON from {tool_name}")
                                # Return health check data directly
                                if tool_name in ["get_server_health"]:
                                    return json_data
                                else:
                                    print("call_tool("+tool_name+"):",json_data)
                                    print("convert_json_to_html_table(json_data):",convert_json_to_html_table(json_data))
                                    return convert_json_to_html_table(json_data)
                            else:
                                return content_text

                        except json.JSONDecodeError as json_err:
                            logger.error(f"Failed to parse JSON from tool {tool_name}. Content: '{content_text[:200]}...'")
                            logger.error(f"JSON decode error: {json_err}")
                            if content_text.startswith('{') or content_text.startswith('['):
                                return {
                                    "error": f"Malformed JSON response: {str(json_err)}",
                                    "tool": tool_name,
                                    "raw_content": content_text[:500],
                                    "content_type": "malformed_json"
                                }
                            else:
                                return {
                                    "result": content_text,
                                    "tool": tool_name,
                                    "content_type": "text",
                                    "message": "Tool returned plain text instead of JSON"
                                }
                except Exception as e:
                    logger.error(f"MCP ClientSession error for tool {tool_name}: {e}")
                    return {
                        "error": str(e),
                        "tool": tool_name,
                        "error_type": type(e).__name__,
                    }
        except Exception as outer_e:
            logger.error(f"Error calling tool {tool_name}: {outer_e}")
            return {
                "error": str(outer_e),
                "tool": tool_name,
                "error_type": type(outer_e).__name__,
            }

    async def check_mcp_connectivity_health(self):
        try:
            url = f"{self.mcp_server_url}/mcp"
            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    session_id = get_session_id()
                    logger.info(f"Health check session ID: {session_id}")
                    if session_id:
                       return {
                        "status": "healthy",
                        "mcp_connection": "ok",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    else:
                       return {
                        "status": "unhealthy",
                        "mcp_connection": "disconnected",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
        return {
            "status": "unhealthy",
            "mcp_connection": "disconnected",
            "error": str(e),
            "last_check": datetime.now(timezone.utc).isoformat()
        }

    async def check_node_analyzer_health(self) -> Dict[str, Any]:
        try:
            await self.connect()

            health_tools = ["get_server_health"]
            health_result = None
            tool_used = None

            for tool_name in health_tools:
                if any(tool["name"] == tool_name for tool in self.available_tools):
                    try:
                        logger.info(f"Trying health check with tool: {tool_name}")
                        health_result = await self.call_tool(tool_name, {})

                        if (isinstance(health_result, dict) and
                            "error" not in health_result and
                            health_result):
                            tool_used = tool_name
                            break
                        elif isinstance(health_result, dict) and "error" in health_result:
                            logger.warning(f"Tool {tool_name} returned error: {health_result.get('error')}")
                            continue

                    except Exception as tool_error:
                        logger.warning(f"Tool {tool_name} failed: {str(tool_error)}")
                        continue

            if health_result and tool_used:
                overall_health = health_result.get("status", "unknown")
                collectors_initialized = health_result.get("collectors_initialized", False)

                return {
                    "status": "healthy" if collectors_initialized else "partial",
                    "node_analyzer_connection": "ok",
                    "tools_available": len(self.available_tools),
                    "collectors_initialized": collectors_initialized,
                    "overall_health": overall_health,
                    "tool_used": tool_used,
                    "last_check": datetime.now(timezone.utc).isoformat(),
                    "health_details": health_result
                }
            else:
                return {
                    "status": "partial",
                    "node_analyzer_connection": "error",
                    "tools_available": len(self.available_tools),
                    "tool_error": "Health check tools failed or returned empty responses",
                    "overall_health": "unknown",
                    "last_check": datetime.now(timezone.utc).isoformat()
                }

        except Exception as e:
            logger.error(f"node analyzer connectivity check failed: {e}")
            return {
                "status": "unhealthy",
                "node_analyzer_connection": "unknown",
                "error": str(e),
                "overall_health": "unknown",
                "last_check": datetime.now(timezone.utc).isoformat()
            }

class ChatBot:
    """LangGraph-powered chatbot for node analyzer MCP interaction"""

    def __init__(self, mcp_client: MCPClient):
        self.mcp_client = mcp_client
        self.memory = MemorySaver()
        self.conversations = {}
        self.conversation_system_prompts = {}

        self.default_system_prompt = """You are an expert OpenShift node performance analyst with deep knowledge of:
- Node health monitoring and diagnostics
- OpenShift cluster node troubleshooting and performance optimization
- Container runtime and kubelet performance analysis
- Pod Lifecycle Event Generator (PLEG) relist latency metrics
- Kubelet runtime operations error rates
- CPU, memory, and cgroup resource management
- Node resource pressure and eviction scenarios
- Be able to explain metrics and typical scenarios that use these metrics

When analyzing node data:
1. Focus on performance bottlenecks, resource pressure, and health issues
2. Provide actionable insights and specific recommendations
3. Explain technical findings in clear, structured responses
4. Use the available MCP tools to gather comprehensive analysis
5. Correlate multiple metrics to identify root causes from CPU, memory, disk I/O, network I/O, PLEG latency, and kubelet runtime operations errors
6. Prioritize critical issues that affect node stability and pod scheduling

Always structure your responses with:
- Explain metrics and typical scenarios that use these metrics
- Executive summary of findings
- Detailed technical analysis
- Specific recommendations with priority levels (use bullet points, NOT markdown tables)
- Next steps for investigation or remediation, always provide suggested tuning methods, highlight with bold and green words
- Highlight critical issues or values higher than threshold via red characters/words, using bold green or bold orange, or bold purple and other colors to distinguish different info/warning/status
- If no specific threshold value is provided, use industry standards
- Make the analysis result readable and clear

**IMPORTANT FORMATTING RULES:**
- Use markdown formatting (the UI converts markdown to HTML)
- Use ### for main section headings, #### for subsections
- Use bullet lists (- or *) with **bold** for emphasis
- NEVER use markdown tables (| column | format) - they look bad in the UI
- Format recommendations as markdown bullet lists with emojis for priority:
  ### Recommendations
  - 🔴 **High Priority**: Critical issue requiring immediate action
  - 🟠 **Medium Priority**: Important improvement to implement soon
  - 🟢 **Low Priority**: Optional enhancement for consideration
- Use **bold** for emphasis and `code blocks` for commands or values
- Use > for important callouts or warnings

Be thorough but concise, and always explain the business impact of technical issues."""

        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BASE_URL")

        self.llm = ChatOpenAI(
            model="gemini-2.5-pro",
            base_url=base_url,
            api_key=api_key,
            temperature=0.1,
            streaming=True
        )

    def set_system_prompt(self, conversation_id: str, system_prompt: str):
        self.conversation_system_prompts[conversation_id] = system_prompt
        if conversation_id in self.conversations:
            del self.conversations[conversation_id]

    def get_system_prompt(self, conversation_id: str) -> str:
        return self.conversation_system_prompts.get(conversation_id, self.default_system_prompt)

    def get_or_create_agent(self, conversation_id: str):
        if conversation_id not in self.conversations:
            agent = create_react_agent(
                self.llm,
                self.mcp_client.langchain_tools,
                checkpointer=self.memory
            )
            self.conversations[conversation_id] = agent

        return self.conversations[conversation_id]

    async def chat_stream(self, message: str, conversation_id: str, system_prompt: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream chat response with proper message streaming"""
        try:
            if system_prompt:
                self.set_system_prompt(conversation_id, system_prompt)

            agent = self.get_or_create_agent(conversation_id)
            config = {"configurable": {"thread_id": conversation_id}}

            tool_results_sent = set()
            last_ai_content = ""

            system_prompt_effective = self.get_system_prompt(conversation_id)
            input_messages = [("system", system_prompt_effective), ("user", message)]

            async for chunk in agent.astream({"messages": input_messages}, config=config):
                if "agent" in chunk:
                    agent_data = chunk["agent"]
                    if "messages" in agent_data:
                        for msg in agent_data["messages"]:
                            if hasattr(msg, 'content') and msg.content:
                                content = str(msg.content)

                                if content != last_ai_content:
                                    last_ai_content = content
                                    yield {
                                        "type": "message",
                                        "content": content,
                                        "timestamp": datetime.now(timezone.utc).isoformat(),
                                        "streaming": True,
                                        "tool_result": False
                                    }

                elif "tools" in chunk:
                    tool_data = chunk["tools"]
                    if "messages" in tool_data:
                        for msg in tool_data["messages"]:
                            if hasattr(msg, 'content') and msg.content:
                                try:
                                    content = msg.content

                                    if isinstance(content, dict):
                                        if any(key in content for key in ['html', 'table', 'content']):
                                            formatted_content = content.get('html') or content.get('table') or content.get('content') or json.dumps(content, indent=2)
                                        else:
                                            formatted_content = json.dumps(content, indent=2)
                                    elif isinstance(content, str):
                                        if content.strip().startswith('<table') or content.strip().startswith('<!DOCTYPE') or '<html>' in content:
                                            formatted_content = content
                                        elif content[:1] in ['{', '[']:
                                            try:
                                                parsed = json.loads(content)
                                                formatted_content = json.dumps(parsed, indent=2)
                                            except:
                                                formatted_content = content
                                        else:
                                            formatted_content = content
                                    else:
                                        formatted_content = str(content)

                                    tool_result_id = hash(str(content))
                                    if tool_result_id not in tool_results_sent:
                                        tool_results_sent.add(tool_result_id)

                                        yield {
                                            "type": "message",
                                            "content": formatted_content,
                                            "timestamp": datetime.now(timezone.utc).isoformat(),
                                            "tool_result": True,
                                            "streaming": False
                                        }
                                except Exception as e:
                                    logger.error(f"Error formatting tool result: {e}")
                                    yield {
                                        "type": "message",
                                        "content": str(msg.content),
                                        "timestamp": datetime.now(timezone.utc).isoformat(),
                                        "tool_result": True,
                                        "streaming": False
                                    }

            yield {
                "type": "message_complete",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        except Exception as e:
            logger.error(f"Error in chat stream: {e}")
            yield {
                "type": "error",
                "content": f"An error occurred: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

# Global instances
mcp_client = MCPClient()
chatbot = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global chatbot
    logger.info("Starting Node Analyzer MCP Client...")
    connected = await mcp_client.connect()
    if connected:
        logger.info("Node Analyzer MCP Client connected successfully")
        chatbot = ChatBot(mcp_client)
    else:
        logger.warning("Failed to connect to MCP server, continuing with limited functionality")
        chatbot = ChatBot(mcp_client)
    yield
    logger.info("Shutting down Node Analyzer MCP Client...")

app = FastAPI(
    title="Node Analyzer MCP Client",
    description="AI-powered chat interface for OpenShift node performance analysis",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

HTML_FILE_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "webroot",
        "node",
        "node_analyzer_mcp_llm.html",
    )
)

@app.get("/", include_in_schema=False)
async def serve_root_html():
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    raise HTTPException(status_code=404, detail="node_analyzer_mcp_llm.html not found")

@app.get("/ui", include_in_schema=False)
async def serve_ui_html():
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    raise HTTPException(status_code=404, detail="node_analyzer_mcp_llm.html not found")

@app.get("/api/mcp/health", response_model=HealthResponse)
async def mcp_health_check():
    health_data = await mcp_client.check_mcp_connectivity_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )

@app.get("/api/node/health", response_model=HealthResponse)
async def node_health_check():
    health_data = await mcp_client.check_node_analyzer_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )

@app.get("/api/tools")
async def list_tools():
    return {
        "tools": mcp_client.available_tools,
        "count": len(mcp_client.available_tools)
    }

@app.post("/api/tools/{tool_name}")
async def call_tool_direct(tool_name: str, params: Dict[str, Any] = None):
    """Direct tool call endpoint"""
    try:
        # Wrap parameters in 'request' key as expected by MCP server
        wrapped_params = {"request": params} if params else {"request": {}}

        result = await mcp_client.call_tool(tool_name, wrapped_params)
        logger.info(f"Direct tool call result type: {type(result)}")

        # If the tool already returned a formatted HTML string, return as is
        if isinstance(result, str):
            return result
        # If the result is a dict with an error from the tool call, pass through
        if isinstance(result, dict) and result.get("error") and result.get("tool"):
            return result
        # Otherwise, return the result
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/system-prompt")
async def set_system_prompt(request: SystemPromptRequest):
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    try:
        chatbot.set_system_prompt(request.conversation_id, request.system_prompt)
        return {
            "status": "success",
            "message": f"System prompt updated for conversation {request.conversation_id}",
            "conversation_id": request.conversation_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to set system prompt: {str(e)}")

@app.get("/api/system-prompt/{conversation_id}")
async def get_system_prompt(conversation_id: str):
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    try:
        system_prompt = chatbot.get_system_prompt(conversation_id)
        return {
            "conversation_id": conversation_id,
            "system_prompt": system_prompt,
            "is_default": system_prompt == chatbot.default_system_prompt
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get system prompt: {str(e)}")

@app.delete("/api/system-prompt/{conversation_id}")
async def reset_system_prompt(conversation_id: str):
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    try:
        if conversation_id in chatbot.conversation_system_prompts:
            del chatbot.conversation_system_prompts[conversation_id]
        if conversation_id in chatbot.conversations:
            del chatbot.conversations[conversation_id]
        return {
            "status": "success",
            "message": f"System prompt reset to default for conversation {conversation_id}",
            "conversation_id": conversation_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reset system prompt: {str(e)}")

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")

    async def generate():
        try:
            async for chunk in chatbot.chat_stream(request.message, request.conversation_id, request.system_prompt):
                yield f"data: {json.dumps(chunk)}\n\n"
        except Exception as e:
            error_chunk = {
                "type": "error",
                "content": f"Stream error: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            yield f"data: {json.dumps(error_chunk)}\n\n"

        completion_chunk = {
            "type": "stream_end",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        yield f"data: {json.dumps(completion_chunk)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no"
        }
    )

@app.post("/chat")
async def chat_simple(request: ChatRequest):
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    responses = []
    async for chunk in chatbot.chat_stream(request.message, request.conversation_id, request.system_prompt):
        responses.append(chunk)
    return {"responses": responses}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "node_analyzer_client_chat:app",
        host="0.0.0.0",
        port=8084,
        reload=True,
        log_level="info"
    )
