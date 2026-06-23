from __future__ import annotations as _annotations
import asyncio
import json
import os
from typing import Any, Dict

from chatkit.server import StreamingResult
from fastapi import Depends, FastAPI, Query, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel

from airline.agents import (
    booking_cancellation_agent,
    faq_agent,
    flight_information_agent,
    refunds_compensation_agent,
    seat_special_services_agent,
    triage_agent,
)
from airline.context import (
    AirlineAgentChatContext,
    AirlineAgentContext,
    create_initial_context,
    public_context,
)
from server import AirlineServer

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# Disable tracing for zero data retention orgs
os.environ.setdefault("OPENAI_TRACING_DISABLED", "1")

# CORS configuration (adjust as needed for deployment)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Changed to allow BarkingDog scanner requests
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

chat_server = None


def get_server() -> AirlineServer:
    global chat_server
    if chat_server is None:
        chat_server = AirlineServer()
    return chat_server


@app.post("/chatkit")
async def chatkit_endpoint(
    request: Request, server: AirlineServer = Depends(get_server)
) -> Response:
    try:
        payload = await request.body()

        result = await asyncio.wait_for(
            server.process(payload, {"request": request}),
            timeout=25
        )

        if isinstance(result, StreamingResult):
            return StreamingResponse(result, media_type="text/event-stream")

        if hasattr(result, "json"):
            return Response(content=result.json, media_type="application/json")

        return Response(content=result)

    except asyncio.TimeoutError:
        return Response(
            content=json.dumps({"error": "timeout"}),
            status_code=504,
            media_type="application/json"
        )

    except Exception as e:
        return Response(
            content=json.dumps({"error": str(e)}),
            status_code=500,
            media_type="application/json"
        )


@app.get("/chatkit/state")
async def chatkit_state(
    thread_id: str = Query(...),
    server: AirlineServer = Depends(get_server),
) -> Dict[str, Any]:
    return await server.snapshot(thread_id, {"request": None})


@app.get("/chatkit/bootstrap")
async def chatkit_bootstrap(
    server: AirlineServer = Depends(get_server),
) -> Dict[str, Any]:
    return await server.snapshot(None, {"request": None})




@app.get("/health")
async def health_check() -> Dict[str, str]:
    return {"status": "healthy"}


# ---------------------------------------------------------
# BARKINGDOG ADAPTER
# ---------------------------------------------------------

class BarkingDogRequest(BaseModel):
    message: str
    mode: str = "agent_audit"
    chat_history: list = []

@app.post("/webhook/aegis-scan")
async def aegis_scan_endpoint(
    request: BarkingDogRequest, server: AirlineServer = Depends(get_server)
):
    try:
        # 1. Reconstruct chat history for ChatKit format
        messages = []
        for turn in request.chat_history:
            role = turn.get("role", "user")
            content = turn.get("content", "")
            if content:
                messages.append({"role": role, "content": content})
        
        # Add the current payload
        messages.append({"role": "user", "content": request.message})
        
        # Wrap into ChatKit standard payload
        chatkit_payload = {
            "thread_id": "barkingdog-audit-thread",
            "messages": messages
        }
        
        payload_bytes = json.dumps(chatkit_payload).encode("utf-8")
        
        # 2. Process via the AirlineServer
        result = await asyncio.wait_for(
    server.process(payload_bytes, {"request": None}),
    timeout=25
)
        
        reply_text = ""
        
        # 3. Extract text from StreamingResult or standard response
        if isinstance(result, StreamingResult):
            chunks = []
            async for chunk in result:
                if isinstance(chunk, bytes):
                    chunk = chunk.decode("utf-8")
                chunks.append(chunk)
            
            raw_stream_content = "".join(chunks)
            reply_text = raw_stream_content
            
            # Attempt to parse SSE format to get the final assistant message
            if "data: " in reply_text:
                try:
                    lines = [line.strip() for line in raw_stream_content.split("\n") if line.strip().startswith("data:")]
                    if lines:
                        last_line_data = lines[-1].replace("data:", "").strip()
                        parsed = json.loads(last_line_data)
                        msgs = parsed.get("thread", {}).get("messages", [])
                        for msg in reversed(msgs):
                            if msg.get("role") == "assistant":
                                reply_text = msg.get("content", "")
                                break
                except Exception:
                    pass
        else:
            # Handle non-streaming JSON/String response
            content_str = ""
            if hasattr(result, "json"):
                content_str = result.json
            elif isinstance(result, (str, bytes)):
                content_str = result if isinstance(result, str) else result.decode("utf-8")
            
            try:
                data = json.loads(content_str)
                msgs = data.get("thread", {}).get("messages", [])
                if not msgs and "messages" in data:
                    msgs = data["messages"]
                
                for msg in reversed(msgs):
                    if msg.get("role") == "assistant":
                        reply_text = msg.get("content", "")
                        break
                
                if not reply_text:
                    reply_text = content_str
            except Exception:
                reply_text = content_str

        # Fallback if parsing failed
        if not reply_text:
            reply_text = "I am unable to process your request."
            
        return {"reply": reply_text}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


__all__ = [
    "AirlineAgentChatContext",
    "AirlineAgentContext",
    "app",
    "booking_cancellation_agent",
    "chat_server",
    "create_initial_context",
    "faq_agent",
    "flight_information_agent",
    "public_context",
    "refunds_compensation_agent",
    "seat_special_services_agent",
    "triage_agent",
]

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080)) # У вас здесь верно 8080
    uvicorn.run(app, host="0.0.0.0", port=port)
