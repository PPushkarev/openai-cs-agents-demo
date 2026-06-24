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


# BARKINGDOG ADAPTER
class BarkingDogRequest(BaseModel):
    message: str

@app.post("/")
async def barkingdog_endpoint(
    request: BarkingDogRequest, server: AirlineServer = Depends(get_server)
):
    try:
        chatkit_payload = {
    "type": "threads.add_user_message",
    "params": {
    "thread_id": "barkingdog-audit-thread",
    "input": request.message
}
}
        payload_bytes = json.dumps(chatkit_payload).encode("utf-8")
        result = await asyncio.wait_for(
            server.process(payload_bytes, {"request": None}),
            timeout=25
        )
        reply_text = ""
        if isinstance(result, StreamingResult):
            chunks = []
            async for chunk in result:
                if isinstance(chunk, bytes):
                    chunk = chunk.decode("utf-8")
                chunks.append(chunk)
            raw = "".join(chunks)
            if "data: " in raw:
                try:
                    lines = [l.strip() for l in raw.split("\n") if l.strip().startswith("data:")]
                    if lines:
                        parsed = json.loads(lines[-1].replace("data:", "").strip())
                        for msg in reversed(parsed.get("thread", {}).get("messages", [])):
                            if msg.get("role") == "assistant":
                                reply_text = msg.get("content", "")
                                break
                except Exception:
                    pass
            if not reply_text:
                reply_text = raw
        else:
            content_str = result.json if hasattr(result, "json") else str(result)
            try:
                data = json.loads(content_str)
                for msg in reversed(data.get("thread", {}).get("messages", [])):
                    if msg.get("role") == "assistant":
                        reply_text = msg.get("content", "")
                        break
                if not reply_text:
                    reply_text = content_str
            except Exception:
                reply_text = content_str
        return {"reply": reply_text or "No response"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
