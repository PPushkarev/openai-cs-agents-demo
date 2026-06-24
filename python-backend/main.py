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
from chatkit.types import UserMessageItem, UserMessageTextContent, InferenceOptions
from datetime import datetime

class BarkingDogRequest(BaseModel):
    message: str

@app.post("/")
async def barkingdog_endpoint(
    request: BarkingDogRequest, server: AirlineServer = Depends(get_server)
):
    try:
        thread = await server.ensure_thread(None, {"request": None})

        user_message = UserMessageItem(
            id=f"msg_{thread.id}",
            thread_id=thread.id,
            created_at=datetime.now(),
            content=[UserMessageTextContent(text=request.message)],
            inference_options=InferenceOptions()
        )

        reply_text = ""
        async for event in server.respond(thread, user_message, {"request": None}):
            from chatkit.types import ThreadItemDoneEvent, AssistantMessageItem
            if isinstance(event, ThreadItemDoneEvent):
                item = event.item
                if isinstance(item, AssistantMessageItem):
                    for part in item.content:
                        text = getattr(part, "text", "")
                        if text:
                            reply_text += text

        return {"reply": reply_text or "No response"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
