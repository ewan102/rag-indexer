#!/usr/bin/env python3
import asyncio
import os
import sys
import argparse
import hashlib
import mimetypes
from typing import Optional

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aiormq import AMQPConnectionError
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# ---------- Config via env ----------
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "rag.index.topic")
# Main queue is bound to "rag.index.*", retries re-route to "rag.index.file"
ROUTING_KEY = os.getenv("ROUTING_KEY", "rag.index.file")

# RAG fields (can also be passed via CLI)
RAG_BASE_URL = os.getenv("RAG_BASE_URL", "")
RAG_API_KEY = os.getenv("RAG_API_KEY", "")

print("rag url:", RAG_BASE_URL)


def md5_of_bytes(data: bytes) -> str:
    h = hashlib.md5()
    h.update(data)
    return h.hexdigest()


def guess_content_type(
    path: Optional[str], fallback: str = "application/octet-stream"
) -> str:
    if not path or path == "-":
        return fallback
    ctype, _ = mimetypes.guess_type(path)
    return ctype or fallback


def build_headers(
    *,
    action: str,
    partition: str,
    file_id: str,
    rag_base_url: str,
    rag_api_key: str,
    doctype: Optional[str] = None,
    md5sum: Optional[str] = None,
    name: Optional[str] = None,
    dir_id: Optional[str] = None,
    dt: Optional[str] = None,
    content_type: Optional[str] = None,
    file_url: Optional[str] = None,
    file_bearer: Optional[str] = None,
) -> dict:
    h = {
        "action": action,  # "upsert" | "delete"
        "partition": partition,
        "file_id": file_id,
        "rag_base_url": rag_base_url,
        "rag_api_key": rag_api_key,
    }
    if doctype:
        h["doctype"] = doctype
    if md5sum:
        h["md5sum"] = md5sum
    if name:
        h["name"] = name
    if dir_id:
        h["dir_id"] = dir_id
    if dt:
        h["datetime"] = dt
    if content_type:
        h["content_type"] = content_type
    if file_url:
        h["file_url"] = file_url
    if file_bearer:
        h["file_bearer"] = file_bearer
    return h


async def publish_message(
    *,
    routing_key: str,
    headers: dict,
    body: bytes | None,
) -> None:
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
    except AMQPConnectionError as e:
        print(f"Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(2)

    async with connection:
        channel = await connection.channel()
        # Declare exchange to be safe (topic, same as consumer side)
        ex = await channel.declare_exchange(
            EXCHANGE_NAME, ExchangeType.TOPIC, durable=True
        )

        msg = Message(
            body=body or b"",
            headers=headers,
            delivery_mode=DeliveryMode.PERSISTENT,  # persistent messages
        )

        await ex.publish(msg, routing_key=routing_key)


# ---------- Subcommands ----------
async def cmd_upsert_file(args: argparse.Namespace) -> None:
    # Read file (or stdin)
    if args.path == "-":
        data = sys.stdin.buffer.read()
        filename = args.name or f"{args.file_id}.bin"
    else:
        with open(args.path, "rb") as f:
            data = f.read()
        filename = args.name or os.path.basename(args.path) or f"{args.file_id}.bin"

    md5sum = args.md5sum or md5_of_bytes(data)
    content_type = args.content_type or guess_content_type(args.path)

    headers = build_headers(
        action="upsert",
        partition=args.partition,
        file_id=args.file_id,
        rag_base_url=args.rag_base_url or RAG_BASE_URL,
        rag_api_key=args.rag_api_key or RAG_API_KEY,
        doctype=args.doctype,
        md5sum=md5sum,
        name=filename,
        dir_id=args.dir_id,
        dt=args.datetime,
        content_type=content_type,
        # No file_url/file_bearer here: binary is sent in the body
    )

    await publish_message(routing_key=args.routing_key, headers=headers, body=data)
    print(f"Published upsert-file for {args.file_id} on {args.partition}")


async def cmd_upsert_url(args: argparse.Namespace) -> None:
    # No body here; consumer will download via file_url
    if not args.file_url:
        print("--file-url is required for upsert-url", file=sys.stderr)
        sys.exit(2)

    headers = build_headers(
        action="upsert",
        partition=args.partition,
        file_id=args.file_id,
        rag_base_url=args.rag_base_url or RAG_BASE_URL,
        rag_api_key=args.rag_api_key or RAG_API_KEY,
        doctype=args.doctype,
        md5sum=args.md5sum,
        name=args.name,
        dir_id=args.dir_id,
        dt=args.datetime,
        content_type=args.content_type,
        file_url=args.file_url,
        file_bearer=args.file_bearer,
    )

    await publish_message(routing_key=args.routing_key, headers=headers, body=b"")
    print(f"Published upsert-url for {args.file_id} on {args.partition}")


async def cmd_delete(args: argparse.Namespace) -> None:
    headers = build_headers(
        action="delete",
        partition=args.partition,
        file_id=args.file_id,
        rag_base_url=args.rag_base_url or RAG_BASE_URL,
        rag_api_key=args.rag_api_key or RAG_API_KEY,
        # other headers are optional
    )
    await publish_message(routing_key=args.routing_key, headers=headers, body=b"")
    print(f"Published delete for {args.file_id} on {args.partition}")


def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="RAG indexer producer (compatible with the aio-pika consumer)."
    )
    p.add_argument(
        "--routing-key",
        default=ROUTING_KEY,
        help="Routing key (default: rag.index.file)",
    )
    p.add_argument("--partition", required=True, help="User/tenant partition")
    p.add_argument("--file-id", required=True, help="Logical file identifier")
    p.add_argument("--rag-base-url", help="Override RAG base URL (fallback: RAG_BASE_URL)")
    p.add_argument("--rag-api-key", help="Override RAG API key (fallback: RAG_API_KEY)")
    p.add_argument("--doctype", help="Document type (optional)")
    p.add_argument("--md5sum", help="Expected MD5 (computed from file if absent for upsert-file)")
    p.add_argument("--name", help="Display name/filename")
    p.add_argument("--dir-id", help="Parent directory (optional)")
    p.add_argument("--datetime", help="ISO datetime (optional)")
    p.add_argument(
        "--content-type", help="Content-Type (inferred from file for upsert-file)"
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # upsert-file
    spf = sub.add_parser(
        "upsert-file", help="Send a binary directly in the body"
    )
    spf.add_argument("path", help="File path or '-' for stdin")
    spf.set_defaults(func=cmd_upsert_file)

    # upsert-url
    spu = sub.add_parser("upsert-url", help="Let the consumer download via URL")
    spu.add_argument("--file-url", required=True, help="URL of the file to download")
    spu.add_argument(
        "--file-bearer", help="Bearer token for the consumer to use when downloading"
    )
    spu.set_defaults(func=cmd_upsert_url)

    # delete
    spd = sub.add_parser("delete", help="Delete an indexed file")
    spd.set_defaults(func=cmd_delete)

    return p


async def amain(argv: list[str]) -> None:
    parser = make_parser()
    args = parser.parse_args(argv)
    await args.func(args)


if __name__ == "__main__":
    try:
        asyncio.run(amain(sys.argv[1:]))
    except KeyboardInterrupt:
        pass
