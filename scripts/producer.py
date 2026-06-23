#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import json
import mimetypes
import os
import sys

import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from aiormq import AMQPConnectionError
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

# ---------- Config via env ----------
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "rag.index.topic")
# Main queue binds "rag.index.*"; retry queues dead-letter back on "rag.index.retry".
ROUTING_KEY = os.getenv("ROUTING_KEY", "rag.index.file")

# RAG fields (can also be passed via CLI)
RAG_BASE_URL = os.getenv("RAG_BASE_URL", "")
RAG_API_KEY = os.getenv("RAG_API_KEY", "")


def md5_of_bytes(data: bytes) -> str:
    h = hashlib.md5()
    h.update(data)
    return h.hexdigest()


def guess_content_type(
    path: str | None, fallback: str = "application/octet-stream"
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
    doctype: str | None = None,
    md5sum: str | None = None,
    name: str | None = None,
    dir_id: str | None = None,
    dt: str | None = None,
    content_type: str | None = None,
    file_url: str | None = None,
    callback_url: str | None = None,
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
    if callback_url:
        h["callback_url"] = callback_url
    return h


async def publish_message(
    *,
    routing_key: str,
    headers: dict,
    body: bytes | None,
    fmt: str = "headers",
) -> None:
    """Publish a message in either 'headers' or 'cozy-json' format.

    'headers' (default): business fields in AMQP headers, file content in body.
    'cozy-json': all business fields JSON-encoded in the body, AMQP headers empty.
                 File content must be provided via the file_url field; any binary
                 body is discarded. content-type is set to application/json.
    """
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

        if fmt == "cozy-json":
            msg = Message(
                body=json.dumps(headers).encode(),
                headers={},
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,
            )
        else:
            msg = Message(
                body=body or b"",
                headers=headers,
                delivery_mode=DeliveryMode.PERSISTENT,
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
    fmt = args.format

    if fmt == "cozy-json" and not getattr(args, "file_url", None):
        print("Warning: --format cozy-json without --file-url; consumer will fail to fetch file content.", file=sys.stderr)

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
        callback_url=args.callback_url,
    )

    # In cozy-json mode the body is the JSON metadata; binary content must come via file_url.
    body = b"" if fmt == "cozy-json" else data
    await publish_message(routing_key=args.routing_key, headers=headers, body=body, fmt=fmt)
    print(f"Published upsert-file for {args.file_id} on {args.partition} [{fmt}]")


async def cmd_upsert_url(args: argparse.Namespace) -> None:
    # No body here; consumer will download via file_url
    if not args.file_url:
        print("--file-url is required for upsert-url", file=sys.stderr)
        sys.exit(2)

    fmt = args.format
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
        callback_url=args.callback_url,
    )

    await publish_message(routing_key=args.routing_key, headers=headers, body=b"", fmt=fmt)
    print(f"Published upsert-url for {args.file_id} on {args.partition} [{fmt}]")


async def cmd_delete(args: argparse.Namespace) -> None:
    fmt = args.format
    headers = build_headers(
        action="delete",
        partition=args.partition,
        file_id=args.file_id,
        rag_base_url=args.rag_base_url or RAG_BASE_URL,
        rag_api_key=args.rag_api_key or RAG_API_KEY,
        callback_url=args.callback_url,
    )
    await publish_message(routing_key=args.routing_key, headers=headers, body=b"", fmt=fmt)
    print(f"Published delete for {args.file_id} on {args.partition} [{fmt}]")


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
    p.add_argument("--callback-url", help="URL de callback cozy pour le statut d'indexation")

    sub = p.add_subparsers(dest="cmd", required=True)

    # upsert-file
    spf = sub.add_parser(
        "upsert-file", help="Send a binary directly in the body"
    )
    spf.add_argument("path", help="File path or '-' for stdin")
    spf.add_argument(
        "--format", choices=["headers", "cozy-json"], default="headers",
        help="Wire format: 'headers' (default) or 'cozy-json' (all fields in JSON body)",
    )
    spf.set_defaults(func=cmd_upsert_file)

    # upsert-url
    spu = sub.add_parser("upsert-url", help="Let the consumer download via URL")
    spu.add_argument("--file-url", required=True, help="URL of the file to download")
    spu.add_argument(
        "--format", choices=["headers", "cozy-json"], default="headers",
        help="Wire format: 'headers' (default) or 'cozy-json' (all fields in JSON body)",
    )
    spu.set_defaults(func=cmd_upsert_url)

    # delete
    spd = sub.add_parser("delete", help="Delete an indexed file")
    spd.add_argument(
        "--format", choices=["headers", "cozy-json"], default="headers",
        help="Wire format: 'headers' (default) or 'cozy-json' (all fields in JSON body)",
    )
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
