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
# La queue principale est bindée sur "rag.index.*", et les retries renvoient sur "rag.index.file"
ROUTING_KEY = os.getenv("ROUTING_KEY", "rag.index.file")

# Champs RAG (peuvent aussi être passés en CLI)
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
        # Déclarer l'exchange pour être sûr (topic comme côté consumer)
        ex = await channel.declare_exchange(
            EXCHANGE_NAME, ExchangeType.TOPIC, durable=True
        )

        msg = Message(
            body=body or b"",
            headers=headers,
            delivery_mode=DeliveryMode.PERSISTENT,  # messages persistants
        )

        await ex.publish(msg, routing_key=routing_key)


# ---------- Subcommands ----------
async def cmd_upsert_file(args: argparse.Namespace) -> None:
    # Lecture du fichier (ou stdin)
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
        # Pas de file_url/file_bearer ici : on envoie le binaire dans le body
    )

    await publish_message(routing_key=args.routing_key, headers=headers, body=data)
    print(f"Published upsert-file for {args.file_id} on {args.partition}")


async def cmd_upsert_url(args: argparse.Namespace) -> None:
    # Ici on ne met pas de body; le consumer téléchargera via file_url
    if not args.file_url:
        print("--file-url est requis pour upsert-url", file=sys.stderr)
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
        # les autres headers sont facultatifs
    )
    await publish_message(routing_key=args.routing_key, headers=headers, body=b"")
    print(f"Published delete for {args.file_id} on {args.partition}")


def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="RAG indexer producer (compatible avec le consumer aio-pika)."
    )
    p.add_argument(
        "--routing-key",
        default=ROUTING_KEY,
        help="Routing key (default: rag.index.file)",
    )
    p.add_argument("--partition", required=True, help="Partition utilisateur/tenant")
    p.add_argument("--file-id", required=True, help="Identifiant logique du fichier")
    p.add_argument("--rag-base-url", help="Override RAG base URL (sinon RAG_BASE_URL)")
    p.add_argument("--rag-api-key", help="Override RAG API key (sinon RAG_API_KEY)")
    p.add_argument("--doctype", help="Type documentaire (facultatif)")
    p.add_argument("--md5sum", help="MD5 attendu (si absent et upsert-file : calculé)")
    p.add_argument("--name", help="Nom/filename d’affichage")
    p.add_argument("--dir-id", help="Répertoire parent (optionnel)")
    p.add_argument("--datetime", help="Datetime ISO (optionnel)")
    p.add_argument(
        "--content-type", help="Content-Type (sinon déduit pour upsert-file)"
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # upsert-file
    spf = sub.add_parser(
        "upsert-file", help="Envoyer un binaire directement dans le body"
    )
    spf.add_argument("path", help="Chemin du fichier ou '-' pour stdin")
    spf.set_defaults(func=cmd_upsert_file)

    # upsert-url
    spu = sub.add_parser("upsert-url", help="Laisser le consumer télécharger via URL")
    spu.add_argument("--file-url", required=True, help="URL du fichier à télécharger")
    spu.add_argument(
        "--file-bearer", help="Bearer à utiliser par le consumer pour télécharger"
    )
    spu.set_defaults(func=cmd_upsert_url)

    # delete
    spd = sub.add_parser("delete", help="Supprimer un fichier indexé")
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
