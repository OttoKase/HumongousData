# rico/parse.py
import json
import os

import boto3
import psycopg2

from rico.traceability import fingerprint


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )


def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple]]:
    """Iterative DFS — returns (element_type, text, bounds) for nodes with text or class."""
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None

    elements = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls  = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements


def text_representation(elements: list[tuple]) -> str:
    """Concatenate texts in reading order: sort by (y_top, x_left), join with spaces."""
    with_text = [e for e in elements if e[1]]
    in_order  = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _, text, _ in in_order)


def run(run_id: str, screen_ids: list[int]) -> None:

    s3     = _s3_client()
    bucket = os.environ["MINIO_BUCKET"]

    for sid in screen_ids:
        hier_key = f"screens/{sid}.json"
        text_key = f"screens/{sid}.txt"

        raw_json = s3.get_object(Bucket=bucket, Key=hier_key)["Body"].read().decode("utf-8")
        elements = parse_hierarchy(raw_json)
        text_rep = text_representation(elements)
        text_bytes = text_rep.encode("utf-8")

        s3.put_object(Bucket=bucket, Key=text_key, Body=text_bytes)

        fp = fingerprint(text_bytes)
        print(f"run_id={run_id} stage=parse screen_id={sid} chars={len(text_rep)} fp={fp[:12]}")

    print(f"run_id={run_id} stage=parse complete screens={len(screen_ids)}")