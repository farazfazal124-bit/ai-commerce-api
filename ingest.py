"""
Data ingestion and normalization module.

Handles loading product data from CSV, JSON, or API sources
and normalizes it into a consistent structured format.
"""

import csv
import json
from pathlib import Path
from typing import Optional


def normalize_product(raw: dict) -> dict:
    """Normalize a raw product record into a consistent schema."""
    tags_raw = raw.get("tags", "")
    if isinstance(tags_raw, str):
        tags = [t.strip() for t in tags_raw.split(";") if t.strip()]
    elif isinstance(tags_raw, list):
        tags = tags_raw
    else:
        tags = []

    def to_float(val, default=0.0):
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def to_int(val, default=0):
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def to_bool(val, default=False):
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in ("true", "1", "yes")
        return default

    return {
        "id": to_int(raw.get("id"), 0),
        "name": str(raw.get("name", "")).strip(),
        "description": str(raw.get("description", "")).strip(),
        "category": str(raw.get("category", "")).strip(),
        "subcategory": str(raw.get("subcategory", "")).strip(),
        "brand": str(raw.get("brand", "")).strip(),
        "pricing": {
            "price": to_float(raw.get("price")),
            "currency": str(raw.get("currency", "USD")).strip().upper(),
        },
        "sku": str(raw.get("sku", "")).strip(),
        "tags": tags,
        "rating": to_float(raw.get("rating")),
        "reviews_count": to_int(raw.get("reviews_count")),
        "in_stock": to_bool(raw.get("in_stock"), True),
        "attributes": {
            "weight_kg": to_float(raw.get("weight_kg")) or None,
            "color": str(raw.get("color", "")).strip() or None,
            "material": str(raw.get("material", "")).strip() or None,
        },
    }


def load_csv(filepath: str) -> list[dict]:
    """Load and normalize products from a CSV file."""
    products = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(normalize_product(row))
    return products


def load_json(filepath: str) -> list[dict]:
    """Load and normalize products from a JSON file."""
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [normalize_product(item) for item in data]
    elif isinstance(data, dict) and "products" in data:
        return [normalize_product(item) for item in data["products"]]
    raise ValueError("JSON must be an array or an object with a 'products' key")


def load_products(filepath: str) -> list[dict]:
    """Auto-detect file type and load products."""
    path = Path(filepath)
    if path.suffix.lower() == ".csv":
        return load_csv(filepath)
    elif path.suffix.lower() == ".json":
        return load_json(filepath)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")


def export_json(products: list[dict], output_path: str) -> None:
    """Export normalized products to a JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"products": products, "total": len(products)}, f, indent=2)


if __name__ == "__main__":
    # Quick CLI usage: python ingest.py data/products.csv data/products.json
    import sys

    src = sys.argv[1] if len(sys.argv) > 1 else "data/products.csv"
    dst = sys.argv[2] if len(sys.argv) > 2 else "data/products.json"
    products = load_products(src)
    export_json(products, dst)
    print(f"Normalized {len(products)} products → {dst}")
