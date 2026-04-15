"""
AI Commerce Platform — Product Search API

A lightweight FastAPI service that exposes structured product catalog data
with keyword search and filtering capabilities.
"""

from fastapi import FastAPI, Query
from typing import Optional
from ingest import load_products

app = FastAPI(
    title="AI Commerce Product API",
    version="0.1.0",
    description="Structured product catalog API with search and filtering",
)

# ---------------------------------------------------------------------------
# Load product data at startup
# ---------------------------------------------------------------------------
PRODUCTS: list[dict] = []


@app.on_event("startup")
def startup():
    global PRODUCTS
    PRODUCTS = load_products("data/products.csv")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _matches_query(product: dict, query: str) -> bool:
    """Check if a product matches a keyword query (case-insensitive)."""
    q = query.lower()
    searchable = " ".join(
        [
            product.get("name", ""),
            product.get("description", ""),
            product.get("category", ""),
            product.get("subcategory", ""),
            product.get("brand", ""),
            " ".join(product.get("tags", [])),
            (product.get("attributes") or {}).get("color", "") or "",
            (product.get("attributes") or {}).get("material", "") or "",
        ]
    ).lower()
    # Support multi-word: all tokens must appear
    return all(token in searchable for token in q.split())


def _relevance_score(product: dict, query: str) -> int:
    """Simple relevance scoring: prioritize name > tags > description."""
    score = 0
    name_lower = product.get("name", "").lower()
    desc_lower = product.get("description", "").lower()
    tags_lower = [t.lower() for t in product.get("tags", [])]

    for token in query.lower().split():
        if token in name_lower:
            score += 10
        if token in tags_lower:
            score += 5
        elif any(token in t for t in tags_lower):
            score += 3
        if token in desc_lower:
            score += 2
    return score


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/search")
def search_products(
    q: str = Query(..., min_length=1, description="Search keyword(s)"),
    category: Optional[str] = Query(None, description="Filter by category"),
    brand: Optional[str] = Query(None, description="Filter by brand"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, ge=0, description="Maximum price"),
    in_stock: Optional[bool] = Query(None, description="Filter by stock availability"),
    sort_by: Optional[str] = Query(
        None,
        description="Sort field: relevance | price_asc | price_desc | rating",
    ),
    limit: int = Query(20, ge=1, le=100, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """
    Search products by keyword with optional filters.

    Returns matching products ranked by relevance (default) with pagination.
    """
    results = [p for p in PRODUCTS if _matches_query(p, q)]

    # Apply filters
    if category:
        cat = category.lower()
        results = [p for p in results if p.get("category", "").lower() == cat]
    if brand:
        b = brand.lower()
        results = [p for p in results if p.get("brand", "").lower() == b]
    if min_price is not None:
        results = [p for p in results if p["pricing"]["price"] >= min_price]
    if max_price is not None:
        results = [p for p in results if p["pricing"]["price"] <= max_price]
    if in_stock is not None:
        results = [p for p in results if p.get("in_stock") == in_stock]

    # Sort
    if sort_by == "price_asc":
        results.sort(key=lambda p: p["pricing"]["price"])
    elif sort_by == "price_desc":
        results.sort(key=lambda p: p["pricing"]["price"], reverse=True)
    elif sort_by == "rating":
        results.sort(key=lambda p: p.get("rating", 0), reverse=True)
    else:
        results.sort(key=lambda p: _relevance_score(p, q), reverse=True)

    total = len(results)
    page = results[offset : offset + limit]

    return {
        "query": q,
        "filters_applied": {
            "category": category,
            "brand": brand,
            "min_price": min_price,
            "max_price": max_price,
            "in_stock": in_stock,
        },
        "sort_by": sort_by or "relevance",
        "total": total,
        "limit": limit,
        "offset": offset,
        "results": page,
    }


@app.get("/products/{product_id}")
def get_product(product_id: int):
    """Retrieve a single product by ID."""
    for p in PRODUCTS:
        if p["id"] == product_id:
            return p
    return {"error": "Product not found"}, 404


@app.get("/categories")
def list_categories():
    """List all available categories with counts."""
    cats: dict[str, int] = {}
    for p in PRODUCTS:
        c = p.get("category", "Unknown")
        cats[c] = cats.get(c, 0) + 1
    return {"categories": [{"name": k, "count": v} for k, v in sorted(cats.items())]}


@app.get("/health")
def health():
    return {"status": "ok", "products_loaded": len(PRODUCTS)}
