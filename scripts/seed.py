"""
Seed script — loads data into all databases.

Usage:
    uv run python -m scripts.seed

Prerequisites:
    Run scripts.migrate first to create database structures.

What to implement in seed():
    Phase 1: Load products.json + customers.json into Postgres and MongoDB
    Phase 2: Initialize Redis inventory counters from Postgres product stock
    Phase 3: Build Neo4j co-purchase graph from historical_orders.json

Seed data files are in the seed_data/ directory.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

SEED_DIR = Path(__file__).parent.parent / "seed_data"


def seed(engine, mongo_db, redis_client=None, neo4j_driver=None):
    """Load seed data into all databases.

    Add your seeding logic here incrementally as you progress through phases.

    Args:
        engine: SQLAlchemy engine connected to Postgres
        mongo_db: pymongo Database instance
        redis_client: redis.Redis instance or None (Phase 2+)
        neo4j_driver: neo4j.Driver instance or None (Phase 3)

    Tip: Use json.load() to read the files in seed_data/:
        products = json.load(open(SEED_DIR / "products.json"))
        customers = json.load(open(SEED_DIR / "customers.json"))
        historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))
    """
    import json
    from sqlalchemy.orm import Session
    from ecommerce_pipeline.postgres_models import Customer, Product

    products_data = json.load(open(SEED_DIR / "products.json"))
    customers_data = json.load(open(SEED_DIR / "customers.json"))

    price_by_id = {p["id"]: p["price"] for p in products_data}

    # --- Phase 1: PostgreSQL ---
    with Session(engine) as session:
        session.add_all([
            Customer(id=c["id"], name=c["name"], email=c["email"])
            for c in customers_data
        ])
        session.flush()
        print(f"  [postgres] {len(customers_data)} customers inserted")

        session.add_all([
            Product(
                id=p["id"],
                name=p["name"],
                price=p["price"],
                stock_quantity=p["stock_quantity"],
                category=p["category"],
                description=p.get("description", ""),
            )
            for p in products_data
        ])
        session.flush()
        print(f"  [postgres] {len(products_data)} products inserted")

        session.commit()

    # --- Phase 1: MongoDB ---
    for p in products_data:
        mongo_db["product_catalog"].replace_one({"id": p["id"]}, p, upsert=True)
    print(f"  [mongo] {len(products_data)} products inserted")

    # --- Phase 2: Redis inventory counters ---
    if redis_client is not None:
        for p in products_data:
            redis_client.set(f"inventory:{p['id']}", p["stock_quantity"])
        print(f"  [redis] {len(products_data)} inventory counters initialized")

    # --- Phase 3: Neo4j co-purchase graph ---
    if neo4j_driver is not None:
        from itertools import combinations

        historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))
        name_by_id = {p["id"]: p["name"] for p in products_data}

        with neo4j_driver.session() as session:
            # Create Product nodes for all products referenced in orders
            for p in products_data:
                session.run(
                    "MERGE (p:Product {id: $id}) SET p.name = $name",
                    id=p["id"], name=p["name"],
                )

            # Build co-purchase edges from historical orders
            for order in historical_orders:
                pids = order["product_ids"]
                for a, b in combinations(pids, 2):
                    session.run(
                        "MERGE (a:Product {id: $a}) "
                        "MERGE (b:Product {id: $b}) "
                        "MERGE (a)-[r:BOUGHT_TOGETHER]-(b) "
                        "ON CREATE SET r.weight = 1 "
                        "ON MATCH SET r.weight = r.weight + 1",
                        a=a, b=b,
                    )

        print(f"  [neo4j] co-purchase graph built from {len(historical_orders)} orders")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _pg_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _mongo_db():
    from pymongo import MongoClient

    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    db = os.environ.get("MONGO_DB", "ecommerce")
    return MongoClient(host, port)[db]


def _redis_client():
    host = os.environ.get("REDIS_HOST")
    if not host:
        return None
    import redis

    port = int(os.environ.get("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, decode_responses=True)


def _neo4j_driver():
    host = os.environ.get("NEO4J_HOST")
    pwd = os.environ.get("NEO4J_PASSWORD")
    if not host or not pwd:
        return None
    from neo4j import GraphDatabase

    port = os.environ.get("NEO4J_BOLT_PORT", "7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    return GraphDatabase.driver(f"bolt://{host}:{port}", auth=(user, pwd))


def main():
    from sqlalchemy import create_engine

    engine = create_engine(_pg_url(), echo=False)
    mongo_db = _mongo_db()
    redis_client = _redis_client()
    neo4j_driver = _neo4j_driver()

    print("Seeding databases...")
    seed(engine, mongo_db, redis_client, neo4j_driver)
    print("Seeding complete.")

    if neo4j_driver:
        neo4j_driver.close()
    engine.dispose()


if __name__ == "__main__":
    main()
