"""
DBAccess — the data access layer.

This is one of the files you implement. The web API is already wired up;
every route calls one method on this class. Your job is to replace each
`raise NotImplementedError(...)` with a real implementation.

Work through the phases in order. Read the corresponding lesson file before
starting each phase.

You also implement scripts/migrate.py and scripts/seed.py alongside this file.
"""

from __future__ import annotations

import json
import logging
from itertools import combinations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import neo4j
    import redis as redis_lib
    from pymongo.database import Database as MongoDatabase
    from sqlalchemy.orm import sessionmaker

    from ecommerce_pipeline.models.requests import OrderItemRequest
    from ecommerce_pipeline.models.responses import (
        CategoryRevenueResponse,
        OrderCustomerEmbed,
        OrderItemResponse,
        OrderResponse,
        OrderSnapshotResponse,
        ProductResponse,
        RecommendationResponse,
    )

logger = logging.getLogger(__name__)


class DBAccess:
    def __init__(
        self,
        pg_session_factory: sessionmaker,
        mongo_db: MongoDatabase,
        redis_client: redis_lib.Redis | None = None,
        neo4j_driver: neo4j.Driver | None = None,
    ) -> None:
        self._pg_session_factory = pg_session_factory
        self._mongo_db = mongo_db
        self._redis = redis_client
        self._neo4j = neo4j_driver

    # ── Phase 1 ───────────────────────────────────────────────────────────────

    def create_order(self, customer_id: int, items: list[OrderItemRequest]) -> OrderResponse:
        """Place an order atomically.

        See OrderItemRequest in models/requests.py for the input shape.
        See OrderResponse in models/responses.py for the return shape.

        Raises ValueError if any product has insufficient stock. When that
        happens, no data is modified in any database.

        After the order is persisted transactionally, a denormalized snapshot
        is saved for read access, and downstream counters and graph edges are
        updated (best-effort, does not roll back the order on failure).
        """
        from sqlalchemy import select
        from ecommerce_pipeline.postgres_models import Customer, Order, OrderItem, Product
        from ecommerce_pipeline.models.responses import (
            OrderCustomerEmbed,
            OrderItemResponse,
            OrderResponse,
        )

        with self._pg_session_factory() as session:
            # Lock product rows to prevent concurrent overselling
            product_ids = [item.product_id for item in items]
            products = {
                p.id: p
                for p in session.execute(
                    select(Product)
                    .where(Product.id.in_(product_ids))
                    .with_for_update()
                ).scalars()
            }

            # Validate stock before any writes
            for item in items:
                product = products.get(item.product_id)
                if product is None:
                    raise ValueError(f"Product {item.product_id} not found")
                if product.stock_quantity < item.quantity:
                    raise ValueError(
                        f"Insufficient stock for product {item.product_id}: "
                        f"requested {item.quantity}, available {product.stock_quantity}"
                    )

            # Fetch customer for the snapshot
            customer = session.get(Customer, customer_id)
            if customer is None:
                raise ValueError(f"Customer {customer_id} not found")

            total_amount = sum(
                products[item.product_id].price * item.quantity for item in items
            )

            order = Order(
                customer_id=customer_id,
                status="completed",
                total_amount=total_amount,
            )
            session.add(order)
            session.flush()  # populate order.id

            order_item_rows = []
            for item in items:
                product = products[item.product_id]
                order_item_rows.append(
                    OrderItem(
                        order_id=order.id,
                        product_id=item.product_id,
                        quantity=item.quantity,
                        unit_price=product.price,
                    )
                )
                product.stock_quantity -= item.quantity

            session.add_all(order_item_rows)
            session.commit()

            order_id = order.id
            created_at = order.created_at.isoformat() if hasattr(order.created_at, "isoformat") else str(order.created_at)
            status = order.status

            item_responses = [
                OrderItemResponse(
                    product_id=item.product_id,
                    product_name=products[item.product_id].name,
                    quantity=item.quantity,
                    unit_price=float(products[item.product_id].price),
                )
                for item in items
            ]
            customer_embed = OrderCustomerEmbed(
                id=customer.id,
                name=customer.name,
                email=customer.email,
            )

        # Best-effort snapshot (does not roll back the order on failure)
        try:
            self.save_order_snapshot(
                order_id=order_id,
                customer=customer_embed,
                items=item_responses,
                total_amount=float(total_amount),
                status=status,
                created_at=created_at,
            )
        except Exception:
            logger.exception("Failed to save order snapshot for order %s", order_id)

        return OrderResponse(
            order_id=order_id,
            customer_id=customer_id,
            status=status,
            total_amount=float(total_amount),
            created_at=created_at,
            items=item_responses,
        )

    def get_product(self, product_id: int) -> ProductResponse | None:
        """Fetch a product by its integer ID.

        See ProductResponse in models/responses.py for the return shape.
        Returns None if not found.
        """
        from ecommerce_pipeline.models.responses import ProductResponse

        doc = self._mongo_db["product_catalog"].find_one({"id": product_id}, {"_id": 0})
        if doc is None:
            return None
        return ProductResponse(**doc)

    def search_products(
        self,
        category: str | None = None,
        q: str | None = None,
    ) -> list[ProductResponse]:
        """Search the product catalog with optional filters.

        category: exact match on the category field
        q: case-insensitive substring match on the product name
        Both filters are ANDed together. Returns all products if both are None.
        """
        import re
        from ecommerce_pipeline.models.responses import ProductResponse

        query: dict = {}
        if category is not None:
            query["category"] = category
        if q is not None:
            query["name"] = {"$regex": re.escape(q), "$options": "i"}

        docs = self._mongo_db["product_catalog"].find(query, {"_id": 0})
        return [ProductResponse(**doc) for doc in docs]

    def save_order_snapshot(
        self,
        order_id: int,
        customer: OrderCustomerEmbed,
        items: list[OrderItemResponse],
        total_amount: float,
        status: str,
        created_at: str,
    ) -> str:
        """Save a denormalized order snapshot for fast read access.

        See OrderCustomerEmbed and OrderItemResponse in models/responses.py
        for the input shapes.

        Embeds all customer and product details as they existed at the time
        of the order, so the snapshot remains accurate even if prices or
        names change later.

        Returns a string identifier for the saved document.

        Called internally by create_order after the transactional write
        commits. Not called directly by routes.
        """
        doc = {
            "order_id": order_id,
            "customer": customer.model_dump(),
            "items": [item.model_dump() for item in items],
            "total_amount": total_amount,
            "status": status,
            "created_at": created_at,
        }
        self._mongo_db["order_snapshots"].replace_one({"order_id": order_id}, doc, upsert=True)
        return str(order_id)

    def get_order(self, order_id: int) -> OrderSnapshotResponse | None:
        """Fetch a single order snapshot by order_id.

        See OrderSnapshotResponse in models/responses.py for the return shape.
        Returns None if not found.
        """
        from ecommerce_pipeline.models.responses import OrderSnapshotResponse

        doc = self._mongo_db["order_snapshots"].find_one({"order_id": order_id}, {"_id": 0})
        if doc is None:
            return None
        return OrderSnapshotResponse(**doc)

    def get_order_history(self, customer_id: int) -> list[OrderSnapshotResponse]:
        """Fetch all order snapshots for a customer, sorted by created_at descending.

        Returns an empty list if the customer has no orders.
        """
        from ecommerce_pipeline.models.responses import OrderSnapshotResponse

        docs = self._mongo_db["order_snapshots"].find(
            {"customer.id": customer_id},
            {"_id": 0},
            sort=[("created_at", -1)],
        )
        return [OrderSnapshotResponse(**doc) for doc in docs]

    def revenue_by_category(self) -> list[CategoryRevenueResponse]:
        """Compute total revenue per product category, sorted by total_revenue descending.

        See CategoryRevenueResponse in models/responses.py for the return shape.
        """
        from sqlalchemy import select, func
        from ecommerce_pipeline.postgres_models import OrderItem, Product
        from ecommerce_pipeline.models.responses import CategoryRevenueResponse

        with self._pg_session_factory() as session:
            rows = session.execute(
                select(
                    Product.category,
                    func.sum(OrderItem.quantity * OrderItem.unit_price).label("total_revenue"),
                )
                .join(Product, OrderItem.product_id == Product.id)
                .group_by(Product.category)
                .order_by(func.sum(OrderItem.quantity * OrderItem.unit_price).desc())
            ).all()

        return [
            CategoryRevenueResponse(category=row.category, total_revenue=float(row.total_revenue))
            for row in rows
        ]

    # ── Phase 2 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to DECR Redis inventory counters after the
    #     Postgres transaction succeeds.
    #   - Optionally, add a fast pre-check: before starting the Postgres
    #     transaction, check the Redis counter. If it shows insufficient
    #     stock, fail fast without hitting Postgres.
    #   - Update scripts/seed.py to initialize inventory counters in Redis.
    #   - Add cache-aside logic to get_product (check Redis first, populate
    #     on miss with a 300-second TTL).

    def invalidate_product_cache(self, product_id: int) -> None:
        """Remove a product's cached entry.

        Call this after updating a product's data so the next read fetches
        fresh data from the primary store. No-op if no entry exists.
        """
        raise NotImplementedError("Phase 2: implement invalidate_product_cache")

    def record_product_view(self, customer_id: int, product_id: int) -> None:
        """Record that a customer viewed a product.

        Maintains a bounded, ordered list of the customer's most recently
        viewed products (most recent first, capped at 10 entries).
        """
        raise NotImplementedError("Phase 2: implement record_product_view")

    def get_recently_viewed(self, customer_id: int) -> list[int]:
        """Return up to 10 recently viewed product IDs for a customer.

        Returns IDs as integers, most recently viewed first.
        Returns an empty list if no views have been recorded.
        """
        raise NotImplementedError("Phase 2: implement get_recently_viewed")

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to MERGE co-purchase edges in Neo4j for every
    #     pair of products in the order, incrementing the edge weight.
    #   - Update scripts/migrate.py to create Neo4j constraints.
    #   - Update scripts/seed.py to build the co-purchase graph from
    #     seed_data/historical_orders.json.

    def get_recommendations(self, product_id: int, limit: int = 5) -> list[RecommendationResponse]:
        """Return product recommendations based on co-purchase patterns.

        See RecommendationResponse in models/responses.py for the return shape.
        Sorted by score descending. Returns an empty list if no co-purchase relationships exist.
        """
        raise NotImplementedError("Phase 3: implement get_recommendations")
