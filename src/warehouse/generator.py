"""Generate deterministic e-commerce transactional CSVs (Python standard library)."""
from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

CUSTOMER_HEADER = ["customer_id", "customer_name", "city"]
PRODUCT_HEADER = ["product_id", "product_name", "category"]
ORDER_HEADER = ["order_id", "customer_id", "order_timestamp", "order_status"]
ITEM_HEADER = ["order_id", "line_no", "product_id", "quantity", "unit_price_cents", "discount_cents"]


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def generate_dataset(out: Path, lines: int = 30, seed: int = 42) -> dict[str, int]:
    """Create exactly `lines` line items; order lines can be for non-completed orders."""
    if lines < 0:
        raise ValueError("--lines must be >= 0")
    rng = random.Random(seed)
    customer_count = min(200, max(3, (lines + 49) // 50)) if lines else 0
    product_count = min(80, max(3, (lines + 99) // 100)) if lines else 0
    customers = [
        {"customer_id": f"C{i:04d}", "customer_name": f"Customer {i}",
         "city": ["Pune", "Indore", "Bengaluru", "Mumbai"][i % 4]}
        for i in range(1, customer_count + 1)
    ]
    products = [
        {"product_id": f"P{i:04d}", "product_name": f"Product {i}",
         "category": ["Electronics", "Home", "Books"][i % 3]}
        for i in range(1, product_count + 1)
    ]
    orders: list[dict] = []
    items: list[dict] = []
    order_count = (lines + 2) // 3
    base = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    for i in range(order_count):
        order_id = f"O{i + 1:07d}"
        timestamp = (base + timedelta(days=rng.randrange(280), minutes=rng.randrange(1440))).isoformat(timespec="seconds")
        # Most orders completed; some are intentionally excluded from sales facts.
        status = "cancelled" if i % 17 == 16 else "pending" if i % 23 == 22 else "completed"
        orders.append({"order_id": order_id, "customer_id": rng.choice(customers)["customer_id"],
                       "order_timestamp": timestamp, "order_status": status})
        for line_no in range(1, min(3, lines - len(items)) + 1):
            unit_price = rng.randint(150, 25000)
            quantity = rng.randint(1, 5)
            gross = unit_price * quantity
            discount = rng.randint(0, gross // 5)
            items.append({"order_id": order_id, "line_no": line_no,
                          "product_id": rng.choice(products)["product_id"],
                          "quantity": quantity, "unit_price_cents": unit_price,
                          "discount_cents": discount})
    write_csv(out / "customers.csv", CUSTOMER_HEADER, customers)
    write_csv(out / "products.csv", PRODUCT_HEADER, products)
    write_csv(out / "orders.csv", ORDER_HEADER, orders)
    write_csv(out / "order_items.csv", ITEM_HEADER, items)
    return {"customers": len(customers), "products": len(products),
            "orders": len(orders), "order_items": len(items)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic e-commerce source CSVs")
    parser.add_argument("--out", type=Path, default=Path("data/sample_input"))
    parser.add_argument("--lines", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    print(generate_dataset(args.out, args.lines, args.seed))


if __name__ == "__main__":
    main()
