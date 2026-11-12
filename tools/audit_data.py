"""Audit: dataset sanity, referential integrity, and cross-file consistency."""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"
D1 = PACKAGE / "Dzien1_Data_Lakes" / "dane" / "bronze"
D3 = PACKAGE / "Dzien3_Data_Warehouse" / "dane" / "bronze"
ADV2 = PACKAGE / "Dzien2_Data_Warehouse_Zaawansowany" / "dane" / "bronze"
ADV3 = PACKAGE / "Dzien3_Data_Warehouse_Zaawansowany" / "dane" / "bronze"


def main() -> int:
    errors = 0

    print("=========== CSV HEADERS ===========")
    files = {
        "customers": D1 / "customers" / "customers.csv",
        "products": D1 / "products" / "products.csv",
        "order_items": D1 / "order_items" / "order_items.csv",
        "orders_15": D1 / "orders" / "dt=2026-01-15" / "orders.csv",
        "orders_16": D1 / "orders" / "dt=2026-01-16" / "orders.csv",
        "orders_20": D3 / "orders" / "dt=2026-01-20" / "orders.csv",
        "items_20": D3 / "order_items_20260120" / "order_items_2026-01-20.csv",
        "cust_upd": D3 / "customers_updates" / "customers_updates.csv",
        "ship_d2": ADV2 / "shipments" / "shipments.csv",
        "ship_d3": ADV3 / "shipments_20260121" / "shipments_2026-01-21.csv",
    }
    tables: dict[str, list[dict[str, str]]] = {}
    for name, p in files.items():
        if not p.exists():
            print(f"  !! MISSING {p.relative_to(ROOT)}")
            errors += 1
            continue
        with p.open(encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        tables[name] = rows
        print(f"{name:12} rows={len(rows):5} cols={list(rows[0].keys())}")

    if errors:
        return 1

    print("\n=========== NEGATIVE / WEIRD AMOUNTS ===========")
    neg = []
    for r in tables["order_items"] + tables["items_20"]:
        net = Decimal(r["qty"]) * Decimal(r["unit_price_pln"]) - Decimal(r["discount_pln"])
        if net <= 0:
            neg.append(
                (r["order_id"], r["product_id"], r["qty"], r["unit_price_pln"], r["discount_pln"], net)
            )
    print(f"rows with net_amount <= 0: {len(neg)}")
    errors += len(neg)
    for row in neg[:10]:
        print("   ", row)

    print("\n=========== REFERENTIAL INTEGRITY ===========")
    cust_ids = {r["customer_id"] for r in tables["customers"]}
    prod_ids = {r["product_id"] for r in tables["products"]}
    orders_all = tables["orders_15"] + tables["orders_16"] + tables["orders_20"]
    order_ids = {r["order_id"] for r in orders_all}
    bad_cust = [o["order_id"] for o in orders_all if o["customer_id"] not in cust_ids]
    bad_items = [
        i["order_id"] for i in tables["order_items"] + tables["items_20"] if i["order_id"] not in order_ids
    ]
    bad_prod = [
        i["order_id"] for i in tables["order_items"] + tables["items_20"] if i["product_id"] not in prod_ids
    ]
    upd_bad = [u["customer_id"] for u in tables["cust_upd"] if u["customer_id"] not in cust_ids]
    bad_ship = [
        s["shipment_id"]
        for s in tables["ship_d2"] + tables["ship_d3"]
        if s["order_id"] not in order_ids
    ]
    print("orders with unknown customer:", len(bad_cust))
    print("items with unknown order:", len(bad_items))
    print("items with unknown product:", len(bad_prod))
    print("updates with unknown customer:", len(upd_bad))
    print("shipments with unknown order:", len(bad_ship))
    dupes = [k for k, v in Counter(o["order_id"] for o in orders_all).items() if v > 1]
    print("duplicate order_id:", len(dupes))
    item_pk = Counter((i["order_id"], i["product_id"]) for i in tables["order_items"])
    dup_items = sum(1 for v in item_pk.values() if v > 1)
    print("duplicate (order_id, product_id) in day1 items:", dup_items)
    errors += (
        len(bad_cust)
        + len(bad_items)
        + len(bad_prod)
        + len(upd_bad)
        + len(bad_ship)
        + len(dupes)
        + dup_items
    )

    print("\n=========== STATUS / CHANNEL DOMAINS ===========")
    print("status:", Counter(o["status"] for o in orders_all))
    print("channel:", Counter(o["channel"] for o in orders_all))
    print(
        "day1 orders cancelled:",
        sum(1 for o in tables["orders_15"] + tables["orders_16"] if o["status"] == "cancelled"),
    )

    print("\n=========== EXPECTED COUNTS FOR SOLUTIONS ===========")
    d1_orders = tables["orders_15"] + tables["orders_16"]
    ok_orders = {o["order_id"] for o in d1_orders if o["status"] in ("paid", "shipped")}
    silver_items = [i for i in tables["order_items"] if i["order_id"] in ok_orders]
    print("bronze_orders:", len(d1_orders))
    print("bronze_order_items:", len(tables["order_items"]))
    print("silver_orders (paid/shipped):", len(ok_orders))
    print("silver_order_items:", len(silver_items))
    rev = sum(
        Decimal(i["qty"]) * Decimal(i["unit_price_pln"]) - Decimal(i["discount_pln"])
        for i in silver_items
    )
    print("silver revenue total:", rev)
    if rev != Decimal("1427124.00"):
        print(f"  !!EXPECTED 1427124.00, GOT {rev}")
        errors += 1

    by_day: dict[str, Decimal] = {}
    omap = {o["order_id"]: o for o in d1_orders}
    for i in silver_items:
        d = omap[i["order_id"]]["order_ts"][:10]
        by_day[d] = (
            by_day.get(d, Decimal(0))
            + Decimal(i["qty"]) * Decimal(i["unit_price_pln"])
            - Decimal(i["discount_pln"])
        )
    print("revenue by day:", {k: str(v) for k, v in sorted(by_day.items())})

    ev_count = 0
    ev_types: Counter[str] = Counter()
    for p in sorted(D1.glob("events/dt=*/events.json")):
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.strip():
                ev_count += 1
                ev_types[json.loads(line)["event_type"]] += 1
    print("events:", ev_count, dict(ev_types))

    print("\n=========== SCD UPDATE SANITY ===========")
    cmap = {c["customer_id"]: c for c in tables["customers"]}
    for u in tables["cust_upd"]:
        old = cmap[u["customer_id"]]
        same_city = old["city"] == u["city"]
        same_seg = old["segment"] == u["segment"]
        if same_city and same_seg:
            errors += 1
        print(
            f"  {u['customer_id']}: {old['city']}->{u['city']} (same={same_city}) "
            f"{old['segment']}->{u['segment']} (same={same_seg}) valid_from={u['valid_from']}"
        )

    print("\n=========== DAY-3 ORDERS BY CHANGED CUSTOMERS ===========")
    changed = {u["customer_id"] for u in tables["cust_upd"]}
    d1_by_changed = [
        o for o in d1_orders if o["customer_id"] in changed and o["status"] in ("paid", "shipped")
    ]
    print("day1 paid/shipped orders placed by later-moving customers:", len(d1_by_changed))
    if not d1_by_changed:
        errors += 1

    print("\n=========== DAY-3 ADVANCED DANE MIRRORS BASIC INCREMENT ===========")
    for rel in (
        Path("customers_updates") / "customers_updates.csv",
        Path("orders") / "dt=2026-01-20" / "orders.csv",
        Path("order_items_20260120") / "order_items_2026-01-20.csv",
    ):
        basic = D3 / rel
        adv = ADV3 / rel
        if not adv.exists():
            print(f"  !! MISSING {rel.as_posix()}")
            errors += 1
        elif basic.read_bytes() != adv.read_bytes():
            print(f"  !! MISMATCH {rel.as_posix()}")
            errors += 1
        else:
            print(f"  OK {rel.as_posix()}")
    ship21 = ADV3 / "shipments_20260121" / "shipments_2026-01-21.csv"
    print(f"  {'OK' if ship21.exists() else '!! MISSING'} shipments_20260121/shipments_2026-01-21.csv")

    print(f"\nSUMMARY: {errors} errors")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
