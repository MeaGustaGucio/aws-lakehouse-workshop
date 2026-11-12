"""Computes the answer to every exercise so the trainer can verify students instantly.

Mirrors the SQL from 02_rozwiazania.md against the generated CSV/JSON, in plain Python.
Run it after tools/generate_data.py whenever the dataset changes.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"
D1 = PACKAGE / "Dzien1_Data_Lakes" / "dane" / "bronze"
D3 = PACKAGE / "Dzien3_Data_Warehouse" / "dane" / "bronze"
OK_STATUS = ("paid", "shipped")


def read(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def money(v: Decimal) -> str:
    return f"{v:,.2f}".replace(",", " ")


customers = read(D1 / "customers" / "customers.csv")
products = read(D1 / "products" / "products.csv")
items = read(D1 / "order_items" / "order_items.csv")
orders = read(D1 / "orders" / "dt=2026-01-15" / "orders.csv") + read(
    D1 / "orders" / "dt=2026-01-16" / "orders.csv"
)
orders20 = read(D3 / "orders" / "dt=2026-01-20" / "orders.csv")
items20 = read(D3 / "order_items_20260120" / "order_items_2026-01-20.csv")
updates = read(D3 / "customers_updates" / "customers_updates.csv")

events = []
for p in sorted(D1.glob("events/dt=*/events.json")):
    dt = p.parent.name.split("=")[1]
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            row["dt"] = dt
            events.append(row)

prod_by_id = {p["product_id"]: p for p in products}
cust_by_id = {c["customer_id"]: c for c in customers}
order_by_id = {o["order_id"]: o for o in orders + orders20}


def net(row: dict) -> Decimal:
    return Decimal(row["qty"]) * Decimal(row["unit_price_pln"]) - Decimal(row["discount_pln"])


silver_orders = [o for o in orders if o["status"] in OK_STATUS]
silver_ids = {o["order_id"] for o in silver_orders}
silver_items = [i for i in items if i["order_id"] in silver_ids]

print("=" * 70)
print("DZIEŃ 1")
print("=" * 70)
print(f"L3  bronze_customers={len(customers)}  bronze_products={len(products)}")
print(f"    bronze_orders={len(orders)}  bronze_order_items={len(items)}  bronze_events={len(events)}")
by_dt = defaultdict(int)
for o in orders:
    by_dt[o["order_ts"][:10]] += 1
print(f"    bronze_orders wg dt: {dict(sorted(by_dt.items()))}")

print(f"\nL4.1 zamówienia paid/shipped = {len(silver_orders)}")

rev_by_day: dict[str, Decimal] = defaultdict(Decimal)
for i in silver_items:
    rev_by_day[order_by_id[i["order_id"]]["order_ts"][:10]] += net(i)
print("L4.2 przychód wg dnia:")
for d, v in sorted(rev_by_day.items()):
    print(f"       {d}  {money(v)}")

rev_by_cat: dict[str, Decimal] = defaultdict(Decimal)
for i in silver_items:
    rev_by_cat[prod_by_id[i["product_id"]]["category"]] += net(i)
print("L4.3 top 5 kategorii:")
for cat, v in sorted(rev_by_cat.items(), key=lambda kv: -kv[1])[:5]:
    print(f"       {cat:12} {money(v)}")

mobile = {o["customer_id"] for o in silver_orders if o["channel"] == "mobile"}
print(f"L4.4 unikalni klienci mobile = {len(mobile)}")

print(f"\nL5  silver_orders={len(silver_orders)}  silver_order_items={len(silver_items)}")
print(f"    suma net_amount_pln = {money(sum(rev_by_day.values()))}")
print("L6  te same kwoty co L4.2 — zmienia się tylko Data scanned")

cart = {(e["customer_id"], e["dt"]) for e in events if e["event_type"] == "add_to_cart"}
buy = {(e["customer_id"], e["dt"]) for e in events if e["event_type"] == "purchase"}
aband: dict[str, int] = defaultdict(int)
for c, d in cart - buy:
    aband[d] += 1
print(f"S2  porzucone koszyki wg dnia: {dict(sorted(aband.items()))}")

print()
print("=" * 70)
print("DZIEŃ 2")
print("=" * 70)
dates = sorted({order_by_id[i["order_id"]]["order_ts"][:10] for i in silver_items})
channels = sorted({o["channel"] for o in silver_orders})
print(f"W3  dim_date={len(dates)} {dates}   dim_channel={len(channels)} {channels}")
print(f"    dim_customer={len(customers)}   dim_product={len(products)}   fact_order_item={len(silver_items)}")

print("W4.1 przychód wg kategorii i dnia (pełna siatka):")
grid: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
for i in silver_items:
    d = order_by_id[i["order_id"]]["order_ts"][:10]
    grid[(d, prod_by_id[i["product_id"]]["category"])] += net(i)
for (d, cat), v in sorted(grid.items(), key=lambda kv: (kv[0][0], -kv[1])):
    print(f"       {d}  {cat:12} {money(v)}")

agd: dict[str, Decimal] = defaultdict(Decimal)
for i in silver_items:
    if prod_by_id[i["product_id"]]["category"] != "AGD":
        continue
    city = cust_by_id[order_by_id[i["order_id"]]["customer_id"]]["city"]
    agd[city] += net(i)
print("W4.2 top 5 miast wg przychodu AGD:")
for city, v in sorted(agd.items(), key=lambda kv: -kv[1])[:5]:
    print(f"       {city:12} {money(v)}")

ch_rev: dict[str, Decimal] = defaultdict(Decimal)
ch_orders: dict[str, set] = defaultdict(set)
for i in silver_items:
    o = order_by_id[i["order_id"]]
    ch_rev[o["channel"]] += net(i)
    ch_orders[o["channel"]].add(o["order_id"])
print("W4.3 AOV wg kanału:")
for ch, v in sorted(ch_rev.items(), key=lambda kv: -(kv[1] / len(ch_orders[kv[0]]))):
    print(f"       {ch:12} AOV {money(v / len(ch_orders[ch]))}  ({len(ch_orders[ch])} zamówień)")

print(f"W4.4 recon silver == gold: {money(sum(rev_by_day.values()))}")

print()
print("=" * 70)
print("DZIEŃ 3")
print("=" * 70)
changed = {u["customer_id"] for u in updates}
print(f"W6  gold_dim_customer_scd: {len(customers) + len(updates)} wierszy, "
      f"{len(customers)} z is_current=true, {len(updates)} klientów w 2 wersjach")

s20 = [o for o in orders20 if o["status"] in OK_STATUS]
s20_ids = {o["order_id"] for o in s20}
i20 = [i for i in items20 if i["order_id"] in s20_ids]
rev20 = sum(net(i) for i in i20)
print(f"W7  silver_orders_20260120={len(s20)}  fact 2026-01-20={len(i20)}  przychód={money(rev20)}")
print(f"    v_fact_order_item_all: {len(silver_items)} + {len(i20)} = {len(silver_items) + len(i20)}")

carts_by_prod: dict[str, int] = defaultdict(int)
for e in events:
    if e["event_type"] == "add_to_cart" and e.get("product_id") is not None:
        carts_by_prod[str(e["product_id"])] += 1
cat_carts: dict[str, int] = defaultdict(int)
cat_rev: dict[str, Decimal] = defaultdict(Decimal)
for pid, n in carts_by_prod.items():
    cat_carts[prod_by_id[pid]["category"]] += n
for i in silver_items:
    cat_rev[prod_by_id[i["product_id"]]["category"]] += net(i)
print("W8  koszyki vs przychód wg kategorii (sortowane po koszykach):")
for cat, n in sorted(cat_carts.items(), key=lambda kv: -kv[1]):
    print(f"       {cat:12} koszyki {n:4}   przychód {money(cat_rev[cat])}")

as_was: dict[str, Decimal] = defaultdict(Decimal)
as_is: dict[str, Decimal] = defaultdict(Decimal)
new_city = {u["customer_id"]: u["city"] for u in updates}
for i in silver_items:
    cid = order_by_id[i["order_id"]]["customer_id"]
    old = cust_by_id[cid]["city"]
    as_was[old] += net(i)
    as_is[new_city.get(cid, old)] += net(i)
print("W9.1/9.2 miasta, w których as-was != as-is:")
for city in sorted(set(as_was) | set(as_is)):
    a, b = as_was.get(city, Decimal(0)), as_is.get(city, Decimal(0))
    if a != b:
        print(f"       {city:12} as-was {money(a):>14}   as-is {money(b):>14}   Δ {money(b - a):>14}")

cart16 = {e["customer_id"] for e in events if e["event_type"] == "add_to_cart" and e["dt"] == "2026-01-16"}
buy16 = {o["customer_id"] for o in silver_orders if o["order_ts"][:10] == "2026-01-16"}
no_buy = cart16 - buy16
pct = 100 * len(no_buy) / len(cart16)
print(f"W9.4 klienci z add_to_cart 16.01 = {len(cart16)}, bez zakupu = {len(no_buy)} ({pct:.1f}%)")
