"""Generates the Aurora e-commerce sample dataset used across all 3 days."""
from __future__ import annotations

import csv
import json
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Dane leżą od razu w układzie prefiksów S3, żeby upload był jednym
# przeciągnięciem katalogu bronze/ zamiast klikania folderów w konsoli.
PACKAGE = ROOT / "Data_Lakes_i_Data_Warehouses"
DAY1 = PACKAGE / "Dzien1_Data_Lakes" / "dane" / "bronze"
DAY3 = PACKAGE / "Dzien3_Data_Warehouse" / "dane" / "bronze"

random.seed(20260813)

CITIES = [
    ("Warszawa", "mazowieckie"),
    ("Kraków", "małopolskie"),
    ("Gdańsk", "pomorskie"),
    ("Wrocław", "dolnośląskie"),
    ("Poznań", "wielkopolskie"),
    ("Łódź", "łódzkie"),
    ("Lublin", "lubelskie"),
    ("Katowice", "śląskie"),
    ("Szczecin", "zachodniopomorskie"),
    ("Białystok", "podlaskie"),
]

FIRST = [
    "Anna", "Piotr", "Kasia", "Marek", "Ola", "Tomek", "Magda", "Bartek",
    "Natalia", "Jakub", "Ewa", "Adam", "Zofia", "Michał", "Julia", "Paweł",
    "Agnieszka", "Krzysztof", "Karolina", "Mateusz",
]
LAST = [
    "Nowak", "Kowalski", "Wiśniewska", "Wójcik", "Kowalczyk", "Kamińska",
    "Lewandowski", "Zielińska", "Szymański", "Woźniak", "Dąbrowska",
    "Kozłowski", "Jankowska", "Mazur", "Kwiatkowska", "Krawczyk",
]

PRODUCTS = [
    (101, "AUR-TV-55", "Telewizor Aurora 55\" 4K", "TV", "Aurora", 2299.00),
    (102, "AUR-TV-65", "Telewizor Aurora 65\" OLED", "TV", "Aurora", 4499.00),
    (103, "SNY-TV-50", "Telewizor Sony 50\" LED", "TV", "Sony", 1999.00),
    (104, "AUR-PH-A1", "Smartfon Aurora A1", "Telefony", "Aurora", 1299.00),
    (105, "AUR-PH-X3", "Smartfon Aurora X3 Pro", "Telefony", "Aurora", 2499.00),
    (106, "SMS-PH-S24", "Samsung Galaxy S24", "Telefony", "Samsung", 3799.00),
    (107, "APL-PH-15", "iPhone 15", "Telefony", "Apple", 4299.00),
    (108, "AUR-LT-14", "Laptop Aurora 14\" i5", "Komputery", "Aurora", 3199.00),
    (109, "AUR-LT-16", "Laptop Aurora 16\" i7", "Komputery", "Aurora", 5299.00),
    (110, "LNV-LT-15", "Lenovo IdeaPad 15", "Komputery", "Lenovo", 2799.00),
    (111, "AUR-HP-NC", "Słuchawki Aurora Noise Cancel", "Audio", "Aurora", 399.00),
    (112, "SNY-HP-WH", "Sony WH-1000XM5", "Audio", "Sony", 1299.00),
    (113, "AUR-SP-BT", "Głośnik Bluetooth Aurora Mini", "Audio", "Aurora", 149.00),
    (114, "AUR-WM-9", "Pralka Aurora 9kg", "AGD", "Aurora", 1899.00),
    (115, "BSH-WM-8", "Pralka Bosch 8kg", "AGD", "Bosch", 2499.00),
    (116, "AUR-FR-300", "Lodówka Aurora 300l", "AGD", "Aurora", 2199.00),
    (117, "AUR-MW-20", "Mikrofalówka Aurora 20l", "AGD", "Aurora", 349.00),
    (118, "AUR-VC-CY", "Odkurzacz Aurora Cyclone", "AGD", "Aurora", 499.00),
    (119, "AUR-KB-MX", "Klawiatura mechaniczna Aurora", "Akcesoria", "Aurora", 289.00),
    (120, "AUR-MS-WL", "Mysz bezprzewodowa Aurora", "Akcesoria", "Aurora", 89.00),
    (121, "AUR-CH-65", "Ładowarka 65W Aurora", "Akcesoria", "Aurora", 129.00),
    (122, "AUR-CS-SIL", "Etui silikonowe Aurora", "Akcesoria", "Aurora", 49.00),
    (123, "AUR-TB-11", "Tablet Aurora 11\"", "Komputery", "Aurora", 1599.00),
    (124, "AUR-CAM-4K", "Kamera sportowa Aurora 4K", "Audio", "Aurora", 699.00),
    (125, "AUR-WATCH", "Smartwatch Aurora Fit", "Telefony", "Aurora", 599.00),
]

CHANNELS = ["web", "mobile", "marketplace"]
STATUSES = ["paid", "paid", "paid", "paid", "shipped", "shipped", "cancelled"]
SEGMENTS = ["standard", "standard", "standard", "premium", "vip"]
EVENT_TYPES = ["page_view", "page_view", "add_to_cart", "search", "purchase"]


DISCOUNT_STEPS = (0, 20, 50, 100)
MAX_DISCOUNT_SHARE = 0.3


def cap_discount(discount: int, line_value: float) -> int:
    """Keep discounts below a third of the line, so no order line ends up negative.

    Etui za 49 PLN ze 100 PLN rabatu dawało ujemny przychód i pytania na sali,
    których ćwiczenie nie chce wywoływać.
    """
    if discount <= MAX_DISCOUNT_SHARE * line_value:
        return discount
    return max(d for d in DISCOUNT_STEPS if d <= MAX_DISCOUNT_SHARE * line_value)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    customers = []
    for i in range(1, 81):
        city, voiv = random.choice(CITIES)
        signup = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 400))
        customers.append(
            {
                "customer_id": f"C{i:04d}",
                "email": f"user{i:03d}@aurora.example",
                "first_name": random.choice(FIRST),
                "last_name": random.choice(LAST),
                "city": city,
                "voivodeship": voiv,
                "segment": random.choice(SEGMENTS),
                "signup_date": signup.strftime("%Y-%m-%d"),
            }
        )

    products = [
        {
            "product_id": p[0],
            "sku": p[1],
            "product_name": p[2],
            "category": p[3],
            "brand": p[4],
            "unit_price_pln": f"{p[5]:.2f}",
        }
        for p in PRODUCTS
    ]

    days = [datetime(2026, 1, 15), datetime(2026, 1, 16)]
    orders_by_day: dict[str, list[dict]] = {d.strftime("%Y-%m-%d"): [] for d in days}
    items: list[dict] = []
    events: list[dict] = []
    order_seq = 1000

    for day in days:
        n_orders = 90 if day.day == 15 else 110
        for _ in range(n_orders):
            order_seq += 1
            cust = random.choice(customers)
            ts = day + timedelta(minutes=random.randint(8 * 60, 21 * 60), seconds=random.randint(0, 59))
            status = random.choice(STATUSES)
            channel = random.choice(CHANNELS)
            n_items = random.randint(1, 4)
            chosen = random.sample(PRODUCTS, n_items)
            orders_by_day[day.strftime("%Y-%m-%d")].append(
                {
                    "order_id": f"O{order_seq}",
                    "customer_id": cust["customer_id"],
                    "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": status,
                    "channel": channel,
                    "delivery_city": cust["city"],
                }
            )
            for prod in chosen:
                qty = random.randint(1, 3)
                disc = random.choice([0, 0, 0, 20, 50, 100])
                disc = cap_discount(disc, qty * prod[5])
                items.append(
                    {
                        "order_id": f"O{order_seq}",
                        "product_id": prod[0],
                        "qty": qty,
                        "unit_price_pln": f"{prod[5]:.2f}",
                        "discount_pln": f"{disc:.2f}",
                    }
                )
            for _e in range(random.randint(2, 6)):
                et = random.choice(EVENT_TYPES)
                # Spread events over the products actually in the basket instead of
                # always reusing the last one from the loop above.
                ev_prod = chosen[len(events) % len(chosen)]
                events.append(
                    {
                        "event_id": f"E{len(events)+1:06d}",
                        "event_ts": (ts - timedelta(minutes=random.randint(0, 40))).strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        ),
                        "customer_id": cust["customer_id"],
                        "event_type": et,
                        "page": random.choice(
                            ["/home", "/search", "/product", "/cart", "/checkout"]
                        ),
                        "product_id": ev_prod[0] if et != "search" else None,
                        "dt": day.strftime("%Y-%m-%d"),
                    }
                )

    # Browsing sessions that never convert. Without them every add_to_cart belongs to
    # somebody who also placed an order, and the funnel question in W9 answers ~2%.
    # Own Random instance so the rest of the dataset stays byte-identical.
    browse_rng = random.Random(20260814)
    for day in days:
        day_key = day.strftime("%Y-%m-%d")
        ordered_today = {o["customer_id"] for o in orders_by_day[day_key]}
        for cust in customers:
            if cust["customer_id"] in ordered_today:
                continue
            ts = day + timedelta(minutes=browse_rng.randint(8 * 60, 21 * 60))
            session = ["page_view"] * browse_rng.randint(1, 3)
            if browse_rng.random() < 0.7:
                session.append("add_to_cart")
            else:
                session.append("search")
            for offset, et in enumerate(session):
                prod = browse_rng.choice(PRODUCTS)
                events.append(
                    {
                        "event_id": f"B{len(events)+1:06d}",
                        "event_ts": (ts + timedelta(minutes=offset * 3)).strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        ),
                        "customer_id": cust["customer_id"],
                        "event_type": et,
                        "page": "/search" if et == "search" else "/product",
                        "product_id": prod[0] if et != "search" else None,
                        "dt": day_key,
                    }
                )

    # Intentionally messy bronze extras: a second date format row is NOT added;
    # we keep CSV clean so Athena works in class. Messiness is in JSON + denormalized city.

    write_csv(
        DAY1 / "customers" / "customers.csv",
        customers,
        [
            "customer_id",
            "email",
            "first_name",
            "last_name",
            "city",
            "voivodeship",
            "segment",
            "signup_date",
        ],
    )
    write_csv(
        DAY1 / "products" / "products.csv",
        products,
        ["product_id", "sku", "product_name", "category", "brand", "unit_price_pln"],
    )
    write_csv(
        DAY1 / "order_items" / "order_items.csv",
        items,
        ["order_id", "product_id", "qty", "unit_price_pln", "discount_pln"],
    )
    for day_key, rows in orders_by_day.items():
        write_csv(
            DAY1 / "orders" / f"dt={day_key}" / "orders.csv",
            rows,
            ["order_id", "customer_id", "order_ts", "status", "channel", "delivery_city"],
        )

    events_dir = DAY1 / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    by_day: dict[str, list[dict]] = {}
    for ev in events:
        by_day.setdefault(ev["dt"], []).append(ev)
    for day_key, rows in by_day.items():
        path = events_dir / f"dt={day_key}" / "events.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                payload = {k: v for k, v in row.items() if k != "dt"}
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    # Day 3: 8 customers change city and/or segment (SCD2)
    changed_ids = [f"C{i:04d}" for i in [1, 4, 9, 12, 18, 23, 31, 44]]
    updates = []
    for c in customers:
        if c["customer_id"] not in changed_ids:
            continue
        new_city, new_voiv = random.choice(
            [x for x in CITIES if x[0] != c["city"]]
        )
        new_seg = "vip" if c["segment"] != "vip" else "premium"
        updates.append(
            {
                "customer_id": c["customer_id"],
                "email": c["email"],
                "first_name": c["first_name"],
                "last_name": c["last_name"],
                "city": new_city,
                "voivodeship": new_voiv,
                "segment": new_seg,
                "signup_date": c["signup_date"],
                "valid_from": "2026-01-20",
            }
        )
    write_csv(
        DAY3 / "customers_updates" / "customers_updates.csv",
        updates,
        [
            "customer_id",
            "email",
            "first_name",
            "last_name",
            "city",
            "voivodeship",
            "segment",
            "signup_date",
            "valid_from",
        ],
    )

    # Extra day of orders for incremental load
    extra_day = datetime(2026, 1, 20)
    extra_orders = []
    extra_items = []
    for _ in range(40):
        order_seq += 1
        cust = random.choice(customers)
        ts = extra_day + timedelta(minutes=random.randint(9 * 60, 18 * 60))
        extra_orders.append(
            {
                "order_id": f"O{order_seq}",
                "customer_id": cust["customer_id"],
                "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "status": random.choice(["paid", "shipped"]),
                "channel": random.choice(CHANNELS),
                "delivery_city": cust["city"],
            }
        )
        prod = random.choice(PRODUCTS)
        extra_items.append(
            {
                "order_id": f"O{order_seq}",
                "product_id": prod[0],
                "qty": random.randint(1, 2),
                "unit_price_pln": f"{prod[5]:.2f}",
                "discount_pln": "0.00",
            }
        )
    write_csv(
        DAY3 / "orders" / "dt=2026-01-20" / "orders.csv",
        extra_orders,
        ["order_id", "customer_id", "order_ts", "status", "channel", "delivery_city"],
    )
    write_csv(
        DAY3 / "order_items_20260120" / "order_items_2026-01-20.csv",
        extra_items,
        ["order_id", "product_id", "qty", "unit_price_pln", "discount_pln"],
    )

    print("customers", len(customers))
    print("products", len(products))
    print("orders", sum(len(v) for v in orders_by_day.values()), "extra", len(extra_orders))
    print("items", len(items), "extra items", len(extra_items))
    print("events", len(events))
    print("scd updates", len(updates))

    write_advanced_shipments(
        [o for rows in orders_by_day.values() for o in rows], extra_orders
    )


# -----------------------------------------------------------------------------
# Advanced version (Dzien2/3_Data_Warehouse_Zaawansowany): shipments feed
# -----------------------------------------------------------------------------
# Own Random instance, called after everything above has been written, so the
# basic-version files stay byte-identical and all their control numbers hold.
ADV2 = PACKAGE / "Dzien2_Data_Warehouse_Zaawansowany" / "dane" / "bronze"
ADV3 = PACKAGE / "Dzien3_Data_Warehouse_Zaawansowany" / "dane" / "bronze"
CARRIERS = ["InPost", "DPD", "DHL", "Poczta Polska"]
SHIPMENT_FIELDS = ["shipment_id", "order_id", "carrier", "shipped_ts", "delivered_ts"]


def write_advanced_shipments(day1_orders: list[dict], day20_orders: list[dict]) -> None:
    """Shipments arrive later than orders, so the fulfillment fact keeps changing.

    Day 2 file: every order already `shipped` on 15/16.01 has a shipment; part of
    them is still in transit (empty delivered_ts). Orders in status `paid` have
    no shipment yet -> the unknown-member row (-1) in the date dimension.
    Day 3 file (dated 2026-01-21): deliveries for the in-transit ones, late
    shipments for part of the `paid` orders and shipments for 20.01 - the same
    order_id can appear in both files, which is exactly what MERGE is for.
    """
    rng = random.Random(20260903)
    fmt = "%Y-%m-%d %H:%M:%S"
    seq = 5000
    day2_rows: list[dict] = []
    day3_rows: list[dict] = []
    in_transit: list[tuple[dict, datetime]] = []

    for o in day1_orders:
        if o["status"] != "shipped":
            continue
        seq += 1
        ordered = datetime.strptime(o["order_ts"], fmt)
        shipped = ordered + timedelta(hours=rng.randint(6, 30))
        delivered = ""
        if rng.random() < 0.7:
            delivered = (shipped + timedelta(hours=rng.randint(20, 70))).strftime(fmt)
        else:
            in_transit.append((o, shipped))
        day2_rows.append(
            {
                "shipment_id": f"S{seq}",
                "order_id": o["order_id"],
                "carrier": rng.choice(CARRIERS),
                "shipped_ts": shipped.strftime(fmt),
                "delivered_ts": delivered,
            }
        )
    by_order = {r["order_id"]: r for r in day2_rows}

    # (a) the in-transit parcels get delivered - same shipment_id, filled delivered_ts
    for o, shipped in in_transit:
        row = dict(by_order[o["order_id"]])
        row["delivered_ts"] = (shipped + timedelta(hours=rng.randint(40, 96))).strftime(fmt)
        day3_rows.append(row)
    # (b) late-arriving shipments for about half of the `paid` orders from 15/16.01
    for o in day1_orders:
        if o["status"] != "paid" or rng.random() >= 0.5:
            continue
        seq += 1
        shipped = datetime(2026, 1, 17) + timedelta(hours=rng.randint(8, 80))
        day3_rows.append(
            {
                "shipment_id": f"S{seq}",
                "order_id": o["order_id"],
                "carrier": rng.choice(CARRIERS),
                "shipped_ts": shipped.strftime(fmt),
                "delivered_ts": (shipped + timedelta(hours=rng.randint(20, 60))).strftime(fmt)
                if rng.random() < 0.6
                else "",
            }
        )
    # (c) shipments for the `shipped` orders of 20.01 - none delivered yet
    for o in day20_orders:
        if o["status"] != "shipped":
            continue
        seq += 1
        ordered = datetime.strptime(o["order_ts"], fmt)
        day3_rows.append(
            {
                "shipment_id": f"S{seq}",
                "order_id": o["order_id"],
                "carrier": rng.choice(CARRIERS),
                "shipped_ts": (ordered + timedelta(hours=rng.randint(6, 30))).strftime(fmt),
                "delivered_ts": "",
            }
        )

    write_csv(ADV2 / "shipments" / "shipments.csv", day2_rows, SHIPMENT_FIELDS)
    write_csv(ADV3 / "shipments_20260121" / "shipments_2026-01-21.csv", day3_rows, SHIPMENT_FIELDS)
    # Advanced Day 3 is one upload tree: the basic increment plus the shipments update.
    for rel in (
        Path("customers_updates") / "customers_updates.csv",
        Path("orders") / "dt=2026-01-20" / "orders.csv",
        Path("order_items_20260120") / "order_items_2026-01-20.csv",
    ):
        dst = ADV3 / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(DAY3 / rel, dst)
    print("adv shipments day2", len(day2_rows), "in transit", len(in_transit))
    print("adv shipments day3", len(day3_rows))


if __name__ == "__main__":
    main()
