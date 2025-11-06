#!/usr/bin/env python3
"""
Messy e-commerce generator with CDC (Raw -> Bronze -> Silver -> Gold)

Tables emitted per day (CSV under ./data/raw/YYYY-MM-DD/):
  - customers.csv
  - products.csv
  - orders.csv
  - orderItems.csv
  - payments.csv (optional with --include-payments)

Features to clean later:
  - camelCase headers (e.g., orderDate, productReleaseDate, holaMundo)
  - NULLs, duplicates, inconsistent casing, leading/trailing spaces
  - Orphan/missing FKs (configurable)
  - Amounts sometimes typed as strings
  - CDC columns: op {I,U,D}, eventTime, seqNum
  - Late arrivals: eventTime < file_date

Star schema (Gold idea):
  - dim_customer, dim_product, dim_date, fact_order_item
"""

import argparse
import csv
import os
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Optional

# ----------------------------- Helpers -----------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def rand_choice_weighted(options: List[str], weights: Optional[List[float]] = None) -> str:
    return random.choices(options, weights=weights, k=1)[0]

def maybe_null(value: Any, null_rate: float) -> Any:
    return None if random.random() < null_rate else value

def maybe_pad_spaces(value: Optional[str], prob: float = 0.2) -> Optional[str]:
    if value is None:
        return value
    if random.random() < prob:
        left = " " * random.randint(0, 2)
        right = " " * random.randint(0, 2)
        return f"{left}{value}{right}"
    return value

def random_name() -> Tuple[str, str]:
    firsts = ["Ana","Luis","María","Juan","Lucía","Carlos","Sofía","Pablo","Laura","Miguel","Marta","Javier"]
    lasts  = ["García","López","Martínez","Sánchez","González","Rodríguez","Fernández","Pérez","Gómez","Díaz"]
    return random.choice(firsts), random.choice(lasts)

def random_email(first: str, last: str) -> str:
    domains = ["example.com","mail.com","correo.es","test.org"]
    sep = random.choice([".","_","","-"])
    return f"{first.lower()}{sep}{last.lower()}@{random.choice(domains)}"

def random_category() -> str:
    # deliberately inconsistent
    cats = ["electronics","Electronics","Electrónica","home","Hogar","sports","Sports","Juguetes","toys"]
    return random.choice(cats)

def random_product_name(category: str) -> str:
    base = {
        "electronics": ["Headphones","Smartphone","Tablet","Camera","Monitor","Keyboard"],
        "home": ["Lamp","Vacuum","Blender","Toaster","Air Purifier","Kettle"],
        "sports": ["Football","Basketball","Tennis Racket","Yoga Mat","Cycling Helmet"],
        "toys": ["Board Game","Doll","Action Figure","Puzzle","Lego Set"],
        "Electronics": ["Bluetooth Speaker","Smartwatch","Webcam","Printer"],
        "Hogar": ["Sartén","Cacerola","Cafetera","Plancha"],
        "Sports": ["Running Shoes","Gym Bag","Skipping Rope"],
        "Juguetes": ["Coche Teledirigido","Peluche","Pinturas"]
    }
    k = category if category in base else random.choice(list(base.keys()))
    return random.choice(base[k])

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    if delta.total_seconds() <= 0:
        return start
    offset = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=offset)

def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def duplicate_rows(rows: List[Dict[str, Any]], dupe_rate: float) -> List[Dict[str, Any]]:
    if dupe_rate <= 0: return rows
    dupes = []
    for r in rows:
        if random.random() < dupe_rate:
            dupes.append(r.copy())
    return rows + dupes

def rand_phone() -> str:
    return "".join(random.choices(string.digits, k=9))

def day_event_time(file_date: datetime.date) -> datetime:
    return datetime.combine(file_date, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))

# ----------------------------- Base Generators -----------------------------

def gen_customers_base(n_customers: int, today_dt: datetime, null_rate: float) -> List[Dict[str, Any]]:
    rows = []
    for _ in range(n_customers):
        first, last = random_name()
        cid = str(uuid.uuid4())
        customer_since = random_date(today_dt - timedelta(days=1800), today_dt - timedelta(days=30))
        email = random_email(first, last)
        rows.append({
            "customerId": cid,
            "firstName": maybe_pad_spaces(first if random.random() < 0.7 else first.upper()),
            "lastName": maybe_pad_spaces(last if random.random() < 0.7 else last.lower()),
            "emailAddress": maybe_null(maybe_pad_spaces(email), null_rate),
            "phoneNumber": maybe_null(rand_phone(), null_rate),
            "customerSince": customer_since.strftime("%Y-%m-%d"),
            "isActive": rand_choice_weighted(["Y","N","y","n"], [0.7,0.1,0.15,0.05]),
            "extraField1": rand_choice_weighted(["", "N/A", "legacy", None], [0.5,0.2,0.2,0.1]),
        })
    return rows

def gen_products_base(n_products: int, today_dt: datetime, null_rate: float) -> List[Dict[str, Any]]:
    rows = []
    for _ in range(n_products):
        pid = str(uuid.uuid4())
        cat = random_category()
        name = random_product_name(cat)
        release = random_date(today_dt - timedelta(days=2000), today_dt - timedelta(days=10))
        price = round(random.uniform(3.0, 800.0), 2)
        price_val = str(price) if random.random() < 0.2 else price
        rows.append({
            "productId": pid,
            "productName": maybe_pad_spaces(name),
            "category": maybe_pad_spaces(cat),
            "unitPrice": maybe_null(price_val, null_rate),
            "currency": rand_choice_weighted(["EUR","eur","Usd","USD"], [0.7,0.05,0.1,0.15]),
            "productReleaseDate": release.strftime("%Y-%m-%d"),
            "isDiscontinued": rand_choice_weighted(["Y","N","NO","yes"], [0.05,0.8,0.1,0.05]),
            "extraField2": maybe_null("toBeDropped", 0.5),
        })
    return rows

def gen_orders_items_base(n_orders: int,
                          customers: List[Dict[str, Any]],
                          products: List[Dict[str, Any]],
                          start: datetime,
                          end: datetime,
                          null_rate: float,
                          orphan_fk_rate: float) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    orders, items = [], []
    for _ in range(n_orders):
        order_id = str(uuid.uuid4())
        cust = random.choice(customers)
        customerId = cust["customerId"]
        order_dt = random_date(start, end)
        ship_dt  = order_dt + timedelta(days=random.randint(0, 10))
        status = rand_choice_weighted(["NEW","PAID","SHIPPED","CANCELLED","new","Shipped"], [0.3,0.2,0.25,0.1,0.1,0.05])
        orders.append({
            "orderId": order_id,
            "customerId": maybe_null(customerId, null_rate * 0.5),
            "orderDate": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "shipDate": ship_dt.strftime("%Y-%m-%d"),
            "status": status,
            "shippingAddress": maybe_pad_spaces(f"C/ {random.choice(['Sol','Luna','Mar','Rio'])}, {random.randint(1, 99)}"),
            "totalAmount": None,
            "extraField1": rand_choice_weighted(["", "legacy", None], [0.5,0.3,0.2]),
        })
        # Items 1..5
        n_lines = random.randint(1,5)
        for _line in range(n_lines):
            prod = random.choice(products)
            use_orphan = random.random() < orphan_fk_rate
            product_id = str(uuid.uuid4()) if use_orphan else prod["productId"]
            qty = random.randint(1, 5)
            unit_price = prod["unitPrice"]
            try:
                unit_price_f = float(unit_price)
            except Exception:
                unit_price_f = round(random.uniform(5, 500), 2)
            amount = round(qty * unit_price_f, 2)
            items.append({
                "orderItemId": str(uuid.uuid4()),
                "orderId": order_id,
                "productId": maybe_null(product_id, null_rate),
                "quantity": qty if random.random() > 0.05 else None,
                "unitPrice": unit_price if random.random() > 0.1 else str(unit_price_f),
                "lineAmount": amount if random.random() > 0.2 else str(amount),
                "currency": rand_choice_weighted(["EUR","eur","USD"], [0.8,0.1,0.1]),
                "holaMundo": maybe_pad_spaces("valor_inutil"),
            })
    return orders, items

def gen_payments_base(orders: List[Dict[str, Any]],
                      start: datetime, end: datetime,
                      null_rate: float, orphan_fk_rate: float) -> List[Dict[str, Any]]:
    methods = ["CARD","card","CASH","PayPal","Bizum","BIZUM"]
    rows = []
    for o in orders:
        if random.random() < 0.85:
            amount = round(random.uniform(5, 1500), 2)
            pay_dt = random_date(start, end + timedelta(days=5))
            use_orphan = random.random() < orphan_fk_rate
            order_id = str(uuid.uuid4()) if use_orphan else o["orderId"]
            rows.append({
                "paymentId": str(uuid.uuid4()),
                "orderId": maybe_null(order_id, null_rate * 0.5),
                "paymentMethod": rand_choice_weighted(methods),
                "amount": amount if random.random() > 0.2 else f"{amount}",
                "paymentDate": pay_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "status": rand_choice_weighted(["OK","ok","FAILED","PENDING"], [0.7,0.1,0.1,0.1]),
            })
    return rows

# ----------------------------- CDC Utilities -----------------------------

def attach_insert_cdc(rows: List[Dict[str, Any]],
                      file_date: datetime.date,
                      late_rate: float) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        rr = r.copy()
        rr["op"] = "I"
        rr["seqNum"] = 1
        ev = day_event_time(file_date)
        if random.random() < late_rate and random.random() < 0.5:
            ev = ev - timedelta(days=random.randint(1,2))
        rr["eventTime"] = ev.strftime("%Y-%m-%d %H:%M:%S")
        out.append(rr)
    return out

def make_updates_deletes(previous_rows: List[Dict[str, Any]],
                         file_date: datetime.date,
                         update_rate: float,
                         delete_rate: float,
                         late_rate: float,
                         table: str) -> List[Dict[str, Any]]:
    if not previous_rows:
        return []
    k_upd = max(1, int(len(previous_rows) * update_rate))
    k_del = max(0, int(len(previous_rows) * delete_rate))
    sample_upd = random.sample(previous_rows, k=min(k_upd, len(previous_rows)))
    sample_del = random.sample(previous_rows, k=min(k_del, len(previous_rows)))

    out = []

    # Updates
    for r in sample_upd:
        u = r.copy()
        u["op"] = "U"
        u["seqNum"] = r.get("seqNum", 1) + 1
        # mutate meaningful fields
        if table == "customers":
            if u.get("emailAddress"):
                u["emailAddress"] = u["emailAddress"].replace("@", f"+u{random.randint(1,9)}@")
            u["isActive"] = rand_choice_weighted(["Y","N","y","n"], [0.7,0.1,0.15,0.05])
        elif table == "products":
            # tweak price 5% up/down if numeric
            try:
                p = float(u.get("unitPrice"))
            except Exception:
                p = round(random.uniform(5, 500), 2)
            u["unitPrice"] = round(p * random.choice([0.95, 1.00, 1.05]), 2)
            u["isDiscontinued"] = rand_choice_weighted(["Y","N","NO","yes"], [0.06,0.78,0.1,0.06])
        elif table == "orders":
            u["status"] = rand_choice_weighted(["NEW","PAID","SHIPPED","CANCELLED","RETURNED"], [0.1,0.35,0.35,0.15,0.05])
        elif table == "orderItems":
            # change quantity or price slightly
            q = u.get("quantity")
            if isinstance(q, int) and q is not None:
                u["quantity"] = max(1, q + random.choice([-1, 0, 1]))
            try:
                up = float(u.get("unitPrice"))
            except Exception:
                up = round(random.uniform(5, 500), 2)
            u["unitPrice"] = round(up * random.choice([0.95, 1.0, 1.05]), 2)
        elif table == "payments":
            try:
                amt = float(u.get("amount"))
            except Exception:
                amt = round(random.uniform(5, 1500), 2)
            u["amount"] = round(amt * random.choice([0.9, 1.0, 1.1]), 2)
            u["status"] = rand_choice_weighted(["OK","ok","FAILED","PENDING"], [0.7,0.1,0.1,0.1])
        ev = day_event_time(file_date)
        if random.random() < late_rate:
            ev = ev - timedelta(days=random.randint(1,2))
        u["eventTime"] = ev.strftime("%Y-%m-%d %H:%M:%S")
        out.append(u)

    # Deletes
    for r in sample_del:
        d = {k: r[k] for k in r.keys()}
        d["op"] = "D"
        d["seqNum"] = r.get("seqNum", 1) + 1
        ev = day_event_time(file_date)
        d["eventTime"] = ev.strftime("%Y-%m-%d %H:%M:%S")
        out.append(d)

    return out

# ----------------------------- Main -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Messy e-commerce data generator with CDC.")
    parser.add_argument("--output-dir", default="data/raw", help="Base output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=300)
    parser.add_argument("--orders", type=int, default=2000)

    parser.add_argument("--days", type=int, default=14, help="Base window for order dates (last N days) for day 1")
    parser.add_argument("--cdc-days", type=int, default=3, help="Number of daily CDC snapshots to emit")
    parser.add_argument("--update-rate", type=float, default=0.15, help="Fraction of rows updated per day")
    parser.add_argument("--delete-rate", type=float, default=0.03, help="Fraction of rows deleted per day")
    parser.add_argument("--late-rate", type=float, default=0.05, help="Probability of late arrival per row")

    parser.add_argument("--null-rate", type=float, default=0.05, help="NULL injection probability")
    parser.add_argument("--dupe-rate", type=float, default=0.05, help="Duplicate row probability (applied day 1)")
    parser.add_argument("--orphan-fk-rate", type=float, default=0.03, help="Probability to emit orphan FKs")

    parser.add_argument("--include-payments", action="store_true", help="Generate payments table")

    args = parser.parse_args()
    random.seed(args.seed)

    # Day references
    today = datetime.utcnow().date()
    day1_date = today  # You can shift if you want historical folders
    # Order base window for day 1
    start_orders = datetime.combine(day1_date - timedelta(days=args.days-1), datetime.min.time())
    end_orders   = datetime.combine(day1_date, datetime.max.time())

    # ---------- Day 1 base data (all op=I) ----------
    base_out_dir = os.path.join(args.output_dir, str(day1_date))
    ensure_dir(base_out_dir)

    customers_base = gen_customers_base(args.customers, datetime.utcnow(), args.null_rate)
    products_base  = gen_products_base(args.products, datetime.utcnow(), args.null_rate)
    orders_base, items_base = gen_orders_items_base(args.orders, customers_base, products_base,
                                                    start_orders, end_orders,
                                                    args.null_rate, args.orphan_fk_rate)
    payments_base = gen_payments_base(orders_base, start_orders, end_orders,
                                      args.null_rate, args.orphan_fk_rate) if args.include_payments else []

    # Duplicates only in day 1 (to clean in Bronze/Silver)
    customers_day1 = duplicate_rows(customers_base, args.dupe_rate)
    products_day1  = duplicate_rows(products_base,  args.dupe_rate * 0.5)
    orders_day1    = duplicate_rows(orders_base,    args.dupe_rate * 0.3)
    items_day1     = duplicate_rows(items_base,     args.dupe_rate * 0.2)
    payments_day1  = duplicate_rows(payments_base,  args.dupe_rate * 0.2) if payments_base else []

    # Attach CDC (I, seq=1, eventTime possibly late)
    customers_day1 = attach_insert_cdc(customers_day1, day1_date, args.late_rate)
    products_day1  = attach_insert_cdc(products_day1,  day1_date, args.late_rate)
    orders_day1    = attach_insert_cdc(orders_day1,    day1_date, args.late_rate)
    items_day1     = attach_insert_cdc(items_day1,     day1_date, args.late_rate)
    if payments_day1:
        payments_day1 = attach_insert_cdc(payments_day1, day1_date, args.late_rate)

    # Write Day 1
    def write_all(out_dir: str,
                  customers_rows, products_rows, orders_rows, items_rows, payments_rows):
        write_csv(os.path.join(out_dir, "customers.csv"), customers_rows, [
            "customerId","firstName","lastName","emailAddress","phoneNumber","customerSince","isActive","extraField1",
            "op","eventTime","seqNum"
        ])
        write_csv(os.path.join(out_dir, "products.csv"), products_rows, [
            "productId","productName","category","unitPrice","currency","productReleaseDate","isDiscontinued","extraField2",
            "op","eventTime","seqNum"
        ])
        write_csv(os.path.join(out_dir, "orders.csv"), orders_rows, [
            "orderId","customerId","orderDate","shipDate","status","shippingAddress","totalAmount","extraField1",
            "op","eventTime","seqNum"
        ])
        write_csv(os.path.join(out_dir, "orderItems.csv"), items_rows, [
            "orderItemId","orderId","productId","quantity","unitPrice","lineAmount","currency","holaMundo",
            "op","eventTime","seqNum"
        ])
        if payments_rows is not None:
            write_csv(os.path.join(out_dir, "payments.csv"), payments_rows, [
                "paymentId","orderId","paymentMethod","amount","paymentDate","status",
                "op","eventTime","seqNum"
            ])

    write_all(base_out_dir, customers_day1, products_day1, orders_day1, items_day1,
              payments_day1 if args.include_payments else None)

    # Keep last emitted states to build U/D on top (seqNum will increase)
    last_customers = customers_day1
    last_products  = products_day1
    last_orders    = orders_day1
    last_items     = items_day1
    last_payments  = payments_day1 if args.include_payments else []

    # ---------- Subsequent CDC days: only mutations (U/D) ----------
    for d in range(2, args.cdc_days + 1):
        file_date = day1_date + timedelta(days=d-1)
        out_dir = os.path.join(args.output_dir, str(file_date))
        ensure_dir(out_dir)

        cust_mut = make_updates_deletes(last_customers, file_date, args.update_rate, args.delete_rate, args.late_rate, "customers")
        prod_mut = make_updates_deletes(last_products,  file_date, args.update_rate, args.delete_rate, args.late_rate, "products")
        ord_mut  = make_updates_deletes(last_orders,    file_date, args.update_rate, args.delete_rate, args.late_rate, "orders")
        itm_mut  = make_updates_deletes(last_items,     file_date, args.update_rate, args.delete_rate, args.late_rate, "orderItems")
        pay_mut  = make_updates_deletes(last_payments,  file_date, args.update_rate, args.delete_rate, args.late_rate, "payments") \
                   if args.include_payments else []

        # Update "last_*" to include emitted mutations (so next day can keep seqNum chain)
        # Note: we append mutations as the "latest known state" for subsequent days
        last_customers = last_customers + cust_mut
        last_products  = last_products + prod_mut
        last_orders    = last_orders + ord_mut
        last_items     = last_items + itm_mut
        if args.include_payments:
            last_payments = last_payments + pay_mut

        # Write ONLY mutations for the day (as typical CDC feeds)
        write_all(out_dir, cust_mut, prod_mut, ord_mut, itm_mut, pay_mut if args.include_payments else None)

    print("Generation complete.")
    print(f"Base day: {day1_date}  |  CDC days total: {args.cdc_days}")
    print(f"Output root: {args.output_dir}")
    print("Each date folder contains CSVs with camelCase + CDC columns (op, eventTime, seqNum).")

if __name__ == "__main__":
    main()
