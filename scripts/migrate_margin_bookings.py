# scripts/migrate_margin_bookings.py

from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "campaign_hub.db"
# при необходимости поправь путь:
# DB_PATH = Path("campaign_hub.db")

def add_column_if_missing(cur, table: str, col: str, coltype: str):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col in cols:
        print(f"{table}.{col} уже существует — пропускаю")
        return
    print(f"Добавляю {table}.{col} {coltype}")
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Добавляем недостающие колонки
    add_column_if_missing(cur, "bookings", "client_brand",      "TEXT")
    add_column_if_missing(cur, "bookings", "payout_model",      "TEXT")
    add_column_if_missing(cur, "bookings", "inventory",         "REAL")
    add_column_if_missing(cur, "bookings", "budget_before_vat", "REAL")

    conn.commit()

    # 2. client_brand: по сути дубликат brand / client_name
    print("Заполняю client_brand…")
    cur.execute("""
        UPDATE bookings
        SET client_brand = COALESCE(client_brand, brand, client_name)
    """)

    # 3. payout_model: просто синоним buying_model
    print("Заполняю payout_model из buying_model…")
    cur.execute("""
        UPDATE bookings
        SET payout_model = COALESCE(payout_model, buying_model)
    """)

    # 4. inventory:
    #    берём commercial, если есть, иначе план, иначе факт
    print("Заполняю inventory из *_inventory…")
    cur.execute("""
        UPDATE bookings
        SET inventory = COALESCE(
            inventory,
            inventory_commercial,
            inventory_total_plan,
            inventory_fact
        )
    """)

    # 5. budget_before_vat:
    #    если модель закупки CPM* → (inventory / 1000) * price_unit
    #    иначе → inventory * price_unit
    #    (если чего‑то не хватает — оставляем NULL)
    print("Считаю budget_before_vat…")
    cur.execute("""
        UPDATE bookings
        SET budget_before_vat = CASE
            WHEN UPPER(COALESCE(payout_model, buying_model)) LIKE 'CPM%%'
                 AND inventory IS NOT NULL
                 AND price_unit IS NOT NULL
              THEN (inventory / 1000.0) * price_unit
            WHEN inventory IS NOT NULL
                 AND price_unit IS NOT NULL
              THEN inventory * price_unit
            ELSE NULL
        END
    """)

    conn.commit()
    conn.close()
    print("OK: миграция для margin завершена")

if __name__ == "__main__":
    main()
