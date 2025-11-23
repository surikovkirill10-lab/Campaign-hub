# scripts/import_bookings_excel.py
# Usage:
#   python scripts/import_bookings_excel.py "/path/to/Total Direct'25 (1).xlsx" --sheet "Свод" --db campaign_hub.db

import argparse, json, sqlite3
from datetime import datetime
import pandas as pd

MAP = {
    'Месяц размещения': 'month_str',
    'ID РК в системе': 'campaign_id',
    'Сейлз': 'sales_manager',
    'Аккаунт': 'account_manager',
    'Бюджет После НДС': 'budget_after_vat',
    'Номер Акта': 'act_number',
    'ID акта': 'act_id',
    'ID Договора': 'contract_id',
    'ID Изначального договора': 'initial_contract_id',
    'Стоимость единицы с ндс': 'price_unit_with_vat',
    'Тип договора': 'contract_type',
    'Статус проекта': 'status',
    'Агентство/Клиент': None,      # теперь разбираем ниже — только АГЕНТСТВО (слева)
    'Юр.лицо': 'legal_entity',
    'Маревен ': None,
    'Бренд': 'brand',               # читаем для справки; КЛИЕНТАМИ считаем бренды (clients.name)
    'Название РК': 'name',
    'Дата старта': 'start_date',
    'Дата завершения': 'end_date',
    'Формат': 'format',
    'Модель закупки': 'buying_model',
    'Инвентарь факт': 'inventory_fact',
    'Тотал инвентарь': 'inventory_total_plan',
    'Комерченский инвентарь': 'inventory_commercial',
    'Бонусный инвентарь': 'inventory_bonus',
    'Цена за единицу с учетом бонуса': 'price_unit_with_bonus',
    'Цена за единицу': 'price_unit',
    'Бюджет клиентский до НДС': 'budget_client_net',
    'Бюджет клиентский с НДС': 'budget_client_gross',
    'ВЗ, %': 'vz_percent',
    'Сумма возврата': 'refund_amount',
    'СРМ/СРС в площадку': 'cpm_cpc_to_platform',
    'Бюджет в нас после вычета СК': 'budget_after_sk',
    'Дата оплаты план': 'plan_payment_date',
    'Дата оплаты факт': 'fact_payment_date',
    'Комментарий для сейлзов': 'comment_sales',
    'Комментарий для акков': 'comment_accounts',
    'Помощь Ромы': None
}

def get_or_create(conn, table, name):
    if not name or str(name).strip() == "":
        return None
    name = str(name).strip()
    row = conn.execute(f"SELECT id FROM {table} WHERE name = ?", (name,)).fetchone()
    if row:
        return row[0]
    cur = conn.execute(f"INSERT INTO {table}(name) VALUES (?)", (name,))
    return cur.lastrowid

def as_date(x):
    if pd.isna(x): return None
    try:
        if isinstance(x, (pd.Timestamp,)):
            return x.strftime("%Y-%m-%d")
        s = str(x)
        dt = pd.to_datetime(s, errors='coerce', dayfirst=True)
        if pd.isna(dt): return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def coerce_float(x):
    if pd.isna(x): return None
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(" ", "").replace(",", "."))  # 10 000,50 -> 10000.50
        except Exception:
            return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("xlsx")
    ap.add_argument("--sheet", default="Свод")
    ap.add_argument("--db", default="campaign_hub.db")
    args = ap.parse_args()

    df = pd.read_excel(args.xlsx, sheet_name=args.sheet)
    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON")

    inserted = 0
    updated = 0

    for _, row in df.iterrows():
        rec = {}

        # КЛИЕНТ = БРЕНД
        client_name = None
        if 'Бренд' in df.columns:
            val_brand = row.get('Бренд')
            if val_brand is not None and not pd.isna(val_brand):
                client_name = str(val_brand).strip() or None

        # АГЕНТСТВО = левая часть поля "Агентство/Клиент"
        agency_name = None
        if 'Агентство/Клиент' in df.columns:
            val_ac = row.get('Агентство/Клиент')
            if val_ac is not None and not pd.isna(val_ac):
                parts = str(val_ac).split("/", 1)
                agency_name = (parts[0].strip() or None) if parts else None

        for src, dst in MAP.items():
            if dst is None:
                continue
            v = row.get(src, None)
            if dst in ("start_date", "end_date", "plan_payment_date", "fact_payment_date"):
                rec[dst] = as_date(v)
            elif dst in ("campaign_id",):
                try:
                    rec[dst] = int(v) if v is not None and not pd.isna(v) else None
                except Exception:
                    rec[dst] = None
            elif isinstance(v, (int, float)) or (isinstance(v, str) and v.strip() != ""):
                if dst.startswith(("budget","inventory","price")) or dst in ("vz_percent","refund_amount","cpm_cpc_to_platform"):
                    rec[dst] = coerce_float(v)
                else:
                    rec[dst] = str(v).strip()
            else:
                rec[dst] = None

        client_id = get_or_create(conn, "clients", client_name) if client_name else None
        agency_id = get_or_create(conn, "agencies", agency_name) if agency_name else None
        rec["client_id"] = client_id
        rec["agency_id"] = agency_id

        ms = row.get("Месяц размещения")
        rec["month_str"] = str(ms) if ms is not None and not pd.isna(ms) else None

        raw = { c: (None if pd.isna(row[c]) else row[c]) for c in df.columns }
        rec["raw_json"] = json.dumps(raw, ensure_ascii=False, default=str)  # ВАЖНО: Timestamp -> str

        if not rec.get("name"):
            rec["name"] = f"РК {rec.get('campaign_id') or ''}".strip()

        try:
            keys = list(rec.keys())
            cols = ", ".join(keys)
            placeholders = ", ".join([":"+k for k in keys])
            conn.execute(f"INSERT INTO bookings ({cols}) VALUES ({placeholders})", rec)
            inserted += 1
        except Exception:
            where = (rec.get("name"), rec.get("start_date"), rec.get("end_date"))
            rowid = conn.execute(
                "SELECT id FROM bookings WHERE name=? AND ifnull(start_date,'')=? AND ifnull(end_date,'')=?",
                where).fetchone()
            if rowid:
                set_clause = ", ".join([f"{k} = :{k}" for k in rec.keys() if k not in ("name","start_date","end_date")])
                rec2 = rec.copy(); rec2["id"] = rowid[0]
                conn.execute(f"UPDATE bookings SET {set_clause} WHERE id = :id", rec2)
                updated += 1

    conn.commit()
    print(json.dumps({"inserted": inserted, "updated": updated}, ensure_ascii=False))

if __name__ == "__main__":
    main()
