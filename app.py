import os
import re
import sqlite3
import psycopg
from psycopg.rows import dict_row
from pathlib import Path
from datetime import date
from functools import wraps
from flask import Flask, g, render_template, request, redirect, url_for, session, flash, send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

DATABASE_URL = os.getenv("DATABASE_URL")
USE_POSTGRES = bool(DATABASE_URL)

def normalize_schema_sql(schema_sql: str) -> str:
    if not USE_POSTGRES:
        return schema_sql
    return schema_sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY').replace('AUTOINCREMENT', '')

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv('DB_PATH', str(BASE_DIR / 'sansu_v5_1.db')))
APP_NAME = 'SANSU AGRIFOOD INTEGRATED FARM SYSTEM'
DASHBOARD_SUBTITLE = 'Mixed Farm Management ERP'
FOOTER_EMAIL = 'sansuagriintegfarm@gmail.com'
MODULES = ['POULTRY', 'HOG', 'FISH']
PARTICIPANT_ROLES = ['Owner', 'Caretaker', 'Investor']
BANK_TYPES = ['Bank', 'GCash', 'Maya', 'Cash on Hand']
TX_TYPES = ['DEPOSIT', 'WITHDRAWAL', 'TRANSFER IN', 'TRANSFER OUT']
POULTRY_TYPES = ['Broiler', 'Free Range', 'Layer', 'Native Chicken']
EXPENSE_CATEGORIES = ['Feeds', 'Medicines', 'Vitamins', 'Vaccines', 'Chicks', 'Labor', 'Utilities', 'Electric Bill', 'Canvass', 'Delivery', 'Logistics', 'Equipment Purchase', 'Repairs / Maintenance', 'Other']
FEED_USAGE_TYPES = ['Direct Use', 'Add to Inventory']

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'sansu-v6-secret')
UPLOAD_DIR = BASE_DIR / 'receipts'
ALLOWED_UPLOADS = {'jpg','jpeg','png','pdf','webp'}
UPLOAD_DIR.mkdir(exist_ok=True)


def safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_date(value, default=''):
    value = (value or '').strip()
    if value:
        return value
    return default or ''


def clean_text(value, default=''):
    value = (value or '').strip()
    return value if value else default


def _connect_db():
    if USE_POSTGRES:
        return psycopg.connect(DATABASE_URL, row_factory=dict_row)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def _adapt_sql(sql, params=()):
    if not isinstance(params, (list, tuple)):
        params = (params,)
    sql = re.sub(r'COLLATE\s+NOCASE', '', sql, flags=re.IGNORECASE)
    sql = re.sub(r"date\(\s*['\"]now['\"]\s*\)", 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    if USE_POSTGRES:
        sql = re.sub(r'"([^"]*)"', lambda m: "'" + m.group(1).replace("'", "''") + "'", sql)
        pieces = []
        in_single = False
        i = 0
        while i < len(sql):
            ch = sql[i]
            if ch == "'":
                pieces.append(ch)
                if in_single and i + 1 < len(sql) and sql[i + 1] == "'":
                    pieces.append(sql[i + 1])
                    i += 2
                    continue
                in_single = not in_single
                i += 1
                continue
            if ch == '%' and in_single:
                pieces.append('%%')
                i += 1
                continue
            if ch == '?' and not in_single:
                pieces.append('%s')
                i += 1
                continue
            pieces.append(ch)
            i += 1
        sql = ''.join(pieces)
    return sql, tuple(params)


def row_value(row, key, default=None):
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return getattr(row, key, default)


def row_dict(row):
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def list_bank_accounts():
    try:
        rows = query("SELECT * FROM bank_accounts ORDER BY account_name, bank_name, id")
        return [row_dict(r) for r in rows]
    except Exception:
        return []


def bank_option_label(account):
    account = row_dict(account)
    account_name = clean_text(account.get('account_name'))
    bank_name = clean_text(account.get('bank_name'))
    account_type = clean_text(account.get('account_type'))
    parts = [p for p in [account_name, bank_name, account_type] if p]
    return ' - '.join(parts) if parts else 'Bank account'


def recompute_bank_balance(account_id):
    account_id = safe_int(account_id)
    if not account_id:
        return 0.0
    opening_row = query('SELECT COALESCE(opening_balance,0) v FROM bank_accounts WHERE id=?', (account_id,), one=True)
    opening = safe_float(row_value(opening_row, 'v', 0))
    tx_row = query(
        'SELECT COALESCE(SUM(CASE WHEN tx_type IN (?,?) THEN amount ELSE -amount END),0) v FROM bank_transactions WHERE account_id=?',
        ('DEPOSIT', 'TRANSFER IN', account_id),
        one=True
    )
    movement = safe_float(row_value(tx_row, 'v', 0))
    current = opening + movement
    execute('UPDATE bank_accounts SET current_balance=? WHERE id=?', (current, account_id))
    return current


def recompute_all_bank_balances():
    for account in list_bank_accounts():
        recompute_bank_balance(row_value(account, 'id'))


def sync_bank_balance(account_id, amount, module_name, entry_date, tx_type, reference_no='', purpose='', notes=''):
    account_id = safe_int(account_id)
    amount = safe_float(amount)
    if not account_id or amount <= 0:
        return False
    execute(
        'INSERT INTO bank_transactions(entry_date,account_id,module_name,tx_type,amount,reference_no,purpose,notes) VALUES(?,?,?,?,?,?,?,?)',
        (entry_date, account_id, module_name, tx_type, amount, clean_text(reference_no), clean_text(purpose), clean_text(notes))
    )
    recompute_bank_balance(account_id)
    return True


def latest_hog_cycle_id(cycle):
    if not cycle:
        return None
    row = query('SELECT id FROM hog_cycles WHERE cycle_id=? ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True)
    return row['id'] if row else None


def latest_fish_cycle_id(cycle):
    if not cycle:
        return None
    row = query('SELECT id FROM fish_cycles WHERE cycle_id=? ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True)
    return row['id'] if row else None


def receipt_preview_url(filename):
    return url_for('view_receipt', filename=filename) if filename else ''


def receipt_download_url(filename):
    return url_for('view_receipt', filename=filename) if filename else ''


def participant_allocations(cycle, finance):
    if not cycle:
        return []
    parts, share = participant_shares(cycle['id'], float(finance.get('profit') or 0))
    allocations = []
    for p in parts:
        allocations.append({
            'id': p['id'],
            'name': p['name'],
            'role': p['role'],
            'profit': round(float(finance.get('profit') or 0), 2),
            'income_share': round(share, 2),
            'share': round(share, 2),
            'capital': round(float(finance.get('capital') or 0), 2),
            'revenue': round(float(finance.get('revenue') or 0), 2),
            'expenses': round(float(finance.get('expenses') or 0), 2),
        })
    return allocations



def get_db():
    if 'db' not in g:
        g.db = _connect_db()
    return g.db


def query(sql, params=(), one=False, commit=False):
    db = get_db()
    sql, params = _adapt_sql(sql, params)
    if USE_POSTGRES:
        with db.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
    else:
        cur = db.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
    if commit:
        db.commit()
    return (rows[0] if rows else None) if one else rows


def execute(sql, params=()):
    db = get_db()
    sql, params = _adapt_sql(sql, params)
    if USE_POSTGRES:
        with db.cursor() as cur:
            if sql.lstrip().upper().startswith('INSERT') and ' RETURNING ' not in sql.upper():
                cur.execute(f"{sql} RETURNING id", params)
                row = cur.fetchone()
                db.commit()
                return row['id'] if row else None
            cur.execute(sql, params)
            row = cur.fetchone() if cur.description else None
        db.commit()
        return row['id'] if row and 'id' in row else None
    cur = db.execute(sql, params)
    db.commit()
    last_id = cur.lastrowid
    cur.close()
    return last_id


@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop('db', None)
    if db:
        db.close()


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return fn(*args, **kwargs)
    return wrapper


def current_role():
    return (session.get('user') or {}).get('role', '')


def is_admin():
    return current_role().lower() == 'admin'


def can_access_module(module_name):
    role = current_role().lower()
    module_name = (module_name or '').upper()
    if role == 'admin':
        return True
    if role == 'secretary':
        return module_name == 'POULTRY'
    return False


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        if not is_admin():
            flash('Admin access required.', 'danger')
            return redirect(url_for('poultry_page'))
        return fn(*args, **kwargs)
    return wrapper


def module_access_required(module_name):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if 'user' not in session:
                return redirect(url_for('login'))
            if not can_access_module(module_name):
                flash('You do not have access to that module.', 'danger')
                return redirect(url_for('poultry_page'))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def allowed_upload(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_UPLOADS


def save_uploaded_receipt(file_storage, prefix='receipt'):
    if not file_storage or not getattr(file_storage, 'filename', ''):
        return None
    filename = secure_filename(file_storage.filename)
    if not filename or not allowed_upload(filename):
        return None
    stem, ext = os.path.splitext(filename)
    safe_name = f"{prefix}_{stem[:40]}{ext.lower()}"
    target = UPLOAD_DIR / safe_name
    counter = 1
    while target.exists():
        target = UPLOAD_DIR / f"{prefix}_{stem[:34]}_{counter}{ext.lower()}"
        counter += 1
    file_storage.save(target)
    return target.name

def init_db():
    schema_sql = """
    CREATE TABLE IF NOT EXISTS users(
        id SERIAL PRIMARY KEY,
        username TEXT UNIQUE,
        password TEXT,
        full_name TEXT,
        role TEXT
    );
    CREATE TABLE IF NOT EXISTS participants(
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        module_name TEXT NOT NULL DEFAULT 'POULTRY',
        notes TEXT,
        active INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS bank_accounts(
        id SERIAL PRIMARY KEY,
        account_name TEXT NOT NULL,
        bank_name TEXT NOT NULL,
        account_type TEXT NOT NULL,
        opening_balance REAL DEFAULT 0,
        current_balance REAL DEFAULT 0,
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS bank_transactions(
        id SERIAL PRIMARY KEY,
        entry_date TEXT NOT NULL,
        account_id INTEGER,
        module_name TEXT,
        tx_type TEXT NOT NULL,
        amount REAL NOT NULL,
        reference_no TEXT,
        purpose TEXT,
        notes TEXT,
        FOREIGN KEY(account_id) REFERENCES bank_accounts(id)
    );
    CREATE TABLE IF NOT EXISTS owner_withdrawals(
        id SERIAL PRIMARY KEY,
        entry_date TEXT NOT NULL,
        account_id INTEGER,
        amount REAL NOT NULL DEFAULT 0,
        reference_no TEXT,
        notes TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(account_id) REFERENCES bank_accounts(id)
    );
    CREATE TABLE IF NOT EXISTS cycles(
        id SERIAL PRIMARY KEY,
        module_name TEXT NOT NULL,
        cycle_name TEXT NOT NULL,
        poultry_type TEXT,
        start_date TEXT NOT NULL,
        end_date TEXT,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS cycle_participants(
        id SERIAL PRIMARY KEY,
        cycle_id INTEGER NOT NULL,
        participant_id INTEGER NOT NULL,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id) ON DELETE CASCADE,
        FOREIGN KEY(participant_id) REFERENCES participants(id)
    );
    CREATE TABLE IF NOT EXISTS capital_entries(
        id SERIAL PRIMARY KEY,
        cycle_id INTEGER,
        module_name TEXT NOT NULL,
        entry_date TEXT NOT NULL,
        source_name TEXT,
        amount REAL DEFAULT 0,
        destination_account_id INTEGER,
        notes TEXT,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id) ON DELETE SET NULL,
        FOREIGN KEY(destination_account_id) REFERENCES bank_accounts(id)
    );
    CREATE TABLE IF NOT EXISTS poultry_batches(
        id SERIAL PRIMARY KEY,
        cycle_id INTEGER,
        poultry_type TEXT NOT NULL,
        batch_name TEXT NOT NULL,
        house_name TEXT,
        start_date TEXT NOT NULL,
        birds_count INTEGER DEFAULT 0,
        supplier TEXT,
        cost REAL DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id)
    );
    CREATE TABLE IF NOT EXISTS poultry_mortality(
        id SERIAL PRIMARY KEY,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        deaths INTEGER DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS poultry_feed_logs(
        id SERIAL PRIMARY KEY,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        feed_type TEXT,
        bags REAL DEFAULT 0,
        amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS poultry_sales(
        id SERIAL PRIMARY KEY,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        buyer TEXT,
        birds_sold INTEGER DEFAULT 0,
        kilos REAL DEFAULT 0,
        price_per_kilo REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS poultry_expenses(
        id SERIAL PRIMARY KEY,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        category TEXT,
        amount REAL DEFAULT 0,
        description TEXT,
        receipt_file TEXT,
        group_ref TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS hog_cycles(
        id SERIAL PRIMARY KEY,
        cycle_id INTEGER,
        pen_name TEXT,
        start_date TEXT,
        heads INTEGER DEFAULT 0,
        source TEXT,
        cost REAL DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT,
        hog_type TEXT DEFAULT 'Other',
        FOREIGN KEY(cycle_id) REFERENCES cycles(id)
    );
    CREATE TABLE IF NOT EXISTS hog_feed_logs(
        id SERIAL PRIMARY KEY,
        hog_cycle_id INTEGER,
        entry_date TEXT,
        feed_type TEXT,
        quantity TEXT,
        amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(hog_cycle_id) REFERENCES hog_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS hog_sales(
        id SERIAL PRIMARY KEY,
        hog_cycle_id INTEGER,
        entry_date TEXT,
        buyer TEXT,
        heads INTEGER DEFAULT 0,
        kilos REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(hog_cycle_id) REFERENCES hog_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS hog_expenses(
        id SERIAL PRIMARY KEY,
        hog_cycle_id INTEGER,
        entry_date TEXT,
        category TEXT,
        amount REAL DEFAULT 0,
        description TEXT,
        receipt_file TEXT,
        receipt_name TEXT,
        group_ref TEXT,
        FOREIGN KEY(hog_cycle_id) REFERENCES hog_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS fish_cycles(
        id SERIAL PRIMARY KEY,
        cycle_id INTEGER,
        period_name TEXT,
        start_date TEXT,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id)
    );
    CREATE TABLE IF NOT EXISTS fish_transactions(
        id SERIAL PRIMARY KEY,
        fish_cycle_id INTEGER,
        entry_date TEXT,
        transaction_type TEXT,
        supplier TEXT,
        buyer TEXT,
        species TEXT,
        kilos REAL DEFAULT 0,
        price_per_kilo REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(fish_cycle_id) REFERENCES fish_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS fish_expenses(
        id SERIAL PRIMARY KEY,
        fish_cycle_id INTEGER,
        entry_date TEXT,
        category TEXT,
        amount REAL DEFAULT 0,
        description TEXT,
        group_ref TEXT,
        receipt_file TEXT,
        receipt_name TEXT,
        FOREIGN KEY(fish_cycle_id) REFERENCES fish_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS feed_inventory(
        id SERIAL PRIMARY KEY,
        module_name TEXT NOT NULL DEFAULT 'POULTRY',
        entry_date TEXT NOT NULL,
        feed_type TEXT NOT NULL,
        sacks REAL DEFAULT 0,
        cost_per_sack REAL DEFAULT 0,
        total_cost REAL DEFAULT 0,
        source_name TEXT,
        usage_type TEXT DEFAULT 'Add to Inventory',
        house_name TEXT,
        cycle_id INTEGER,
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS audit_log(
        id SERIAL PRIMARY KEY,
        event_time TEXT DEFAULT CURRENT_TIMESTAMP,
        event_type TEXT,
        record_type TEXT,
        record_id INTEGER,
        details TEXT
    );
    """

    if USE_POSTGRES:
        db = psycopg.connect(DATABASE_URL, row_factory=dict_row)
        statements = [s.strip() for s in normalize_schema_sql(schema_sql).split(';') if s.strip()]
        with db:
            with db.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
                cur.execute('SELECT username FROM users')
                existing_users = {row['username'] for row in cur.fetchall()}
                if 'admin' not in existing_users:
                    cur.execute(
                        'INSERT INTO users(username,password,full_name,role) VALUES(%s,%s,%s,%s)',
                        ('admin', generate_password_hash('admin123'), 'Administrator', 'Admin')
                    )
                if 'secretary' not in existing_users:
                    cur.execute(
                        'INSERT INTO users(username,password,full_name,role) VALUES(%s,%s,%s,%s)',
                        ('secretary', generate_password_hash('secretary123'), 'Farm Secretary', 'Secretary')
                    )
        db.close()
    else:
        db = sqlite3.connect(DB_PATH)
        db.executescript(schema_sql)
        existing_users = {row[0] for row in db.execute('SELECT username FROM users').fetchall()}
        if 'admin' not in existing_users:
            db.execute(
                'INSERT INTO users(username,password,full_name,role) VALUES(?,?,?,?)',
                ('admin', generate_password_hash('admin123'), 'Administrator', 'Admin')
            )
        if 'secretary' not in existing_users:
            db.execute(
                'INSERT INTO users(username,password,full_name,role) VALUES(?,?,?,?)',
                ('secretary', generate_password_hash('secretary123'), 'Farm Secretary', 'Secretary')
            )
        db.commit()
        db.close()


def get_cycle(module):
    return query('SELECT * FROM cycles WHERE module_name=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (module,), one=True)


def cycle_history(module):
    return query('SELECT * FROM cycles WHERE module_name=? ORDER BY id DESC', (module,))


def participant_shares(cycle_id, profit):
    parts = query(
        '''SELECT p.* FROM cycle_participants cp
           JOIN participants p ON p.id=cp.participant_id
           WHERE cp.cycle_id=? AND p.active=1
           ORDER BY p.name COLLATE NOCASE, p.id DESC''',
        (cycle_id,)
    )
    count = len(parts)
    distributable_profit = max(float(profit or 0), 0.0)
    share = distributable_profit / count if count else 0
    return parts, share


def participant_rows_for_profit(module_name, cycle=None):
    module_name = (module_name or '').upper()
    if cycle:
        linked, _ = participant_shares(cycle['id'], 0)
        if linked:
            return [dict(p) for p in linked]
    return [dict(p) for p in module_participants(module_name, active_only=True)]


def owner_income_share(module_name, finance, cycle=None):
    profit = max(float((finance or {}).get('profit') or 0), 0.0)
    return round(profit * 0.5, 2)


def owner_withdrawal_summary(owner_share_total):
    withdrawable_base = round(float(owner_share_total or 0) * 0.5, 2)

    try:
        withdrawn_row = query('SELECT COALESCE(SUM(amount),0) v FROM owner_withdrawals', one=True)
        withdrawn_total = float((withdrawn_row or {}).get('v', 0) if isinstance(withdrawn_row, dict) else (withdrawn_row['v'] if withdrawn_row else 0))
        recent_owner_withdrawals = query(
            '''SELECT ow.*, ba.account_name
               FROM owner_withdrawals ow
               LEFT JOIN bank_accounts ba ON ba.id=ow.account_id
               ORDER BY ow.id DESC LIMIT 8'''
        )
    except Exception:
        withdrawn_total = 0.0
        recent_owner_withdrawals = []

    withdrawable_remaining = round(max(withdrawable_base - withdrawn_total, 0), 2)

    return {
        'withdrawable_base': withdrawable_base,
        'withdrawn_total': round(withdrawn_total, 2),
        'withdrawable_remaining': withdrawable_remaining,
        'recent_owner_withdrawals': recent_owner_withdrawals,
    }


def module_participants(module_name, active_only=True):
    module_name = (module_name or '').upper()
    sql = 'SELECT * FROM participants WHERE module_name=?'
    params = [module_name]
    if active_only:
        sql += ' AND active=1'
    sql += ' ORDER BY name COLLATE NOCASE, id DESC'
    return query(sql, tuple(params))


def participant_display_rows(module_name, cycle, finance):
    profit = max(float(finance.get('profit') or 0), 0.0)
    participant_rows = participant_rows_for_profit(module_name, cycle)
    count = len(participant_rows)
    share = (profit / count) if count else 0
    display_rows = []
    for p in participant_rows:
        item = dict(p)
        item['income_share'] = round(share, 2)
        display_rows.append(item)
    return display_rows, share


def module_cycle_filter(module, cycle):
    if not cycle:
        return '', ()
    if module == 'POULTRY':
        return ' WHERE cycle_id=? ', (cycle['id'],)
    if module == 'HOG':
        return ' WHERE cycle_id=? ', (cycle['id'],)
    return ' WHERE cycle_id=? ', (cycle['id'],)


def finance_summary_for_module(module, active_only=False):
    if module == 'POULTRY':
        if active_only and get_cycle('POULTRY'):
            cycle = get_cycle('POULTRY')
            revenue = query('SELECT COALESCE(SUM(ps.total_amount),0) v FROM poultry_sales ps LEFT JOIN poultry_batches pb ON pb.id=ps.batch_id WHERE pb.cycle_id=? OR ps.batch_id IS NULL', (cycle['id'],), one=True)['v']
            expenses = query('SELECT COALESCE(SUM(pb.cost),0) v FROM poultry_batches pb WHERE pb.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(pf.amount),0) v FROM poultry_feed_logs pf LEFT JOIN poultry_batches pb ON pb.id=pf.batch_id WHERE pb.cycle_id=? OR pf.batch_id IS NULL', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(pe.amount),0) v FROM poultry_expenses pe LEFT JOIN poultry_batches pb ON pb.id=pe.batch_id WHERE pb.cycle_id=? OR pe.batch_id IS NULL', (cycle['id'],), one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY" AND (cycle_id=? OR cycle_id IS NULL)', (cycle['id'],), one=True)['v']
        else:
            revenue = query('SELECT COALESCE(SUM(total_amount),0) v FROM poultry_sales', one=True)['v']
            expenses = query('SELECT COALESCE(SUM(cost),0) v FROM poultry_batches', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM poultry_feed_logs', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM poultry_expenses', one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY"', one=True)['v']
    elif module == 'HOG':
        if active_only and get_cycle('HOG'):
            cycle = get_cycle('HOG')
            revenue = query('SELECT COALESCE(SUM(hs.total_amount),0) v FROM hog_sales hs LEFT JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id WHERE hc.cycle_id=? OR hs.hog_cycle_id IS NULL', (cycle['id'],), one=True)['v']
            expenses = query('SELECT COALESCE(SUM(hc.cost),0) v FROM hog_cycles hc WHERE hc.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(hf.amount),0) v FROM hog_feed_logs hf LEFT JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id WHERE hc.cycle_id=? OR hf.hog_cycle_id IS NULL', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(he.amount),0) v FROM hog_expenses he LEFT JOIN hog_cycles hc ON hc.id=he.hog_cycle_id WHERE hc.cycle_id=? OR he.hog_cycle_id IS NULL', (cycle['id'],), one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="HOG" AND (cycle_id=? OR cycle_id IS NULL)', (cycle['id'],), one=True)['v']
        else:
            revenue = query('SELECT COALESCE(SUM(total_amount),0) v FROM hog_sales', one=True)['v']
            expenses = query('SELECT COALESCE(SUM(cost),0) v FROM hog_cycles', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM hog_feed_logs', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM hog_expenses', one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="HOG"', one=True)['v']
    else:
        if active_only and get_cycle('FISH'):
            cycle = get_cycle('FISH')
            revenue = query('SELECT COALESCE(SUM(CASE WHEN ft.transaction_type="SELL" THEN ft.total_amount ELSE 0 END),0) v FROM fish_transactions ft LEFT JOIN fish_cycles fc ON fc.id=ft.fish_cycle_id WHERE fc.cycle_id=? OR ft.fish_cycle_id IS NULL', (cycle['id'],), one=True)['v']
            expenses = query('SELECT COALESCE(SUM(CASE WHEN ft.transaction_type="BUY" THEN ft.total_amount ELSE 0 END),0) v FROM fish_transactions ft LEFT JOIN fish_cycles fc ON fc.id=ft.fish_cycle_id WHERE fc.cycle_id=? OR ft.fish_cycle_id IS NULL', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(fe.amount),0) v FROM fish_expenses fe LEFT JOIN fish_cycles fc ON fc.id=fe.fish_cycle_id WHERE fc.cycle_id=? OR fe.fish_cycle_id IS NULL', (cycle['id'],), one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="FISH" AND (cycle_id=? OR cycle_id IS NULL)', (cycle['id'],), one=True)['v']
        else:
            revenue = query('SELECT COALESCE(SUM(CASE WHEN transaction_type="SELL" THEN total_amount ELSE 0 END),0) v FROM fish_transactions', one=True)['v']
            expenses = query('SELECT COALESCE(SUM(CASE WHEN transaction_type="BUY" THEN total_amount ELSE 0 END),0) v FROM fish_transactions', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM fish_expenses', one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="FISH"', one=True)['v']
    return {'revenue': revenue, 'expenses': expenses, 'profit': revenue - expenses, 'capital': capital, 'remaining': capital + revenue - expenses}


def module_cashflow(module, active_only=True):
    rows = []
    if module == 'POULTRY':
        cycle = get_cycle('POULTRY') if active_only else None
        cap_sql = 'SELECT ce.id, ce.entry_date, "Capital" kind, ce.source_name particulars, ce.amount cash_in, 0 cash_out, ce.notes FROM capital_entries ce WHERE ce.module_name="POULTRY"'
        params = []
        if cycle:
            cap_sql += ' AND ce.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(cap_sql, tuple(params))]
        sale_sql = 'SELECT ps.id, ps.entry_date, "Sale" kind, COALESCE(ps.buyer,"Poultry sale") particulars, ps.total_amount cash_in, 0 cash_out, ps.notes FROM poultry_sales ps LEFT JOIN poultry_batches pb ON pb.id=ps.batch_id'
        params=[]
        if cycle:
            sale_sql += ' WHERE pb.cycle_id=? OR ps.batch_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(sale_sql, tuple(params))]
        batch_sql = 'SELECT pb.id, pb.start_date entry_date, "Batch Cost" kind, pb.batch_name particulars, 0 cash_in, pb.cost cash_out, pb.notes notes FROM poultry_batches pb'
        params=[]
        if cycle:
            batch_sql += ' WHERE pb.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(batch_sql, tuple(params))]
        feed_sql = 'SELECT pf.id, pf.entry_date, "Feed" kind, COALESCE(pf.feed_type,"Feed log") particulars, 0 cash_in, pf.amount cash_out, pf.notes FROM poultry_feed_logs pf LEFT JOIN poultry_batches pb ON pb.id=pf.batch_id'
        params=[]
        if cycle:
            feed_sql += ' WHERE pb.cycle_id=? OR pf.batch_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(feed_sql, tuple(params))]
        exp_sql = 'SELECT pe.id, pe.entry_date, pe.category kind, COALESCE(pe.description, pe.category) particulars, 0 cash_in, pe.amount cash_out, pe.description notes FROM poultry_expenses pe LEFT JOIN poultry_batches pb ON pb.id=pe.batch_id'
        params=[]
        if cycle:
            exp_sql += ' WHERE pb.cycle_id=? OR pe.batch_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(exp_sql, tuple(params))]
    elif module == 'HOG':
        cycle = get_cycle('HOG') if active_only else None
        cap_sql = 'SELECT ce.id, ce.entry_date, "Capital" kind, ce.source_name particulars, ce.amount cash_in, 0 cash_out, ce.notes FROM capital_entries ce WHERE ce.module_name="HOG"'
        params=[]
        if cycle:
            cap_sql += ' AND ce.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(cap_sql, tuple(params))]
        sales_sql = 'SELECT hs.id, hs.entry_date, "Sale" kind, COALESCE(hs.buyer,"Hog sale") particulars, hs.total_amount cash_in, 0 cash_out, hs.notes FROM hog_sales hs LEFT JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id'
        params=[]
        if cycle:
            sales_sql += ' WHERE hc.cycle_id=? OR hs.hog_cycle_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(sales_sql, tuple(params))]
        base_sql = 'SELECT hc.id, hc.start_date entry_date, "Cycle Cost" kind, hc.pen_name particulars, 0 cash_in, hc.cost cash_out, hc.notes FROM hog_cycles hc'
        params=[]
        if cycle:
            base_sql += ' WHERE hc.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(base_sql, tuple(params))]
        feed_sql = 'SELECT hf.id, hf.entry_date, "Feed" kind, COALESCE(hf.feed_type,"Feed log") particulars, 0 cash_in, hf.amount cash_out, hf.notes FROM hog_feed_logs hf LEFT JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id'
        params=[]
        if cycle:
            feed_sql += ' WHERE hc.cycle_id=? OR hf.hog_cycle_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(feed_sql, tuple(params))]
        exp_sql = 'SELECT he.id, he.entry_date, he.category kind, COALESCE(he.description, he.category) particulars, 0 cash_in, he.amount cash_out, he.description notes FROM hog_expenses he LEFT JOIN hog_cycles hc ON hc.id=he.hog_cycle_id'
        params=[]
        if cycle:
            exp_sql += ' WHERE hc.cycle_id=? OR he.hog_cycle_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(exp_sql, tuple(params))]
    else:
        cycle = get_cycle('FISH') if active_only else None
        cap_sql = 'SELECT ce.id, ce.entry_date, "Capital" kind, ce.source_name particulars, ce.amount cash_in, 0 cash_out, ce.notes FROM capital_entries ce WHERE ce.module_name="FISH"'
        params=[]
        if cycle:
            cap_sql += ' AND ce.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(cap_sql, tuple(params))]
        tx_sql = 'SELECT ft.id, ft.entry_date, CASE WHEN ft.transaction_type="SELL" THEN "Sale" ELSE "Buy" END kind, COALESCE(ft.buyer, ft.supplier, ft.species) particulars, CASE WHEN ft.transaction_type="SELL" THEN ft.total_amount ELSE 0 END cash_in, CASE WHEN ft.transaction_type="BUY" THEN ft.total_amount ELSE 0 END cash_out, ft.notes FROM fish_transactions ft'
        params=[]
        if cycle:
            tx_sql = 'SELECT ft.id, ft.entry_date, CASE WHEN ft.transaction_type="SELL" THEN "Sale" ELSE "Buy" END kind, COALESCE(ft.buyer, ft.supplier, ft.species) particulars, CASE WHEN ft.transaction_type="SELL" THEN ft.total_amount ELSE 0 END cash_in, CASE WHEN ft.transaction_type="BUY" THEN ft.total_amount ELSE 0 END cash_out, ft.notes FROM fish_transactions ft LEFT JOIN fish_cycles fc ON fc.id=ft.fish_cycle_id'
        params=[]
        if cycle:
            tx_sql += ' WHERE fc.cycle_id=? OR ft.fish_cycle_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(tx_sql, tuple(params))]
        exp_sql = 'SELECT fe.id, fe.entry_date, fe.category kind, COALESCE(fe.description, fe.category) particulars, 0 cash_in, fe.amount cash_out, fe.description notes FROM fish_expenses fe'
        params=[]
        if cycle:
            exp_sql = 'SELECT fe.id, fe.entry_date, fe.category kind, COALESCE(fe.description, fe.category) particulars, 0 cash_in, fe.amount cash_out, fe.description notes FROM fish_expenses fe LEFT JOIN fish_cycles fc ON fc.id=fe.fish_cycle_id'
        params=[]
        if cycle:
            exp_sql += ' WHERE fc.cycle_id=? OR fe.fish_cycle_id IS NULL'; params.append(cycle['id'])
        rows += [dict(r) for r in query(exp_sql, tuple(params))]
    rows.sort(key=lambda x: (x['entry_date'] or '', x['id']), reverse=True)
    running = 0
    ordered = []
    for row in reversed(rows):
        running += float(row['cash_in'] or 0) - float(row['cash_out'] or 0)
        row['running_balance'] = running
        ordered.append(row)
    ordered.reverse()
    summary = {
        'cash_in': round(sum(float(r['cash_in'] or 0) for r in rows), 2),
        'cash_out': round(sum(float(r['cash_out'] or 0) for r in rows), 2),
        'remaining': round(sum(float(r['cash_in'] or 0) for r in rows) - sum(float(r['cash_out'] or 0) for r in rows), 2),
    }
    return rows, summary




def poultry_house_monitoring():
    cycle = get_cycle('POULTRY')
    participant_count = 0
    if cycle:
        participant_count = len(query("SELECT p.* FROM cycle_participants cp JOIN participants p ON p.id=cp.participant_id WHERE cp.cycle_id=?", (cycle['id'],)))
    batches = query('SELECT * FROM poultry_batches WHERE status="ACTIVE" ORDER BY id DESC')
    if cycle:
        batches = [b for b in batches if b['cycle_id'] == cycle['id']]
    houses = {}
    total_birds = sum(int(b['birds_count'] or 0) for b in batches) or 0
    if cycle:
        cycle_capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY" AND (cycle_id=? OR cycle_id IS NULL)', (cycle['id'],), one=True)['v']
    else:
        cycle_capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY"', one=True)['v']
    for b in batches:
        house = (b['house_name'] or 'Unassigned House').strip()
        row = houses.setdefault(house, {
            'house_name': house, 'batch_names': [], 'birds_placed': 0, 'mortality': 0, 'sold_birds': 0,
            'live_birds': 0, 'feed_sacks': 0.0, 'revenue': 0.0, 'expenses': 0.0, 'capital': 0.0,
            'profit': 0.0, 'income_share': 0.0
        })
        row['batch_names'].append(b['batch_name'])
        birds = int(b['birds_count'] or 0)
        row['birds_placed'] += birds
        deaths = query('SELECT COALESCE(SUM(deaths),0) v FROM poultry_mortality WHERE batch_id=?', (b['id'],), one=True)['v']
        sold_birds = query('SELECT COALESCE(SUM(birds_sold),0) v FROM poultry_sales WHERE batch_id=?', (b['id'],), one=True)['v']
        feed_sacks = query('SELECT COALESCE(SUM(bags),0) v FROM poultry_feed_logs WHERE batch_id=?', (b['id'],), one=True)['v']
        sales = query('SELECT COALESCE(SUM(total_amount),0) v FROM poultry_sales WHERE batch_id=?', (b['id'],), one=True)['v']
        feed_cost = query('SELECT COALESCE(SUM(amount),0) v FROM poultry_feed_logs WHERE batch_id=?', (b['id'],), one=True)['v']
        other_exp = query('SELECT COALESCE(SUM(amount),0) v FROM poultry_expenses WHERE batch_id=?', (b['id'],), one=True)['v']
        batch_cost = float(b['cost'] or 0)
        row['mortality'] += int(deaths or 0)
        row['sold_birds'] += int(sold_birds or 0)
        row['feed_sacks'] += float(feed_sacks or 0)
        row['revenue'] += float(sales or 0)
        row['expenses'] += batch_cost + float(feed_cost or 0) + float(other_exp or 0)
    for row in houses.values():
        row['live_birds'] = max(row['birds_placed'] - row['mortality'] - row['sold_birds'], 0)
        row['capital'] = round((cycle_capital * row['birds_placed'] / total_birds), 2) if total_birds else 0
        row['profit'] = round(row['revenue'] - row['expenses'], 2)
        distributable_profit = max(float(row['profit'] or 0), 0.0)
        row['income_share'] = round((distributable_profit / participant_count), 2) if participant_count else 0
        row['mortality_rate'] = safe_pct(row['mortality'], row['birds_placed'])
        row['batches_label'] = ', '.join(row['batch_names'])
    return sorted(houses.values(), key=lambda r: r['house_name'])


def dashboard_context():
    poultry_cycle = get_cycle('POULTRY')
    hog_cycle = get_cycle('HOG')
    fish_cycle = get_cycle('FISH')
    poultry_active = finance_summary_for_module('POULTRY', active_only=True)
    hog_active = finance_summary_for_module('HOG', active_only=True)
    fish_active = finance_summary_for_module('FISH', active_only=True)
    owner_share_chart = {
        'poultry': owner_income_share('POULTRY', poultry_active, poultry_cycle),
        'hog': owner_income_share('HOG', hog_active, hog_cycle),
        'fish': owner_income_share('FISH', fish_active, fish_cycle),
    }
    poultry_live = query('SELECT COALESCE(SUM(birds_count),0) v FROM poultry_batches WHERE status="ACTIVE"', one=True)['v'] - query('SELECT COALESCE(SUM(deaths),0) v FROM poultry_mortality', one=True)['v'] - query('SELECT COALESCE(SUM(birds_sold),0) v FROM poultry_sales', one=True)['v']
    hog_rows = query('SELECT pen_name, heads FROM hog_cycles WHERE status="ACTIVE"')
    hog_sold = float(query('SELECT COALESCE(SUM(heads),0) v FROM hog_sales', one=True)['v'] or 0)
    hog_breakdown = {'piglets': 0, 'sows': 0, 'fattener': 0, 'other': 0}
    for row in hog_rows:
        pen = (row['pen_name'] or '').lower()
        heads = float(row['heads'] or 0)
        if 'piglet' in pen:
            hog_breakdown['piglets'] += heads
        elif 'sow' in pen or 'breeder' in pen:
            hog_breakdown['sows'] += heads
        elif 'fattener' in pen or 'finisher' in pen or 'grower' in pen:
            hog_breakdown['fattener'] += heads
        else:
            hog_breakdown['other'] += heads
    hog_heads = max(0, sum(hog_breakdown.values()) - hog_sold)
    fish_kilos = query('SELECT COALESCE(SUM(CASE WHEN transaction_type="BUY" THEN kilos ELSE -kilos END),0) v FROM fish_transactions', one=True)['v']
    fish_buy = float(query('SELECT COALESCE(SUM(kilos),0) v FROM fish_transactions WHERE transaction_type="BUY"', one=True)['v'] or 0)
    fish_export = float(query('SELECT COALESCE(SUM(kilos),0) v FROM fish_transactions WHERE transaction_type="EXPORT"', one=True)['v'] or 0)
    fish_sales = float(query('SELECT COALESCE(SUM(kilos),0) v FROM fish_transactions WHERE transaction_type="SELL"', one=True)['v'] or 0)
    modules = {'POULTRY': poultry_active, 'HOG': hog_active, 'FISH': fish_active}
    total_remaining = max(sum(max(v['remaining'], 0) for v in modules.values()), 1)
    module_mix = {k.lower(): safe_pct(max(v['remaining'], 0), total_remaining) for k, v in modules.items()}
    expense_chart = {k.lower(): float(v['expenses'] or 0) for k, v in modules.items()}
    capital_chart = {k.lower(): float(v['capital'] or 0) for k, v in modules.items()}
    cashflow_chart = {k.lower(): float(v['remaining'] or 0) for k, v in modules.items()}
    owner_share_total = round(sum(float(v or 0) for v in owner_share_chart.values()), 2)
    withdrawal = owner_withdrawal_summary(owner_share_total)
    return {
        'poultry': poultry_active,
        'hog': hog_active,
        'fish': fish_active,
        'module_mix': module_mix,
        'owner_share_chart': owner_share_chart,
        'expense_chart': expense_chart,
        'capital_chart': capital_chart,
        'cashflow_chart': cashflow_chart,
        'owner_share_total': owner_share_total,
        'withdrawable_base': withdrawal['withdrawable_base'],
        'withdrawn_total': withdrawal['withdrawn_total'],
        'withdrawable_remaining': withdrawal['withdrawable_remaining'],
        'recent_owner_withdrawals': withdrawal['recent_owner_withdrawals'],
        'poultry_live': max(0, poultry_live),
        'hog_heads': max(0, hog_heads),
        'hog_breakdown': {k:int(v) for k,v in hog_breakdown.items()},
        'fish_kilos': max(0, float(fish_kilos or 0)),
        'fish_breakdown': {'buy': fish_buy, 'export': fish_export, 'sales': fish_sales},
        'bank_total': sum(safe_float(row_value(a, 'current_balance', 0)) for a in list_bank_accounts()),
        'bank_accounts': list_bank_accounts(),
        'recent_bank': query('SELECT bt.*, ba.account_name FROM bank_transactions bt LEFT JOIN bank_accounts ba ON ba.id=bt.account_id ORDER BY bt.id DESC LIMIT 8'),
        'house_board': poultry_house_monitoring(),
    }


def safe_pct(part, whole):
    whole = float(whole or 0)
    part = float(part or 0)
    if whole <= 0:
        return 0
    return round(max(0, min(100, (part / whole) * 100)), 1)


def module_visuals(finance, cashflow_summary=None):
    revenue = float(finance.get('revenue') or 0)
    expenses = float(finance.get('expenses') or 0)
    capital = float(finance.get('capital') or 0)
    remaining = float(finance.get('remaining') or 0)
    inflow = capital + revenue
    outflow = expenses
    total_mix = inflow + outflow
    return {
        'inflow_pct': safe_pct(inflow, total_mix),
        'outflow_pct': safe_pct(outflow, total_mix),
        'profit_margin_pct': safe_pct(max(revenue-expenses, 0), revenue),
        'expense_ratio_pct': safe_pct(expenses, inflow if inflow else expenses),
        'remaining_pct': safe_pct(max(remaining, 0), inflow if inflow else max(remaining,1)),
        'cash_in_pct': safe_pct((cashflow_summary or {}).get('cash_in',0), ((cashflow_summary or {}).get('cash_in',0)+(cashflow_summary or {}).get('cash_out',0)),)
    }


def unified_finance_history(module, active_only=True):
    rows = []
    if module == 'POULTRY':
        cycle = get_cycle('POULTRY') if active_only else None
        batches = query('SELECT * FROM poultry_batches ORDER BY id DESC')
        sales = query('SELECT ps.*, pb.batch_name FROM poultry_sales ps LEFT JOIN poultry_batches pb ON pb.id=ps.batch_id ORDER BY ps.id DESC')
        feed_logs = query('SELECT pf.*, pb.batch_name FROM poultry_feed_logs pf LEFT JOIN poultry_batches pb ON pb.id=pf.batch_id ORDER BY pf.id DESC')
        expenses = query('SELECT pe.*, pb.batch_name FROM poultry_expenses pe LEFT JOIN poultry_batches pb ON pb.id=pe.batch_id ORDER BY pe.id DESC')
        capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="POULTRY" ORDER BY ce.id DESC')
        if cycle:
            batches = [r for r in batches if r['cycle_id'] == cycle['id']]
            sales = [r for r in sales if (r['batch_id'] is None) or ((query('SELECT cycle_id FROM poultry_batches WHERE id=?', (r['batch_id'],), one=True) or {'cycle_id':None})['cycle_id'] == cycle['id'])]
            feed_logs = [r for r in feed_logs if (r['batch_id'] is None) or ((query('SELECT cycle_id FROM poultry_batches WHERE id=?', (r['batch_id'],), one=True) or {'cycle_id':None})['cycle_id'] == cycle['id'])]
            expenses = [r for r in expenses if (r['batch_id'] is None) or ((query('SELECT cycle_id FROM poultry_batches WHERE id=?', (r['batch_id'],), one=True) or {'cycle_id':None})['cycle_id'] == cycle['id'])]
            capital = [r for r in capital if r['cycle_id'] in (None, cycle['id'])]
        for r in capital:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Capital', 'details': r['source_name'] or 'Capital entry', 'amount': float(r['amount'] or 0), 'record_type': 'capital', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in sales:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Sale', 'details': (r['buyer'] or 'Poultry sale') + (f" - {r['batch_name']}" if r['batch_name'] else ''), 'amount': float(r['total_amount'] or 0), 'record_type': 'poultry_sale', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in feed_logs:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Feed Expense', 'details': (r['feed_type'] or 'Feed log') + (f" - {r['batch_name']}" if r['batch_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'poultry_feed', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
        for r in expenses:
            rows.append({'entry_date': r['entry_date'], 'type_label': r['category'] or 'Expense', 'details': (r['description'] or r['category'] or 'Expense') + (f" - {r['batch_name']}" if r['batch_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'poultry_expense', 'record_id': r['id'], 'nature':'out', 'receipt_file': r['receipt_file']})
        for r in batches:
            if float(r['cost'] or 0) > 0:
                rows.append({'entry_date': r['start_date'], 'type_label': 'Batch Cost', 'details': r['batch_name'], 'amount': float(r['cost'] or 0), 'record_type': 'poultry_batch', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
    elif module == 'HOG':
        cycle = get_cycle('HOG') if active_only else None
        cycles = query('SELECT * FROM hog_cycles ORDER BY id DESC')
        sales = query('SELECT hs.*, hc.pen_name, hc.cycle_id parent_cycle_id FROM hog_sales hs LEFT JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id ORDER BY hs.id DESC')
        feed_logs = query('SELECT hf.*, hc.pen_name, hc.cycle_id parent_cycle_id FROM hog_feed_logs hf LEFT JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id ORDER BY hf.id DESC')
        expenses = query('SELECT he.*, hc.pen_name, hc.cycle_id parent_cycle_id FROM hog_expenses he LEFT JOIN hog_cycles hc ON hc.id=he.hog_cycle_id ORDER BY he.id DESC')
        capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="HOG" ORDER BY ce.id DESC')
        if cycle:
            cycles = [r for r in cycles if r['cycle_id'] == cycle['id']]
            sales = [r for r in sales if r['parent_cycle_id'] in (None, cycle['id'])]
            feed_logs = [r for r in feed_logs if r['parent_cycle_id'] in (None, cycle['id'])]
            expenses = [r for r in expenses if r['parent_cycle_id'] in (None, cycle['id'])]
            capital = [r for r in capital if r['cycle_id'] in (None, cycle['id'])]
        for r in capital:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Capital', 'details': r['source_name'] or 'Capital entry', 'amount': float(r['amount'] or 0), 'record_type': 'capital', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in sales:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Sale', 'details': (r['buyer'] or 'Hog sale') + (f" - {r['pen_name']}" if r['pen_name'] else ''), 'amount': float(r['total_amount'] or 0), 'record_type': 'hog_sale', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in feed_logs:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Feed Expense', 'details': (r['feed_type'] or 'Feed log') + (f" - {r['pen_name']}" if r['pen_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'hog_feed', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
        for r in expenses:
            rows.append({'entry_date': r['entry_date'], 'type_label': r['category'] or 'Expense', 'details': (r['description'] or r['category'] or 'Expense') + (f" - {r['pen_name']}" if r['pen_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'hog_expense', 'record_id': r['id'], 'nature':'out', 'receipt_file': r['receipt_file']})
        for r in cycles:
            if float(r['cost'] or 0) > 0:
                rows.append({'entry_date': r['start_date'], 'type_label': 'Cycle Cost', 'details': r['pen_name'] or 'Hog cycle', 'amount': float(r['cost'] or 0), 'record_type': 'hog_cycle', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
    else:
        cycle = get_cycle('FISH') if active_only else None
        tx = query('SELECT ft.*, fc.cycle_id parent_cycle_id FROM fish_transactions ft LEFT JOIN fish_cycles fc ON fc.id=ft.fish_cycle_id ORDER BY ft.id DESC')
        expenses = query('SELECT fe.*, fc.cycle_id parent_cycle_id FROM fish_expenses fe LEFT JOIN fish_cycles fc ON fc.id=fe.fish_cycle_id ORDER BY fe.id DESC')
        capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="FISH" ORDER BY ce.id DESC')
        if cycle:
            tx = [r for r in tx if r['parent_cycle_id'] in (None, cycle['id'])]
            expenses = [r for r in expenses if r['parent_cycle_id'] in (None, cycle['id'])]
            capital = [r for r in capital if r['cycle_id'] in (None, cycle['id'])]
        for r in capital:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Capital', 'details': r['source_name'] or 'Capital entry', 'amount': float(r['amount'] or 0), 'record_type': 'capital', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in tx:
            nature = 'in' if r['transaction_type'] == 'SELL' else 'out'
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Sale' if nature == 'in' else 'Fish Purchase', 'details': (r['buyer'] or r['supplier'] or r['species'] or 'Fish transaction') + (f" - {r['species']}" if r['species'] else ''), 'amount': float(r['total_amount'] or 0), 'record_type': 'fish_tx', 'record_id': r['id'], 'nature':nature, 'receipt_file': None})
        for r in expenses:
            rows.append({'entry_date': r['entry_date'], 'type_label': r['category'] or 'Expense', 'details': r['description'] or r['category'] or 'Expense', 'amount': float(r['amount'] or 0), 'record_type': 'fish_expense', 'record_id': r['id'], 'nature':'out', 'receipt_file': r['receipt_file']})
    rows.sort(key=lambda x: ((x['entry_date'] or ''), x['record_id']), reverse=True)
    return rows



def ensure_column(table, column, definition):
    db = sqlite3.connect(DB_PATH)
    cols = [r[1] for r in db.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        db.commit()
    db.close()


def migrate_db():
    ensure_column('participants', 'module_name', 'TEXT NOT NULL DEFAULT "POULTRY"')
    db = sqlite3.connect(DB_PATH)

    db.execute("UPDATE participants SET module_name='POULTRY' WHERE module_name IS NULL OR TRIM(module_name)=''")

    db.execute('''
        CREATE TABLE IF NOT EXISTS owner_withdrawals(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_date TEXT NOT NULL,
            account_id INTEGER,
            amount REAL NOT NULL DEFAULT 0,
            reference_no TEXT,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    db.commit()
    db.close()

    ensure_column('poultry_expenses', 'group_ref', 'TEXT')
    ensure_column('hog_expenses', 'group_ref', 'TEXT')
    ensure_column('fish_expenses', 'group_ref', 'TEXT')
    ensure_column('poultry_expenses', 'receipt_file', 'TEXT')
    ensure_column('hog_expenses', 'receipt_file', 'TEXT')
    ensure_column('fish_expenses', 'receipt_file', 'TEXT')
    ensure_column('hog_expenses', 'receipt_name', 'TEXT')
    ensure_column('fish_expenses', 'receipt_name', 'TEXT')
    ensure_column('hog_cycles', 'hog_type', 'TEXT DEFAULT "Other"')


def log_audit(event_type, record_type, record_id, details):
    execute('INSERT INTO audit_log(event_type,record_type,record_id,details) VALUES(?,?,?,?)', (event_type, record_type, record_id, details))


def parse_bulk_rows(form):
    rows=[]
    for idx in range(1,21):
        category = (form.get(f'item_category_{idx}') or '').strip()
        description = (form.get(f'item_description_{idx}') or '').strip()
        qty = float(form.get(f'item_qty_{idx}') or 0)
        unit_cost = float(form.get(f'item_unit_cost_{idx}') or 0)
        amount = float(form.get(f'item_total_{idx}') or 0)
        if amount == 0 and qty and unit_cost:
            amount = qty * unit_cost
        if category or description or amount:
            rows.append({'category': category or 'Other', 'description': description or category or 'Bulk item', 'qty': qty, 'unit_cost': unit_cost, 'amount': amount})
    return rows


def current_feed_stock(module='POULTRY'):
    stock_in = query('SELECT COALESCE(SUM(CASE WHEN usage_type="Add to Inventory" THEN sacks ELSE 0 END),0) v FROM feed_inventory WHERE module_name=?', (module,), one=True)['v']
    stock_out = query('SELECT COALESCE(SUM(bags),0) v FROM poultry_feed_logs', one=True)['v'] if module == 'POULTRY' else 0
    return max(0, float(stock_in or 0) - float(stock_out or 0))


def default_logo_url():
    env_logo = os.getenv('LOGO_URL', '').strip()
    if env_logo:
        return env_logo
    local_logo = BASE_DIR / 'static' / 'logo.png'
    if local_logo.exists():
        return url_for('static', filename='logo.png')
    return ''


def grouped_expenses(rows):
    groups = {}
    for r in rows:
        key = (r['group_ref'] or f"single-{r['id']}")
        groups.setdefault(key, {'entry_date':r['entry_date'], 'group_ref':r['group_ref'], 'items':[], 'total':0.0, 'receipt_file': r['receipt_file'] if 'receipt_file' in r.keys() else None})
        groups[key]['items'].append(r)
        groups[key]['total'] += float(r['amount'] or 0)
    return list(groups.values())

@app.context_processor
def inject_globals():
    def fmt_money(value):
        try:
            return f"P{float(value or 0):,.2f}"
        except Exception:
            return "P0.00"
    return {
        'app_name': APP_NAME,
        'dashboard_subtitle': DASHBOARD_SUBTITLE,
        'footer_email': FOOTER_EMAIL,
        'logo_url': default_logo_url(),
        'current_user': session.get('user'),
        'modules': MODULES,
        'is_admin_user': is_admin(),
        'can_access_module': can_access_module,
        'fmt_money': fmt_money,
        'receipt_preview_url': receipt_preview_url,
        'receipt_download_url': receipt_download_url,
        'bank_option_label': bank_option_label,
    }


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        user = query('SELECT * FROM users WHERE username=?', (username,), one=True)
        seed_passwords = {
            'admin': {'admin', 'admin123'},
            'secretary': {'secretary', 'secretary123'},
        }
        valid = False
        if user and check_password_hash(user['password'], password):
            valid = True
        elif user and username.lower() in seed_passwords and password in seed_passwords[username.lower()]:
            execute('UPDATE users SET password=? WHERE id=?', (generate_password_hash(password), user['id']))
            user = query('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
            valid = True
        if valid:
            session['user'] = {'username': user['username'], 'full_name': user['full_name'], 'role': user['role']}
            return redirect(url_for('dashboard' if user['role'] == 'Admin' else 'poultry_page'))
        flash('Invalid login.', 'danger')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))



@app.route('/health')
def health():
    return {'status':'ok'}


@app.route('/receipts/<path:filename>')
@login_required
def view_receipt(filename):
    return send_from_directory(UPLOAD_DIR, filename)


@app.route('/')
@login_required
def dashboard():
    if not is_admin():
        return redirect(url_for('poultry_page'))
    return render_template('dashboard.html', ctx=dashboard_context())


@app.route('/owner-withdraw', methods=['POST'])
@admin_required
def owner_withdraw():
    amount = safe_float(request.form.get('amount'))
    account_id = request.form.get('account_id') or None
    entry_date = safe_date(request.form.get('entry_date'), str(date.today()))
    reference_no = clean_text(request.form.get('reference_no'))
    notes = clean_text(request.form.get('notes'))
    ctx = dashboard_context()

    if amount <= 0:
        flash('Enter a valid withdrawal amount.', 'danger')
        return redirect(url_for('dashboard'))
    if amount > float(ctx.get('withdrawable_remaining') or 0):
        flash('Withdrawal exceeds available withdrawable owner profit.', 'danger')
        return redirect(url_for('dashboard'))
    if not account_id:
        flash('Select a bank account for the withdrawal.', 'danger')
        return redirect(url_for('dashboard'))

    account = query('SELECT * FROM bank_accounts WHERE id=?', (account_id,), one=True)
    if not account:
        flash('Selected bank account was not found.', 'danger')
        return redirect(url_for('dashboard'))
    current_balance = float(account['current_balance'] or 0)
    if amount > current_balance:
        flash('Withdrawal exceeds selected bank balance.', 'danger')
        return redirect(url_for('dashboard'))

    execute(
        'INSERT INTO owner_withdrawals(entry_date,account_id,amount,reference_no,notes) VALUES(?,?,?,?,?)',
        (entry_date, account_id, amount, reference_no, notes)
    )
    sync_bank_balance(account_id, amount, 'OWNER', entry_date, 'WITHDRAWAL', reference_no=reference_no, purpose='Owner profit withdrawal', notes=notes)
    flash('Owner withdrawal saved and deducted from bank balance.', 'success')
    return redirect(url_for('dashboard'))


@app.route('/participants', methods=['GET', 'POST'])
@admin_required
def participants_page():
    if request.method == 'POST':
        execute(
            'INSERT INTO participants(name,role,module_name,notes) VALUES(?,?,?,?)',
            (
                clean_text(request.form.get('name')),
                clean_text(request.form.get('role')),
                clean_text(request.form.get('module_name'), 'POULTRY').upper(),
                clean_text(request.form.get('notes'))
            )
        )
        flash('Participant added.', 'success')
        return redirect(url_for('participants_page'))
    return render_template('participants.html', participants=query('SELECT * FROM participants ORDER BY id DESC'), roles=PARTICIPANT_ROLES, modules=MODULES)


@app.route('/participants/<int:pid>/toggle')
@admin_required
def participant_toggle(pid):
    row = query('SELECT * FROM participants WHERE id=?', (pid,), one=True)
    if row:
        execute('UPDATE participants SET active=? WHERE id=?', (0 if row['active'] else 1, pid))
        flash('Participant updated.', 'success')
    return redirect(url_for('participants_page'))


@app.route('/bank', methods=['GET', 'POST'])
@admin_required
def bank_page():
    if request.method == 'POST' and request.form.get('form_name') == 'account':
        opening = float(request.form.get('opening_balance') or 0)
        account_id = execute('INSERT INTO bank_accounts(account_name,bank_name,account_type,opening_balance,current_balance,notes) VALUES(?,?,?,?,?,?)',
                (request.form['account_name'], request.form['bank_name'], request.form['account_type'], opening, opening, request.form.get('notes')))
        recompute_bank_balance(account_id)
        flash('Account added.', 'success')
        return redirect(url_for('bank_page'))
    accounts = list_bank_accounts()
    tx = query('SELECT bt.*, ba.account_name FROM bank_transactions bt LEFT JOIN bank_accounts ba ON ba.id=bt.account_id ORDER BY bt.id DESC LIMIT 60')
    return render_template('bank.html', accounts=accounts, tx=tx, bank_types=BANK_TYPES, tx_types=TX_TYPES)


@app.route('/bank/tx/add', methods=['POST'])
@admin_required
def bank_tx_add():
    account_id = int(request.form['account_id'])
    amount = float(request.form.get('amount') or 0)
    tx_type = request.form['tx_type']
    execute('INSERT INTO bank_transactions(entry_date,account_id,module_name,tx_type,amount,reference_no,purpose,notes) VALUES(?,?,?,?,?,?,?,?)',
            (request.form['entry_date'], account_id, request.form.get('module_name'), tx_type, amount, request.form.get('reference_no'), request.form.get('purpose'), request.form.get('notes')))
    recompute_bank_balance(account_id)
    log_audit('ADD', 'bank_tx', 0, f'account={account_id} {tx_type} {amount}')
    flash('Bank transaction added.', 'success')
    return redirect(url_for('bank_page'))


@app.route('/capital/add', methods=['POST'])
@login_required
def capital_add():
    if not can_access_module(request.form.get('module_name')):
        flash('You do not have access to add capital to that module.', 'danger')
        return redirect(url_for('poultry_page'))
    capital_id = execute('INSERT INTO capital_entries(cycle_id,module_name,entry_date,source_name,amount,destination_account_id,notes) VALUES(?,?,?,?,?,?,?)',
            (request.form.get('cycle_id') or None, request.form['module_name'], request.form['entry_date'], request.form.get('source_name'), float(request.form.get('amount') or 0), request.form.get('destination_account_id') or None, request.form.get('notes')))
    if request.form.get('destination_account_id'):
        sync_bank_balance(request.form.get('destination_account_id'), float(request.form.get('amount') or 0), request.form['module_name'], request.form['entry_date'], 'DEPOSIT', purpose='Capital entry', notes=request.form.get('notes'))
    log_audit('ADD', 'capital', capital_id or 0, f"{request.form['module_name']} capital {request.form.get('amount')}")
    flash('Capital entry added.', 'success')
    return redirect(request.referrer or url_for('finance_page'))


def start_cycle(module, cycle_name, poultry_type=None, notes=None, participants=None):
    execute('UPDATE cycles SET status="ENDED", end_date=date("now") WHERE module_name=? AND status="ACTIVE"', (module,))
    cycle_id = execute('INSERT INTO cycles(module_name,cycle_name,poultry_type,start_date,status,notes) VALUES(?,?,?,date("now"),"ACTIVE",?)', (module, cycle_name, poultry_type, notes))
    if participants:
        for pid in participants:
            execute('INSERT INTO cycle_participants(cycle_id,participant_id) VALUES(?,?)', (cycle_id, int(pid)))
    return cycle_id


@app.route('/cycle/start', methods=['POST'])
@login_required
def cycle_start():
    if not can_access_module(request.form.get('module_name')):
        flash('You do not have access to start a cycle for that module.', 'danger')
        return redirect(url_for('poultry_page'))
    cycle_id = start_cycle(request.form['module_name'], request.form['cycle_name'], request.form.get('poultry_type'), request.form.get('notes'), request.form.getlist('participants'))
    flash('New cycle started.', 'success')
    return redirect({'POULTRY':'/poultry','HOG':'/hog','FISH':'/fish'}.get(request.form['module_name'], '/'))


@app.route('/cycle/end/<int:cycle_id>')
@login_required
def cycle_end(cycle_id):
    cycle = query('SELECT * FROM cycles WHERE id=?', (cycle_id,), one=True)
    if not cycle or not can_access_module(cycle['module_name']):
        flash('You do not have access to end that cycle.', 'danger')
        return redirect(url_for('poultry_page'))
    execute('UPDATE cycles SET status="ENDED", end_date=date("now") WHERE id=?', (cycle_id,))
    flash('Cycle ended and moved to history.', 'success')
    return redirect(request.referrer or url_for('dashboard'))


@app.route('/poultry', methods=['GET', 'POST'])
@module_access_required('POULTRY')
def poultry_page():
    tab = request.args.get('tab', 'overview')
    cycle = get_cycle('POULTRY')
    if request.method == 'POST':
        form_name = request.form.get('form_name')
        if form_name == 'batch':
            execute('INSERT INTO poultry_batches(cycle_id,poultry_type,batch_name,house_name,start_date,birds_count,supplier,cost,notes) VALUES(?,?,?,?,?,?,?,?,?)',
                    (cycle['id'] if cycle else None, request.form['poultry_type'], request.form['batch_name'], request.form.get('house_name'), request.form['start_date'], int(request.form.get('birds_count') or 0), request.form.get('supplier'), float(request.form.get('cost') or 0), request.form.get('notes')))
            flash('Batch added.', 'success')
            return redirect(url_for('poultry_page', tab='batches'))
        if form_name == 'mortality':
            execute('INSERT INTO poultry_mortality(batch_id,entry_date,deaths,notes) VALUES(?,?,?,?)', (int(request.form['batch_id']), request.form['entry_date'], int(request.form.get('deaths') or 0), request.form.get('notes')))
            flash('Mortality saved.', 'success')
            return redirect(url_for('poultry_page', tab='operations'))
        if form_name == 'feed':
            feed_id = execute('INSERT INTO poultry_feed_logs(batch_id,entry_date,feed_type,bags,amount,notes) VALUES(?,?,?,?,?,?)', (int(request.form['batch_id']), request.form['entry_date'], request.form.get('feed_type'), float(request.form.get('bags') or 0), float(request.form.get('amount') or 0), request.form.get('notes')))
            log_audit('ADD', 'poultry_feed', feed_id, f"{request.form.get('feed_type')} {request.form.get('bags')} sacks")
            flash('Feed log saved.', 'success')
            return redirect(url_for('poultry_page', tab='operations'))
        if form_name == 'feed_stock':
            sacks = float(request.form.get('sacks') or 0)
            cps = float(request.form.get('cost_per_sack') or 0)
            total = sacks * cps
            inv_id = execute('INSERT INTO feed_inventory(module_name,entry_date,feed_type,sacks,cost_per_sack,total_cost,source_name,usage_type,house_name,cycle_id,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?)', ('POULTRY', request.form['entry_date'], request.form.get('feed_type'), sacks, cps, total, request.form.get('source_name'), request.form.get('usage_type') or 'Add to Inventory', request.form.get('house_name'), cycle['id'] if cycle else None, request.form.get('notes')))
            log_audit('ADD', 'feed_inventory', inv_id, f"{request.form.get('feed_type')} {sacks} sacks")
            flash('Feed stock saved.', 'success')
            return redirect(url_for('poultry_page', tab='operations'))
        if form_name == 'sale':
            kilos = float(request.form.get('kilos') or 0)
            ppk = float(request.form.get('price_per_kilo') or 0)
            total = kilos * ppk
            execute('INSERT INTO poultry_sales(batch_id,entry_date,buyer,birds_sold,kilos,price_per_kilo,total_amount,notes,account_id) VALUES(?,?,?,?,?,?,?,?,?)',
                    (int(request.form['batch_id']), request.form['entry_date'], request.form.get('buyer'), int(request.form.get('birds_sold') or 0), kilos, ppk, total, request.form.get('notes'), request.form.get('account_id') or None))
            if request.form.get('account_id'):
                sync_bank_balance(request.form.get('account_id'), total, 'POULTRY', request.form['entry_date'], 'DEPOSIT', purpose='Poultry sale', notes=request.form.get('notes'))
            flash('Poultry sale saved.', 'success')
            return redirect(url_for('poultry_page', tab='finance'))
        if form_name == 'expense':
            receipt_file = save_uploaded_receipt(
                request.files.get('receipt_file'),
                f"poultry_{request.form.get('entry_date')}"
            )
            exp_id = execute(
                'INSERT INTO poultry_expenses(batch_id,entry_date,category,amount,description,receipt_file,account_id) VALUES(?,?,?,?,?,?,?)',
                (
                    None,
                    request.form['entry_date'],
                    request.form.get('category'),
                    float(request.form.get('amount') or 0),
                    f"{request.form.get('item_name')}{' - ' + request.form.get('description') if request.form.get('description') else ''}",
                    receipt_file,
                    request.form.get('account_id') or None
                )
            )
            if request.form.get('account_id'):
                sync_bank_balance(request.form.get('account_id'), float(request.form.get('amount') or 0), 'POULTRY', request.form['entry_date'], 'WITHDRAWAL', purpose=request.form.get('category'), notes=request.form.get('description'))
            log_audit('ADD', 'poultry_expense', exp_id, request.form.get('description'))
            flash('Expense saved.', 'success')
            return redirect(url_for('poultry_page', tab='finance'))
        if form_name == 'bulk_expense':
            rows = parse_bulk_rows(request.form)
            group_ref = f"PB-{request.form.get('entry_date')}-{int(request.form.get('batch_id') or 0)}-{len(rows)}"
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), group_ref.lower())
            for item in rows:
                desc = f"{item['description']} | Qty: {item['qty']} | Unit Cost: {item['unit_cost']}"
                execute('INSERT INTO poultry_expenses(batch_id,entry_date,category,amount,description,group_ref,receipt_file,account_id) VALUES(?,?,?,?,?,?,?,?)', (int(request.form['batch_id']), request.form['entry_date'], item['category'], item['amount'], desc, group_ref, receipt_file, request.form.get('account_id') or None))
            flash(f'Bulk expense saved with {len(rows)} items.', 'success')
            return redirect(url_for('poultry_page', tab='finance'))
    batches = query('SELECT * FROM poultry_batches ORDER BY id DESC')
    mortality = query('SELECT pm.*, pb.batch_name FROM poultry_mortality pm LEFT JOIN poultry_batches pb ON pb.id=pm.batch_id ORDER BY pm.id DESC')
    feed_logs = query('SELECT pf.*, pb.batch_name FROM poultry_feed_logs pf LEFT JOIN poultry_batches pb ON pb.id=pf.batch_id ORDER BY pf.id DESC')
    sales = query('SELECT ps.*, pb.batch_name FROM poultry_sales ps LEFT JOIN poultry_batches pb ON pb.id=ps.batch_id ORDER BY ps.id DESC')
    expenses = query('SELECT pe.*, pb.batch_name FROM poultry_expenses pe LEFT JOIN poultry_batches pb ON pb.id=pe.batch_id ORDER BY pe.id DESC')
    capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="POULTRY" ORDER BY ce.id DESC')
    finance = finance_summary_for_module('POULTRY', active_only=True)
    cashflow_rows, cashflow_summary = module_cashflow('POULTRY', active_only=True)
    finance_history = unified_finance_history('POULTRY', active_only=True)
    feed_inventory = query("SELECT * FROM feed_inventory WHERE module_name='POULTRY' ORDER BY id DESC LIMIT 100")
    bulk_groups = grouped_expenses(query('SELECT pe.*, pb.batch_name FROM poultry_expenses pe LEFT JOIN poultry_batches pb ON pb.id=pe.batch_id ORDER BY pe.id DESC'))
    visuals = module_visuals(finance, cashflow_summary)
    available_participants = module_participants('POULTRY', active_only=True)
    share_participants, share = participant_display_rows('POULTRY', cycle, finance)
    return render_template('poultry.html', tab=tab, cycle=cycle, cycles=cycle_history('POULTRY'), batches=batches, mortality=mortality, feed_logs=feed_logs, sales=sales, expenses=expenses, capital=capital, finance=finance, finance_history=finance_history, participants=available_participants, available_participants=available_participants, share=share, share_participants=share_participants, poultry_types=POULTRY_TYPES, expense_categories=EXPENSE_CATEGORIES, bank_accounts=list_bank_accounts(), cashflow_rows=cashflow_rows, cashflow_summary=cashflow_summary, visuals=visuals, feed_inventory=feed_inventory, feed_stock_remaining=current_feed_stock('POULTRY'), feed_usage_types=FEED_USAGE_TYPES, bulk_groups=bulk_groups)


@app.route('/hog', methods=['GET', 'POST'])
@admin_required
def hog_page():
    tab = request.args.get('tab', 'overview')
    cycle = get_cycle('HOG')
    if request.method == 'POST':
        form_name = request.form.get('form_name')
        if form_name == 'hog_cycle':
            execute('INSERT INTO hog_cycles(cycle_id,pen_name,start_date,heads,source,cost,notes) VALUES(?,?,?,?,?,?,?)',
                    (cycle['id'] if cycle else None, request.form.get('pen_name'), request.form.get('start_date'), int(request.form.get('heads') or 0), request.form.get('source'), float(request.form.get('cost') or 0), request.form.get('notes')))
            flash('Hog record added.', 'success')
            return redirect(url_for('hog_page', tab='operations'))
        if form_name == 'hog_feed':
            hog_cycle_id = safe_int(request.form.get('hog_cycle_id')) or latest_hog_cycle_id(cycle)
            if not hog_cycle_id:
                flash('Create a hog cycle first before saving feed.', 'danger')
                return redirect(url_for('hog_page', tab='operations'))
            execute('INSERT INTO hog_feed_logs(hog_cycle_id,entry_date,feed_type,quantity,amount,notes) VALUES(?,?,?,?,?,?)',
                    (hog_cycle_id, safe_date(request.form.get('entry_date')), clean_text(request.form.get('feed_type'),'Feed'), clean_text(request.form.get('quantity')), safe_float(request.form.get('amount')), clean_text(request.form.get('notes'))))
            flash('Hog feed saved.', 'success')
            return redirect(url_for('hog_page', tab='operations'))
        if form_name == 'hog_sale':
            hog_cycle_id = safe_int(request.form.get('hog_cycle_id')) or latest_hog_cycle_id(cycle)
            if not hog_cycle_id:
                flash('Create a hog cycle first before saving a sale.', 'danger')
                return redirect(url_for('hog_page', tab='finance'))
            execute('INSERT INTO hog_sales(hog_cycle_id,entry_date,buyer,heads,kilos,total_amount,notes,account_id) VALUES(?,?,?,?,?,?,?,?)',
                    (hog_cycle_id, safe_date(request.form.get('entry_date')), clean_text(request.form.get('buyer')), safe_int(request.form.get('heads')), safe_float(request.form.get('kilos')), safe_float(request.form.get('total_amount')), clean_text(request.form.get('notes')), request.form.get('account_id') or None))
            if request.form.get('account_id'):
                sync_bank_balance(request.form.get('account_id'), safe_float(request.form.get('total_amount')), 'HOG', safe_date(request.form.get('entry_date')), 'DEPOSIT', purpose='Hog sale', notes=clean_text(request.form.get('notes')))
            flash('Hog sale saved.', 'success')
            return redirect(url_for('hog_page', tab='finance'))
        if form_name == 'hog_expense':
            hog_cycle_id = safe_int(request.form.get('hog_cycle_id')) or latest_hog_cycle_id(cycle)
            if not hog_cycle_id:
                flash('Create a hog cycle first before saving an expense.', 'danger')
                return redirect(url_for('hog_page', tab='finance'))
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), f"hog_{safe_date(request.form.get('entry_date'))}")
            execute('INSERT INTO hog_expenses(hog_cycle_id,entry_date,category,amount,description,receipt_file,receipt_name,account_id) VALUES(?,?,?,?,?,?,?,?)',
                    (hog_cycle_id, safe_date(request.form.get('entry_date')), clean_text(request.form.get('category'),'Other'), safe_float(request.form.get('amount')), clean_text(request.form.get('description')), receipt_file, secure_filename(getattr(request.files.get('receipt_file'), 'filename', '') or ''), request.form.get('account_id') or None))
            if request.form.get('account_id'):
                sync_bank_balance(request.form.get('account_id'), safe_float(request.form.get('amount')), 'HOG', safe_date(request.form.get('entry_date')), 'WITHDRAWAL', purpose=clean_text(request.form.get('category'), 'Hog expense'), notes=clean_text(request.form.get('description')))
            flash('Hog expense saved.', 'success')
            return redirect(url_for('hog_page', tab='finance'))
        if form_name == 'hog_bulk_expense':
            rows = parse_bulk_rows(request.form)
            hog_cycle_id = safe_int(request.form.get('hog_cycle_id')) or latest_hog_cycle_id(cycle)
            if not hog_cycle_id:
                flash('Create a hog cycle first before saving expenses.', 'danger')
                return redirect(url_for('hog_page', tab='finance'))
            if not rows:
                flash('No expense items to save.', 'danger')
                return redirect(url_for('hog_page', tab='finance'))
            entry_date = safe_date(request.form.get('entry_date'))
            group_ref = f"HB-{entry_date}-{int(hog_cycle_id or 0)}-{len(rows)}"
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), group_ref.lower())
            receipt_name = secure_filename(getattr(request.files.get('receipt_file'), 'filename', '') or '')
            for item in rows:
                execute('INSERT INTO hog_expenses(hog_cycle_id,entry_date,category,amount,description,group_ref,receipt_file,receipt_name,account_id) VALUES(?,?,?,?,?,?,?,?,?)', (hog_cycle_id, entry_date, item['category'], item['amount'], f"{item['description']} | Qty: {item['qty']} | Unit Cost: {item['unit_cost']}", group_ref, receipt_file, receipt_name, request.form.get('account_id') or None))
            flash(f'Hog bulk expense saved with {len(rows)} items.', 'success')
            return redirect(url_for('hog_page', tab='finance'))
    cycles = query('SELECT * FROM hog_cycles ORDER BY id DESC')
    feed_logs = query('SELECT hf.*, hc.pen_name FROM hog_feed_logs hf LEFT JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id ORDER BY hf.id DESC')
    sales = query('SELECT hs.*, hc.pen_name FROM hog_sales hs LEFT JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id ORDER BY hs.id DESC')
    expenses = query('SELECT he.*, hc.pen_name FROM hog_expenses he LEFT JOIN hog_cycles hc ON hc.id=he.hog_cycle_id ORDER BY he.id DESC')
    capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="HOG" ORDER BY ce.id DESC')
    finance = finance_summary_for_module('HOG', active_only=True)
    cashflow_rows, cashflow_summary = module_cashflow('HOG', active_only=True)
    finance_history = unified_finance_history('HOG', active_only=True)
    visuals = module_visuals(finance, cashflow_summary)
    available_participants = module_participants('HOG', active_only=True)
    share_participants, share = participant_display_rows('HOG', cycle, finance)
    return render_template('hog.html', tab=tab, cycle=cycle, cycle_history=cycle_history('HOG'), cycles=cycles, feed_logs=feed_logs, sales=sales, expenses=expenses, capital=capital, finance=finance, finance_history=finance_history, participants=available_participants, available_participants=available_participants, share=share, share_participants=share_participants, expense_categories=EXPENSE_CATEGORIES, bank_accounts=list_bank_accounts(), cashflow_rows=cashflow_rows, cashflow_summary=cashflow_summary, visuals=visuals)


@app.route('/fish', methods=['GET', 'POST'])
@admin_required
def fish_page():
    tab = request.args.get('tab', 'overview')
    cycle = get_cycle('FISH')
    if request.method == 'POST':
        form_name = request.form.get('form_name')
        if form_name == 'fish_tx':
            kilos = safe_float(request.form.get('kilos'))
            ppk = safe_float(request.form.get('price_per_kilo'))
            total = kilos * ppk
            active_fish_cycle = query('SELECT * FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True) if cycle else None
            if cycle and not active_fish_cycle:
                fc_id = execute('INSERT INTO fish_cycles(cycle_id,period_name,start_date,status,notes) VALUES(?,?,?,?,?)', (cycle['id'], clean_text(request.form.get('period_name'), cycle['cycle_name']), safe_date(request.form.get('entry_date')), 'ACTIVE', clean_text(request.form.get('notes'))))
            else:
                fc_id = active_fish_cycle['id'] if active_fish_cycle else latest_fish_cycle_id(cycle)
            if not fc_id and cycle:
                fc_id = execute('INSERT INTO fish_cycles(cycle_id,period_name,start_date,status,notes) VALUES(?,?,?,?,?)', (cycle['id'], clean_text(request.form.get('period_name'), cycle['cycle_name']), safe_date(request.form.get('entry_date')), 'ACTIVE', clean_text(request.form.get('notes'))))
            execute('INSERT INTO fish_transactions(fish_cycle_id,entry_date,transaction_type,supplier,buyer,species,kilos,price_per_kilo,total_amount,notes,account_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (fc_id, safe_date(request.form.get('entry_date')), clean_text(request.form.get('transaction_type'),'BUY'), clean_text(request.form.get('supplier')), clean_text(request.form.get('buyer')), clean_text(request.form.get('species'),'Fish'), kilos, ppk, total, clean_text(request.form.get('notes')), request.form.get('account_id') or None))
            if request.form.get('account_id') and total > 0:
                sync_bank_balance(request.form.get('account_id'), total, 'FISH', safe_date(request.form.get('entry_date')), 'WITHDRAWAL' if clean_text(request.form.get('transaction_type'),'BUY').upper() == 'BUY' else 'DEPOSIT', purpose='Fish transaction', notes=clean_text(request.form.get('notes')))
            flash('Fish transaction saved.', 'success')
            return redirect(url_for('fish_page', tab='operations'))
        if form_name == 'fish_expense':
            active_fish_cycle = query('SELECT * FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True) if cycle else None
            fc_id = active_fish_cycle['id'] if active_fish_cycle else latest_fish_cycle_id(cycle)
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), f"fish_{safe_date(request.form.get('entry_date'))}")
            execute('INSERT INTO fish_expenses(fish_cycle_id,entry_date,category,amount,description,receipt_file,receipt_name,account_id) VALUES(?,?,?,?,?,?,?,?)',
                    (fc_id, safe_date(request.form.get('entry_date')), clean_text(request.form.get('category'),'Other'), safe_float(request.form.get('amount')), clean_text(request.form.get('description')), receipt_file, secure_filename(getattr(request.files.get('receipt_file'), 'filename', '') or ''), request.form.get('account_id') or None))
            if request.form.get('account_id'):
                sync_bank_balance(request.form.get('account_id'), safe_float(request.form.get('amount')), 'FISH', safe_date(request.form.get('entry_date')), 'WITHDRAWAL', purpose=clean_text(request.form.get('category'), 'Fish expense'), notes=clean_text(request.form.get('description')))
            flash('Fish expense saved.', 'success')
            return redirect(url_for('fish_page', tab='finance'))
        if form_name == 'fish_bulk_expense':
            active_fish_cycle = query('SELECT * FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True) if cycle else None
            fc_id = active_fish_cycle['id'] if active_fish_cycle else latest_fish_cycle_id(cycle)
            rows = parse_bulk_rows(request.form)
            if not rows:
                flash('No expense items to save.', 'danger')
                return redirect(url_for('fish_page', tab='finance'))
            entry_date = safe_date(request.form.get('entry_date'))
            group_ref = f"FB-{entry_date}-{int(fc_id or 0)}-{len(rows)}"
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), group_ref.lower())
            receipt_name = secure_filename(getattr(request.files.get('receipt_file'), 'filename', '') or '')
            for item in rows:
                execute('INSERT INTO fish_expenses(fish_cycle_id,entry_date,category,amount,description,group_ref,receipt_file,receipt_name,account_id) VALUES(?,?,?,?,?,?,?,?,?)', (fc_id, entry_date, item['category'], item['amount'], f"{item['description']} | Qty: {item['qty']} | Unit Cost: {item['unit_cost']}", group_ref, receipt_file, receipt_name, request.form.get('account_id') or None))
            flash(f'Fish bulk expense saved with {len(rows)} items.', 'success')
            return redirect(url_for('fish_page', tab='finance'))
    tx = query('SELECT * FROM fish_transactions ORDER BY id DESC')
    expenses = query('SELECT * FROM fish_expenses ORDER BY id DESC')
    capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="FISH" ORDER BY ce.id DESC')
    finance = finance_summary_for_module('FISH', active_only=True)
    cashflow_rows, cashflow_summary = module_cashflow('FISH', active_only=True)
    finance_history = unified_finance_history('FISH', active_only=True)
    visuals = module_visuals(finance, cashflow_summary)
    available_participants = module_participants('FISH', active_only=True)
    share_participants, share = participant_display_rows('FISH', cycle, finance)
    return render_template('fish.html', tab=tab, cycle=cycle, cycle_history=cycle_history('FISH'), tx=tx, expenses=expenses, capital=capital, finance=finance, finance_history=finance_history, participants=available_participants, available_participants=available_participants, share=share, share_participants=share_participants, expense_categories=EXPENSE_CATEGORIES, bank_accounts=list_bank_accounts(), cashflow_rows=cashflow_rows, cashflow_summary=cashflow_summary, visuals=visuals)


def get_edit_config(record_type):
    configs = {
        'poultry_batch': {'table':'poultry_batches','fields':['poultry_type','batch_name','house_name','start_date','birds_count','supplier','cost','status','notes'],'redirect':'poultry_page','tab':'batches'},
        'poultry_mortality': {'table':'poultry_mortality','fields':['entry_date','deaths','notes'],'redirect':'poultry_page','tab':'operations'},
        'poultry_feed': {'table':'poultry_feed_logs','fields':['entry_date','feed_type','bags','amount','notes'],'redirect':'poultry_page','tab':'operations'},
        'poultry_sale': {'table':'poultry_sales','fields':['entry_date','buyer','birds_sold','kilos','price_per_kilo','notes'],'redirect':'poultry_page','tab':'finance'},
        'poultry_expense': {'table':'poultry_expenses','fields':['entry_date','category','amount','description'],'redirect':'poultry_page','tab':'finance'},
        'hog_cycle': {'table':'hog_cycles','fields':['pen_name','start_date','heads','source','cost','status','notes'],'redirect':'hog_page','tab':'operations'},
        'hog_feed': {'table':'hog_feed_logs','fields':['entry_date','feed_type','quantity','amount','notes'],'redirect':'hog_page','tab':'operations'},
        'hog_sale': {'table':'hog_sales','fields':['entry_date','buyer','heads','kilos','total_amount','notes'],'redirect':'hog_page','tab':'finance'},
        'hog_expense': {'table':'hog_expenses','fields':['entry_date','category','amount','description'],'redirect':'hog_page','tab':'finance'},
        'fish_tx': {'table':'fish_transactions','fields':['entry_date','transaction_type','supplier','buyer','species','kilos','price_per_kilo','notes'],'redirect':'fish_page','tab':'operations'},
        'fish_expense': {'table':'fish_expenses','fields':['entry_date','category','amount','description'],'redirect':'fish_page','tab':'finance'},
        'capital': {'table':'capital_entries','fields':['entry_date','source_name','amount','notes'],'redirect':'finance_page','tab':None},
        'bank_tx': {'table':'bank_transactions','fields':['entry_date','account_id','module_name','tx_type','amount','reference_no','purpose','notes'],'redirect':'bank_page','tab':None},
    }
    return configs.get(record_type)


@app.route('/record/edit/<record_type>/<int:record_id>', methods=['GET', 'POST'])
@login_required
def record_edit(record_type, record_id):
    if not is_admin() and not record_type.startswith('poultry_'):
        flash('You do not have access to edit that record.', 'danger')
        return redirect(url_for('poultry_page'))
    cfg = get_edit_config(record_type)
    if not cfg:
        flash('Unknown record type.', 'danger')
        return redirect(url_for('dashboard'))
    row = query(f'SELECT * FROM {cfg["table"]} WHERE id=?', (record_id,), one=True)
    if not row:
        flash('Record not found.', 'danger')
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        previous_account_id = safe_int(row_value(row, 'account_id')) if record_type == 'bank_tx' else None
        fields = []
        values = []
        for f in cfg['fields']:
            val = request.form.get(f)
            if f in ['birds_count','deaths','birds_sold','heads','account_id']:
                val = safe_int(val, None) if f == 'account_id' else int(val or 0)
            elif f in ['cost','bags','amount','kilos','price_per_kilo','total_amount']:
                val = float(val or 0)
            fields.append(f'{f}=?')
            values.append(val)
        if record_type == 'poultry_sale':
            kilos = float(request.form.get('kilos') or 0)
            ppk = float(request.form.get('price_per_kilo') or 0)
            fields.append('total_amount=?')
            values.append(kilos * ppk)
        if record_type == 'fish_tx':
            kilos = float(request.form.get('kilos') or 0)
            ppk = float(request.form.get('price_per_kilo') or 0)
            fields.append('total_amount=?')
            values.append(kilos * ppk)
        values.append(record_id)
        execute(f'UPDATE {cfg["table"]} SET {", ".join(fields)} WHERE id=?', tuple(values))
        if record_type == 'bank_tx':
            updated = query('SELECT * FROM bank_transactions WHERE id=?', (record_id,), one=True)
            updated_account_id = safe_int(row_value(updated, 'account_id'))
            touched = {aid for aid in [previous_account_id, updated_account_id] if aid}
            for aid in touched:
                recompute_bank_balance(aid)
        log_audit('EDIT', record_type, record_id, 'Record updated')
        flash('Record updated.', 'success')
        if cfg['redirect'] == 'bank_page':
            return redirect(url_for('bank_page'))
        if cfg['redirect'] == 'finance_page':
            return redirect(url_for('finance_page'))
        return redirect(url_for(cfg['redirect'], tab=cfg['tab']))
    return render_template('record_edit.html', record=row, record_type=record_type, fields=cfg['fields'], expense_categories=EXPENSE_CATEGORIES, tx_types=TX_TYPES, bank_accounts=list_bank_accounts(), modules=MODULES, hog_types=HOG_TYPES)


@app.route('/record/delete/<record_type>/<int:record_id>')
@login_required
def record_delete(record_type, record_id):
    if not is_admin() and not record_type.startswith('poultry_'):
        flash('You do not have access to delete that record.', 'danger')
        return redirect(url_for('poultry_page'))
    cfg = get_edit_config(record_type)
    if not cfg:
        flash('Unknown record type.', 'danger')
        return redirect(url_for('dashboard'))
    affected_account_id = safe_int(row_value(query(f'SELECT * FROM {cfg["table"]} WHERE id=?', (record_id,), one=True), 'account_id')) if record_type == 'bank_tx' else None
    log_audit('DELETE', record_type, record_id, 'Record deleted')
    execute(f'DELETE FROM {cfg["table"]} WHERE id=?', (record_id,))
    if record_type == 'bank_tx' and affected_account_id:
        recompute_bank_balance(affected_account_id)
    flash('Record deleted.', 'success')
    if cfg['redirect'] == 'bank_page':
        return redirect(url_for('bank_page'))
    if cfg['redirect'] == 'finance_page':
        return redirect(url_for('finance_page'))
    return redirect(url_for(cfg['redirect'], tab=cfg['tab']))


@app.route('/finance')
@admin_required
def finance_page():
    poultry = finance_summary_for_module('POULTRY', active_only=True)
    hog = finance_summary_for_module('HOG', active_only=True)
    fish = finance_summary_for_module('FISH', active_only=True)
    global_cash_in = poultry['capital'] + poultry['revenue'] + hog['capital'] + hog['revenue'] + fish['capital'] + fish['revenue']
    global_cash_out = poultry['expenses'] + hog['expenses'] + fish['expenses']
    return render_template('finance.html', poultry=poultry, hog=hog, fish=fish, farm={'cash_in':global_cash_in,'cash_out':global_cash_out,'remaining':global_cash_in-global_cash_out}, cycles=query('SELECT * FROM cycles ORDER BY id DESC LIMIT 100'), capital=query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id ORDER BY ce.id DESC LIMIT 100'), bank_accounts=list_bank_accounts())


def initialize_database():
    init_db()
    migrate_db()
    try:
        recompute_all_bank_balances()
    except Exception:
        pass

initialize_database()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '5000')), debug=os.getenv('FLASK_DEBUG', '0') == '1')
