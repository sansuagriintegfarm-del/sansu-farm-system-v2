import os
import sqlite3
from pathlib import Path
from datetime import date
from functools import wraps
from flask import Flask, g, render_template, request, redirect, url_for, session, flash, send_from_directory, abort
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

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


def get_db():
    if 'db' not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        g.db = conn
    return g.db


def query(sql, params=(), one=False, commit=False):
    db = get_db()
    cur = db.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    if commit:
        db.commit()
    return (rows[0] if rows else None) if one else rows


def execute(sql, params=()):
    db = get_db()
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



def today_str():
    return date.today().isoformat()


def str_or_none(value, default=None):
    value = (value or '').strip() if isinstance(value, str) or value is None else value
    return value if value not in ('', None) else default


def as_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except (TypeError, ValueError):
        return default


def as_float(value, default=0.0):
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def as_date(value, default=None):
    value = str_or_none(value)
    return value or (default if default is not None else today_str())


def latest_row(table, where_sql='', params=()):
    sql = f'SELECT * FROM {table}'
    if where_sql:
        sql += f' WHERE {where_sql}'
    sql += ' ORDER BY id DESC LIMIT 1'
    return query(sql, params, one=True)


def latest_poultry_batch(cycle_id=None):
    if cycle_id:
        row = latest_row('poultry_batches', 'cycle_id=?', (cycle_id,))
        if row:
            return row
    return latest_row('poultry_batches')


def latest_hog_cycle(cycle_id=None):
    if cycle_id:
        row = latest_row('hog_cycles', 'cycle_id=?', (cycle_id,))
        if row:
            return row
    return latest_row('hog_cycles')


def latest_fish_cycle(cycle_id=None):
    if cycle_id:
        row = latest_row('fish_cycles', 'cycle_id=? AND status="ACTIVE"', (cycle_id,))
        if row:
            return row
    return latest_row('fish_cycles')


def safe_batch_cycle_id(batch_id):
    if not batch_id:
        return None
    row = query('SELECT cycle_id FROM poultry_batches WHERE id=?', (batch_id,), one=True)
    return row['cycle_id'] if row else None

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript('''
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        full_name TEXT,
        role TEXT
    );
    CREATE TABLE IF NOT EXISTS participants(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        notes TEXT,
        active INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS bank_accounts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_name TEXT NOT NULL,
        bank_name TEXT NOT NULL,
        account_type TEXT NOT NULL,
        opening_balance REAL DEFAULT 0,
        current_balance REAL DEFAULT 0,
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS bank_transactions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    CREATE TABLE IF NOT EXISTS cycles(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module_name TEXT NOT NULL,
        cycle_name TEXT NOT NULL,
        poultry_type TEXT,
        start_date TEXT NOT NULL,
        end_date TEXT,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT
    );
    CREATE TABLE IF NOT EXISTS cycle_participants(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cycle_id INTEGER NOT NULL,
        participant_id INTEGER NOT NULL,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id) ON DELETE CASCADE,
        FOREIGN KEY(participant_id) REFERENCES participants(id)
    );
    CREATE TABLE IF NOT EXISTS capital_entries(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        deaths INTEGER DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS poultry_feed_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        feed_type TEXT,
        bags REAL DEFAULT 0,
        amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS poultry_sales(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER,
        entry_date TEXT NOT NULL,
        category TEXT,
        amount REAL DEFAULT 0,
        description TEXT,
        FOREIGN KEY(batch_id) REFERENCES poultry_batches(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS hog_cycles(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cycle_id INTEGER,
        pen_name TEXT,
        start_date TEXT,
        heads INTEGER DEFAULT 0,
        source TEXT,
        cost REAL DEFAULT 0,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id)
    );
    CREATE TABLE IF NOT EXISTS hog_feed_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hog_cycle_id INTEGER,
        entry_date TEXT,
        feed_type TEXT,
        quantity TEXT,
        amount REAL DEFAULT 0,
        notes TEXT,
        FOREIGN KEY(hog_cycle_id) REFERENCES hog_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS hog_sales(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hog_cycle_id INTEGER,
        entry_date TEXT,
        category TEXT,
        amount REAL DEFAULT 0,
        description TEXT,
        FOREIGN KEY(hog_cycle_id) REFERENCES hog_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS fish_cycles(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cycle_id INTEGER,
        period_name TEXT,
        start_date TEXT,
        status TEXT DEFAULT 'ACTIVE',
        notes TEXT,
        FOREIGN KEY(cycle_id) REFERENCES cycles(id)
    );
    CREATE TABLE IF NOT EXISTS fish_transactions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fish_cycle_id INTEGER,
        entry_date TEXT,
        category TEXT,
        amount REAL DEFAULT 0,
        description TEXT,
        group_ref TEXT,
        FOREIGN KEY(fish_cycle_id) REFERENCES fish_cycles(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS feed_inventory(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_time TEXT DEFAULT CURRENT_TIMESTAMP,
        event_type TEXT,
        record_type TEXT,
        record_id INTEGER,
        details TEXT
    );
    ''')
    existing_users = {row[0] for row in db.execute("SELECT username FROM users").fetchall()}
    if 'admin' not in existing_users:
        db.execute('INSERT INTO users(username,password,full_name,role) VALUES(?,?,?,?)', ('admin', generate_password_hash('admin123'), 'Administrator', 'Admin'))
    if 'secretary' not in existing_users:
        db.execute('INSERT INTO users(username,password,full_name,role) VALUES(?,?,?,?)', ('secretary', generate_password_hash('secretary123'), 'Farm Secretary', 'Secretary'))
    db.commit()
    db.close()


def get_cycle(module):
    return query('SELECT * FROM cycles WHERE module_name=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (module,), one=True)


def cycle_history(module):
    return query('SELECT * FROM cycles WHERE module_name=? ORDER BY id DESC', (module,))


def participant_shares(cycle_id, profit):
    parts = query('''SELECT p.* FROM cycle_participants cp JOIN participants p ON p.id=cp.participant_id WHERE cp.cycle_id=?''', (cycle_id,))
    count = len(parts)
    share = profit / count if count else 0
    return parts, share


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
            revenue = query('SELECT COALESCE(SUM(ps.total_amount),0) v FROM poultry_sales ps JOIN poultry_batches pb ON pb.id=ps.batch_id WHERE pb.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses = query('SELECT COALESCE(SUM(pb.cost),0) v FROM poultry_batches pb WHERE pb.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(pf.amount),0) v FROM poultry_feed_logs pf JOIN poultry_batches pb ON pb.id=pf.batch_id WHERE pb.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(pe.amount),0) v FROM poultry_expenses pe JOIN poultry_batches pb ON pb.id=pe.batch_id WHERE pb.cycle_id=?', (cycle['id'],), one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY" AND cycle_id=?', (cycle['id'],), one=True)['v']
        else:
            revenue = query('SELECT COALESCE(SUM(total_amount),0) v FROM poultry_sales', one=True)['v']
            expenses = query('SELECT COALESCE(SUM(cost),0) v FROM poultry_batches', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM poultry_feed_logs', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM poultry_expenses', one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY"', one=True)['v']
    elif module == 'HOG':
        if active_only and get_cycle('HOG'):
            cycle = get_cycle('HOG')
            revenue = query('SELECT COALESCE(SUM(hs.total_amount),0) v FROM hog_sales hs JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id WHERE hc.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses = query('SELECT COALESCE(SUM(hc.cost),0) v FROM hog_cycles hc WHERE hc.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(hf.amount),0) v FROM hog_feed_logs hf JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id WHERE hc.cycle_id=?', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(he.amount),0) v FROM hog_expenses he JOIN hog_cycles hc ON hc.id=he.hog_cycle_id WHERE hc.cycle_id=?', (cycle['id'],), one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="HOG" AND cycle_id=?', (cycle['id'],), one=True)['v']
        else:
            revenue = query('SELECT COALESCE(SUM(total_amount),0) v FROM hog_sales', one=True)['v']
            expenses = query('SELECT COALESCE(SUM(cost),0) v FROM hog_cycles', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM hog_feed_logs', one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM hog_expenses', one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="HOG"', one=True)['v']
    else:
        if active_only and get_cycle('FISH'):
            cycle = get_cycle('FISH')
            revenue = query('SELECT COALESCE(SUM(CASE WHEN ft.transaction_type="SELL" THEN ft.total_amount ELSE 0 END),0) v FROM fish_transactions ft WHERE ft.fish_cycle_id IN (SELECT id FROM fish_cycles WHERE cycle_id=?)', (cycle['id'],), one=True)['v']
            expenses = query('SELECT COALESCE(SUM(CASE WHEN ft.transaction_type="BUY" THEN ft.total_amount ELSE 0 END),0) v FROM fish_transactions ft WHERE ft.fish_cycle_id IN (SELECT id FROM fish_cycles WHERE cycle_id=?)', (cycle['id'],), one=True)['v']
            expenses += query('SELECT COALESCE(SUM(amount),0) v FROM fish_expenses WHERE fish_cycle_id IN (SELECT id FROM fish_cycles WHERE cycle_id=?)', (cycle['id'],), one=True)['v']
            capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="FISH" AND cycle_id=?', (cycle['id'],), one=True)['v']
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
        sale_sql = 'SELECT ps.id, ps.entry_date, "Sale" kind, COALESCE(ps.buyer,"Poultry sale") particulars, ps.total_amount cash_in, 0 cash_out, ps.notes FROM poultry_sales ps JOIN poultry_batches pb ON pb.id=ps.batch_id'
        params=[]
        if cycle:
            sale_sql += ' WHERE pb.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(sale_sql, tuple(params))]
        batch_sql = 'SELECT pb.id, pb.start_date entry_date, "Batch Cost" kind, pb.batch_name particulars, 0 cash_in, pb.cost cash_out, pb.notes notes FROM poultry_batches pb'
        params=[]
        if cycle:
            batch_sql += ' WHERE pb.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(batch_sql, tuple(params))]
        feed_sql = 'SELECT pf.id, pf.entry_date, "Feed" kind, COALESCE(pf.feed_type,"Feed log") particulars, 0 cash_in, pf.amount cash_out, pf.notes FROM poultry_feed_logs pf JOIN poultry_batches pb ON pb.id=pf.batch_id'
        params=[]
        if cycle:
            feed_sql += ' WHERE pb.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(feed_sql, tuple(params))]
        exp_sql = 'SELECT pe.id, pe.entry_date, pe.category kind, COALESCE(pe.description, pe.category) particulars, 0 cash_in, pe.amount cash_out, pe.description notes FROM poultry_expenses pe JOIN poultry_batches pb ON pb.id=pe.batch_id'
        params=[]
        if cycle:
            exp_sql += ' WHERE pb.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(exp_sql, tuple(params))]
    elif module == 'HOG':
        cycle = get_cycle('HOG') if active_only else None
        cap_sql = 'SELECT ce.id, ce.entry_date, "Capital" kind, ce.source_name particulars, ce.amount cash_in, 0 cash_out, ce.notes FROM capital_entries ce WHERE ce.module_name="HOG"'
        params=[]
        if cycle:
            cap_sql += ' AND ce.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(cap_sql, tuple(params))]
        sales_sql = 'SELECT hs.id, hs.entry_date, "Sale" kind, COALESCE(hs.buyer,"Hog sale") particulars, hs.total_amount cash_in, 0 cash_out, hs.notes FROM hog_sales hs JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id'
        params=[]
        if cycle:
            sales_sql += ' WHERE hc.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(sales_sql, tuple(params))]
        base_sql = 'SELECT hc.id, hc.start_date entry_date, "Cycle Cost" kind, hc.pen_name particulars, 0 cash_in, hc.cost cash_out, hc.notes FROM hog_cycles hc'
        params=[]
        if cycle:
            base_sql += ' WHERE hc.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(base_sql, tuple(params))]
        feed_sql = 'SELECT hf.id, hf.entry_date, "Feed" kind, COALESCE(hf.feed_type,"Feed log") particulars, 0 cash_in, hf.amount cash_out, hf.notes FROM hog_feed_logs hf JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id'
        params=[]
        if cycle:
            feed_sql += ' WHERE hc.cycle_id=?'; params.append(cycle['id'])
        rows += [dict(r) for r in query(feed_sql, tuple(params))]
        exp_sql = 'SELECT he.id, he.entry_date, he.category kind, COALESCE(he.description, he.category) particulars, 0 cash_in, he.amount cash_out, he.description notes FROM hog_expenses he JOIN hog_cycles hc ON hc.id=he.hog_cycle_id'
        params=[]
        if cycle:
            exp_sql += ' WHERE hc.cycle_id=?'; params.append(cycle['id'])
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
            tx_sql += ' WHERE ft.fish_cycle_id IN (SELECT id FROM fish_cycles WHERE cycle_id=?)'; params.append(cycle['id'])
        rows += [dict(r) for r in query(tx_sql, tuple(params))]
        exp_sql = 'SELECT fe.id, fe.entry_date, fe.category kind, COALESCE(fe.description, fe.category) particulars, 0 cash_in, fe.amount cash_out, fe.description notes FROM fish_expenses fe'
        params=[]
        if cycle:
            exp_sql += ' WHERE fe.fish_cycle_id IN (SELECT id FROM fish_cycles WHERE cycle_id=?)'; params.append(cycle['id'])
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
        cycle_capital = query('SELECT COALESCE(SUM(amount),0) v FROM capital_entries WHERE module_name="POULTRY" AND cycle_id=?', (cycle['id'],), one=True)['v']
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
        row['income_share'] = round((row['profit'] / participant_count), 2) if participant_count else 0
        row['mortality_rate'] = safe_pct(row['mortality'], row['birds_placed'])
        row['batches_label'] = ', '.join(row['batch_names'])
    return sorted(houses.values(), key=lambda r: r['house_name'])


def dashboard_context():
    poultry_active = finance_summary_for_module('POULTRY', active_only=True)
    hog_active = finance_summary_for_module('HOG', active_only=True)
    fish_active = finance_summary_for_module('FISH', active_only=True)
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
    revenue_chart = {k.lower(): float(v['revenue'] or 0) for k, v in modules.items()}
    expense_chart = {k.lower(): float(v['expenses'] or 0) for k, v in modules.items()}
    capital_chart = {k.lower(): float(v['capital'] or 0) for k, v in modules.items()}
    return {
        'poultry': poultry_active,
        'hog': hog_active,
        'fish': fish_active,
        'module_mix': module_mix,
        'revenue_chart': revenue_chart,
        'expense_chart': expense_chart,
        'capital_chart': capital_chart,
        'poultry_live': max(0, poultry_live),
        'hog_heads': max(0, hog_heads),
        'hog_breakdown': {k:int(v) for k,v in hog_breakdown.items()},
        'fish_kilos': max(0, float(fish_kilos or 0)),
        'fish_breakdown': {'buy': fish_buy, 'export': fish_export, 'sales': fish_sales},
        'bank_total': query('SELECT COALESCE(SUM(current_balance),0) v FROM bank_accounts', one=True)['v'],
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
            sales = [r for r in sales if safe_batch_cycle_id(r['batch_id']) == cycle['id']]
            feed_logs = [r for r in feed_logs if safe_batch_cycle_id(r['batch_id']) == cycle['id']]
            expenses = [r for r in expenses if (safe_batch_cycle_id(r['batch_id']) == cycle['id']) or (r['batch_id'] is None)]
            capital = [r for r in capital if r['cycle_id'] == cycle['id']]
        for r in capital:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Capital', 'details': r['source_name'] or 'Capital entry', 'amount': float(r['amount'] or 0), 'record_type': 'capital', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in sales:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Sale', 'details': (r['buyer'] or 'Poultry sale') + (f" - {r['batch_name']}" if r['batch_name'] else ''), 'amount': float(r['total_amount'] or 0), 'record_type': 'poultry_sale', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in feed_logs:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Feed Expense', 'details': (r['feed_type'] or 'Feed log') + (f" - {r['batch_name']}" if r['batch_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'poultry_feed', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
        for r in expenses:
            rows.append({'entry_date': r['entry_date'], 'type_label': r['category'] or 'Expense', 'details': (r['description'] or r['category'] or 'Expense') + (f" - {r['batch_name']}" if r['batch_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'poultry_expense', 'record_id': r['id'], 'nature':'out', 'receipt_file': r['receipt_file']})
        for r in batches:
            rows.append({'entry_date': r['start_date'], 'type_label': 'Batch Record' if float(r['cost'] or 0) == 0 else 'Batch Cost', 'details': r['batch_name'], 'amount': float(r['cost'] or 0), 'record_type': 'poultry_batch', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
    elif module == 'HOG':
        cycle = get_cycle('HOG') if active_only else None
        cycles = query('SELECT * FROM hog_cycles ORDER BY id DESC')
        sales = query('SELECT hs.*, hc.pen_name, hc.cycle_id parent_cycle_id FROM hog_sales hs LEFT JOIN hog_cycles hc ON hc.id=hs.hog_cycle_id ORDER BY hs.id DESC')
        feed_logs = query('SELECT hf.*, hc.pen_name, hc.cycle_id parent_cycle_id FROM hog_feed_logs hf LEFT JOIN hog_cycles hc ON hc.id=hf.hog_cycle_id ORDER BY hf.id DESC')
        expenses = query('SELECT he.*, hc.pen_name, hc.cycle_id parent_cycle_id FROM hog_expenses he LEFT JOIN hog_cycles hc ON hc.id=he.hog_cycle_id ORDER BY he.id DESC')
        capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="HOG" ORDER BY ce.id DESC')
        if cycle:
            cycles = [r for r in cycles if r['cycle_id'] == cycle['id']]
            sales = [r for r in sales if r['parent_cycle_id'] == cycle['id']]
            feed_logs = [r for r in feed_logs if r['parent_cycle_id'] == cycle['id']]
            expenses = [r for r in expenses if r['parent_cycle_id'] == cycle['id']]
            capital = [r for r in capital if r['cycle_id'] == cycle['id']]
        for r in capital:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Capital', 'details': r['source_name'] or 'Capital entry', 'amount': float(r['amount'] or 0), 'record_type': 'capital', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in sales:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Sale', 'details': (r['buyer'] or 'Hog sale') + (f" - {r['pen_name']}" if r['pen_name'] else ''), 'amount': float(r['total_amount'] or 0), 'record_type': 'hog_sale', 'record_id': r['id'], 'nature':'in', 'receipt_file': None})
        for r in feed_logs:
            rows.append({'entry_date': r['entry_date'], 'type_label': 'Feed Expense', 'details': (r['feed_type'] or 'Feed log') + (f" - {r['pen_name']}" if r['pen_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'hog_feed', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
        for r in expenses:
            rows.append({'entry_date': r['entry_date'], 'type_label': r['category'] or 'Expense', 'details': (r['description'] or r['category'] or 'Expense') + (f" - {r['pen_name']}" if r['pen_name'] else ''), 'amount': float(r['amount'] or 0), 'record_type': 'hog_expense', 'record_id': r['id'], 'nature':'out', 'receipt_file': r['receipt_file']})
        for r in cycles:
            rows.append({'entry_date': r['start_date'], 'type_label': 'Cycle Record' if float(r['cost'] or 0) == 0 else 'Cycle Cost', 'details': r['pen_name'] or 'Hog cycle', 'amount': float(r['cost'] or 0), 'record_type': 'hog_cycle', 'record_id': r['id'], 'nature':'out', 'receipt_file': None})
    else:
        cycle = get_cycle('FISH') if active_only else None
        tx = query('SELECT ft.*, fc.cycle_id parent_cycle_id FROM fish_transactions ft LEFT JOIN fish_cycles fc ON fc.id=ft.fish_cycle_id ORDER BY ft.id DESC')
        expenses = query('SELECT fe.*, fc.cycle_id parent_cycle_id FROM fish_expenses fe LEFT JOIN fish_cycles fc ON fc.id=fe.fish_cycle_id ORDER BY fe.id DESC')
        capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="FISH" ORDER BY ce.id DESC')
        if cycle:
            tx = [r for r in tx if r['parent_cycle_id'] == cycle['id']]
            expenses = [r for r in expenses if r['parent_cycle_id'] == cycle['id']]
            capital = [r for r in capital if r['cycle_id'] == cycle['id']]
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
    ensure_column('poultry_expenses', 'group_ref', 'TEXT')
    ensure_column('hog_expenses', 'group_ref', 'TEXT')
    ensure_column('fish_expenses', 'group_ref', 'TEXT')
    ensure_column('poultry_expenses', 'receipt_file', 'TEXT')
    ensure_column('hog_expenses', 'receipt_file', 'TEXT')
    ensure_column('fish_expenses', 'receipt_file', 'TEXT')


def log_audit(event_type, record_type, record_id, details):
    execute('INSERT INTO audit_log(event_type,record_type,record_id,details) VALUES(?,?,?,?)', (event_type, record_type, record_id, details))


def parse_bulk_rows(form):
    rows=[]
    for idx in range(1,21):
        category = (form.get(f'item_category_{idx}') or '').strip()
        description = (form.get(f'item_description_{idx}') or '').strip()
        qty = as_float(form.get(f'item_qty_{idx}'), 0)
        unit_cost = as_float(form.get(f'item_unit_cost_{idx}'), 0)
        amount = as_float(form.get(f'item_total_{idx}'), 0)
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


@app.route('/participants', methods=['GET', 'POST'])
@admin_required
def participants_page():
    if request.method == 'POST':
        execute('INSERT INTO participants(name,role,notes) VALUES(?,?,?)', (request.form['name'], request.form['role'], request.form.get('notes')))
        flash('Participant added.', 'success')
        return redirect(url_for('participants_page'))
    return render_template('participants.html', participants=query('SELECT * FROM participants ORDER BY id DESC'), roles=PARTICIPANT_ROLES)


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
        execute('INSERT INTO bank_accounts(account_name,bank_name,account_type,opening_balance,current_balance,notes) VALUES(?,?,?,?,?,?)',
                (request.form['account_name'], request.form['bank_name'], request.form['account_type'], opening, opening, request.form.get('notes')))
        flash('Account added.', 'success')
        return redirect(url_for('bank_page'))
    accounts = query('SELECT * FROM bank_accounts ORDER BY id DESC')
    tx = query('SELECT bt.*, ba.account_name FROM bank_transactions bt LEFT JOIN bank_accounts ba ON ba.id=bt.account_id ORDER BY bt.id DESC LIMIT 60')
    return render_template('bank.html', accounts=accounts, tx=tx, bank_types=BANK_TYPES, tx_types=TX_TYPES)


@app.route('/bank/tx/add', methods=['POST'])
@admin_required
def bank_tx_add():
    account_id = as_int(request.form.get('account_id'), 0)
    amount = as_float(request.form.get('amount'), 0)
    tx_type = request.form['tx_type']
    execute('INSERT INTO bank_transactions(entry_date,account_id,module_name,tx_type,amount,reference_no,purpose,notes) VALUES(?,?,?,?,?,?,?,?)',
            (as_date(request.form.get('entry_date')), account_id, str_or_none(request.form.get('module_name')), tx_type, amount, str_or_none(request.form.get('reference_no')), str_or_none(request.form.get('purpose')), str_or_none(request.form.get('notes'))))
    account = query('SELECT * FROM bank_accounts WHERE id=?', (account_id,), one=True)
    new_balance = float(account['current_balance']) + amount if tx_type in ['DEPOSIT', 'TRANSFER IN'] else float(account['current_balance']) - amount
    execute('UPDATE bank_accounts SET current_balance=? WHERE id=?', (new_balance, account_id))
    log_audit('ADD', 'bank_tx', account_id, f'{tx_type} {amount}')
    flash('Bank transaction added.', 'success')
    return redirect(url_for('bank_page'))


@app.route('/capital/add', methods=['POST'])
@login_required
def capital_add():
    if not can_access_module(request.form.get('module_name')):
        flash('You do not have access to add capital to that module.', 'danger')
        return redirect(url_for('poultry_page'))
    execute('INSERT INTO capital_entries(cycle_id,module_name,entry_date,source_name,amount,destination_account_id,notes) VALUES(?,?,?,?,?,?,?)',
            (request.form.get('cycle_id') or None, request.form['module_name'], as_date(request.form.get('entry_date')), str_or_none(request.form.get('source_name')), as_float(request.form.get('amount'), 0), request.form.get('destination_account_id') or None, str_or_none(request.form.get('notes'))))
    log_audit('ADD', 'capital', 0, f"{request.form['module_name']} capital {request.form.get('amount')}")
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
    cycle_id = start_cycle(request.form['module_name'], str_or_none(request.form.get('cycle_name'),'New Cycle'), str_or_none(request.form.get('poultry_type')), str_or_none(request.form.get('notes')), request.form.getlist('participants'))
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
                    (cycle['id'] if cycle else None, str_or_none(request.form.get('poultry_type'),'Broiler'), str_or_none(request.form.get('batch_name'),'Untitled Batch'), str_or_none(request.form.get('house_name')), as_date(request.form.get('start_date')), as_int(request.form.get('birds_count'), 0), str_or_none(request.form.get('supplier')), as_float(request.form.get('cost'), 0), str_or_none(request.form.get('notes'))))
            flash('Batch added.', 'success')
            return redirect(url_for('poultry_page', tab='batches'))
        if form_name == 'mortality':
            execute('INSERT INTO poultry_mortality(batch_id,entry_date,deaths,notes) VALUES(?,?,?,?)', (as_int(request.form.get('batch_id') or (latest_poultry_batch(cycle['id'] if cycle else None) or {'id':0})['id'], 0), as_date(request.form.get('entry_date')), as_int(request.form.get('deaths'), 0), str_or_none(request.form.get('notes'))))
            flash('Mortality saved.', 'success')
            return redirect(url_for('poultry_page', tab='operations'))
        if form_name == 'feed':
            feed_id = execute('INSERT INTO poultry_feed_logs(batch_id,entry_date,feed_type,bags,amount,notes) VALUES(?,?,?,?,?,?)', (as_int(request.form.get('batch_id') or (latest_poultry_batch(cycle['id'] if cycle else None) or {'id':0})['id'], 0), as_date(request.form.get('entry_date')), str_or_none(request.form.get('feed_type'),'Feed'), as_float(request.form.get('bags'), 0), as_float(request.form.get('amount'), 0), str_or_none(request.form.get('notes'))))
            log_audit('ADD', 'poultry_feed', feed_id, f"{request.form.get('feed_type')} {request.form.get('bags')} sacks")
            flash('Feed log saved.', 'success')
            return redirect(url_for('poultry_page', tab='operations'))
        if form_name == 'feed_stock':
            sacks = float(request.form.get('sacks') or 0)
            cps = float(request.form.get('cost_per_sack') or 0)
            total = sacks * cps
            inv_id = execute('INSERT INTO feed_inventory(module_name,entry_date,feed_type,sacks,cost_per_sack,total_cost,source_name,usage_type,house_name,cycle_id,notes) VALUES(?,?,?,?,?,?,?,?,?,?,?)', ('POULTRY', as_date(request.form.get('entry_date')), str_or_none(request.form.get('feed_type'),'Feed'), sacks, cps, total, str_or_none(request.form.get('source_name')), str_or_none(request.form.get('usage_type'),'Add to Inventory'), str_or_none(request.form.get('house_name')), cycle['id'] if cycle else None, str_or_none(request.form.get('notes'))))
            log_audit('ADD', 'feed_inventory', inv_id, f"{request.form.get('feed_type')} {sacks} sacks")
            flash('Feed stock saved.', 'success')
            return redirect(url_for('poultry_page', tab='operations'))
        if form_name == 'sale':
            kilos = as_float(request.form.get('kilos'), 0)
            ppk = as_float(request.form.get('price_per_kilo'), 0)
            total = kilos * ppk
            default_batch = latest_poultry_batch(cycle['id'] if cycle else None)
            batch_id = as_int(request.form.get('batch_id') or (default_batch['id'] if default_batch else 0), 0)
            execute('INSERT INTO poultry_sales(batch_id,entry_date,buyer,birds_sold,kilos,price_per_kilo,total_amount,notes) VALUES(?,?,?,?,?,?,?,?)',
                    (batch_id, as_date(request.form.get('entry_date')), str_or_none(request.form.get('buyer')), as_int(request.form.get('birds_sold'), 0), kilos, ppk, total, str_or_none(request.form.get('notes'))))
            flash('Poultry sale saved.', 'success')
            return redirect(url_for('poultry_page', tab='finance'))
        if form_name == 'expense':
            receipt_file = save_uploaded_receipt(
                request.files.get('receipt_file'),
                f"poultry_{request.form.get('entry_date')}"
            )
            exp_id = execute(
                'INSERT INTO poultry_expenses(batch_id,entry_date,category,amount,description,receipt_file) VALUES(?,?,?,?,?,?)',
                (
                    as_int(request.form.get('batch_id') or ((latest_poultry_batch(cycle['id'] if cycle else None) or {'id': 0})['id']), 0) or None,
                    as_date(request.form.get('entry_date')),
                    str_or_none(request.form.get('category'),'Other'),
                    as_float(request.form.get('amount'), 0),
                    f"{str_or_none(request.form.get('item_name'),'Expense Item')}{' - ' + request.form.get('description') if request.form.get('description') else ''}",
                    receipt_file
                )
            )
            log_audit('ADD', 'poultry_expense', exp_id, request.form.get('description'))
            flash('Expense saved.', 'success')
            return redirect(url_for('poultry_page', tab='finance'))
        if form_name == 'bulk_expense':
            rows = parse_bulk_rows(request.form)
            batch_id = as_int(request.form.get('batch_id') or ((latest_poultry_batch(cycle['id'] if cycle else None) or {'id': 0})['id']), 0)
            group_ref = f"PB-{as_date(request.form.get('entry_date'))}-{batch_id}-{len(rows)}"
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), group_ref.lower())
            for item in rows:
                desc = f"{item['description']} | Qty: {item['qty']} | Unit Cost: {item['unit_cost']}"
                execute('INSERT INTO poultry_expenses(batch_id,entry_date,category,amount,description,group_ref,receipt_file) VALUES(?,?,?,?,?,?,?)', (batch_id or None, as_date(request.form.get('entry_date')), item['category'], item['amount'], desc, group_ref, receipt_file))
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
    parts, share = participant_shares(cycle['id'], finance['profit']) if cycle else ([], 0)
    return render_template('poultry.html', tab=tab, cycle=cycle, cycles=cycle_history('POULTRY'), batches=batches, mortality=mortality, feed_logs=feed_logs, sales=sales, expenses=expenses, capital=capital, finance=finance, finance_history=finance_history, participants=query('SELECT * FROM participants WHERE active=1'), share=share, share_participants=parts, poultry_types=POULTRY_TYPES, expense_categories=EXPENSE_CATEGORIES, bank_accounts=query('SELECT * FROM bank_accounts ORDER BY account_name'), cashflow_rows=cashflow_rows, cashflow_summary=cashflow_summary, visuals=visuals, feed_inventory=feed_inventory, feed_stock_remaining=current_feed_stock('POULTRY'), feed_usage_types=FEED_USAGE_TYPES, bulk_groups=bulk_groups)


@app.route('/hog', methods=['GET', 'POST'])
@admin_required
def hog_page():
    tab = request.args.get('tab', 'overview')
    cycle = get_cycle('HOG')
    if request.method == 'POST':
        form_name = request.form.get('form_name')
        if form_name == 'hog_cycle':
            execute('INSERT INTO hog_cycles(cycle_id,pen_name,start_date,heads,source,cost,notes) VALUES(?,?,?,?,?,?,?)',
                    (cycle['id'] if cycle else None, str_or_none(request.form.get('pen_name'),'Unassigned Pen'), as_date(request.form.get('start_date')), as_int(request.form.get('heads'), 0), str_or_none(request.form.get('source')), as_float(request.form.get('cost'), 0), str_or_none(request.form.get('notes'))))
            flash('Hog record added.', 'success')
            return redirect(url_for('hog_page', tab='operations'))
        if form_name == 'hog_feed':
            execute('INSERT INTO hog_feed_logs(hog_cycle_id,entry_date,feed_type,quantity,amount,notes) VALUES(?,?,?,?,?,?)',
                    (as_int(request.form.get('hog_cycle_id') or ((latest_hog_cycle(cycle['id'] if cycle else None) or {'id':0})['id']), 0), as_date(request.form.get('entry_date')), str_or_none(request.form.get('feed_type'),'Feed'), str_or_none(request.form.get('quantity'),'0'), as_float(request.form.get('amount'), 0), str_or_none(request.form.get('notes'))))
            flash('Hog feed saved.', 'success')
            return redirect(url_for('hog_page', tab='operations'))
        if form_name == 'hog_sale':
            execute('INSERT INTO hog_sales(hog_cycle_id,entry_date,buyer,heads,kilos,total_amount,notes) VALUES(?,?,?,?,?,?,?)',
                    (as_int(request.form.get('hog_cycle_id') or ((latest_hog_cycle(cycle['id'] if cycle else None) or {'id':0})['id']), 0), as_date(request.form.get('entry_date')), str_or_none(request.form.get('buyer')), as_int(request.form.get('heads'), 0), as_float(request.form.get('kilos'), 0), as_float(request.form.get('total_amount'), 0), str_or_none(request.form.get('notes'))))
            flash('Hog sale saved.', 'success')
            return redirect(url_for('hog_page', tab='finance'))
        if form_name == 'hog_expense':
            execute('INSERT INTO hog_expenses(hog_cycle_id,entry_date,category,amount,description) VALUES(?,?,?,?,?)',
                    (as_int(request.form.get('hog_cycle_id') or ((latest_hog_cycle(cycle['id'] if cycle else None) or {'id':0})['id']), 0), as_date(request.form.get('entry_date')), str_or_none(request.form.get('category'),'Other'), as_float(request.form.get('amount'), 0), str_or_none(request.form.get('description'),'Expense')))
            flash('Hog expense saved.', 'success')
            return redirect(url_for('hog_page', tab='finance'))
        if form_name == 'hog_bulk_expense':
            rows = parse_bulk_rows(request.form)
            hog_cycle_id = as_int(request.form.get('hog_cycle_id') or ((latest_hog_cycle(cycle['id'] if cycle else None) or {'id':0})['id']), 0)
            group_ref = f"HB-{as_date(request.form.get('entry_date'))}-{hog_cycle_id}-{len(rows)}"
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), group_ref.lower())
            for item in rows:
                execute('INSERT INTO hog_expenses(hog_cycle_id,entry_date,category,amount,description,group_ref,receipt_file) VALUES(?,?,?,?,?,?,?)', (hog_cycle_id or None, as_date(request.form.get('entry_date')), item['category'], item['amount'], f"{item['description']} | Qty: {item['qty']} | Unit Cost: {item['unit_cost']}", group_ref, receipt_file))
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
    parts, share = participant_shares(cycle['id'], finance['profit']) if cycle else ([], 0)
    return render_template('hog.html', tab=tab, cycle=cycle, cycle_history=cycle_history('HOG'), cycles=cycles, feed_logs=feed_logs, sales=sales, expenses=expenses, capital=capital, finance=finance, finance_history=finance_history, participants=query('SELECT * FROM participants WHERE active=1'), share=share, share_participants=parts, expense_categories=EXPENSE_CATEGORIES, bank_accounts=query('SELECT * FROM bank_accounts ORDER BY account_name'), cashflow_rows=cashflow_rows, cashflow_summary=cashflow_summary, visuals=visuals)


@app.route('/fish', methods=['GET', 'POST'])
@admin_required
def fish_page():
    tab = request.args.get('tab', 'overview')
    cycle = get_cycle('FISH')
    if request.method == 'POST':
        form_name = request.form.get('form_name')
        if form_name == 'fish_tx':
            kilos = as_float(request.form.get('kilos'), 0)
            ppk = as_float(request.form.get('price_per_kilo'), 0)
            total = kilos * ppk
            fish_cycle_id = execute('INSERT INTO fish_cycles(cycle_id,period_name,start_date,status,notes) SELECT ?, ?, ?, "ACTIVE", ? WHERE NOT EXISTS(SELECT 1 FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE")',
                                    (cycle['id'] if cycle else None, str_or_none(request.form.get('period_name'),'Default Period'), as_date(request.form.get('entry_date')), str_or_none(request.form.get('notes')), cycle['id'] if cycle else None)) if False else None
            active_fish_cycle = query('SELECT * FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True) if cycle else None
            if cycle and not active_fish_cycle:
                fc_id = execute('INSERT INTO fish_cycles(cycle_id,period_name,start_date,status,notes) VALUES(?,?,?,?,?)', (cycle['id'], str_or_none(request.form.get('period_name'), cycle['cycle_name']), as_date(request.form.get('entry_date')), 'ACTIVE', str_or_none(request.form.get('notes'))))
            else:
                fc_id = active_fish_cycle['id'] if active_fish_cycle else None
            execute('INSERT INTO fish_transactions(fish_cycle_id,entry_date,transaction_type,supplier,buyer,species,kilos,price_per_kilo,total_amount,notes) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (fc_id, as_date(request.form.get('entry_date')), str_or_none(request.form.get('transaction_type'),'BUY'), str_or_none(request.form.get('supplier')), str_or_none(request.form.get('buyer')), str_or_none(request.form.get('species'),'Mixed Fish'), kilos, ppk, total, str_or_none(request.form.get('notes'))))
            flash('Fish transaction saved.', 'success')
            return redirect(url_for('fish_page', tab='operations'))
        if form_name == 'fish_expense':
            active_fish_cycle = query('SELECT * FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True) if cycle else None
            fc_id = active_fish_cycle['id'] if active_fish_cycle else None
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), f"fish_{request.form.get('entry_date')}")
            execute('INSERT INTO fish_expenses(fish_cycle_id,entry_date,category,amount,description,receipt_file) VALUES(?,?,?,?,?,?)',
                    (fc_id, as_date(request.form.get('entry_date')), str_or_none(request.form.get('category'),'Other'), as_float(request.form.get('amount'), 0), str_or_none(request.form.get('description'),'Expense'), receipt_file))
            flash('Fish expense saved.', 'success')
            return redirect(url_for('fish_page', tab='finance'))
        if form_name == 'fish_bulk_expense':
            active_fish_cycle = query('SELECT * FROM fish_cycles WHERE cycle_id=? AND status="ACTIVE" ORDER BY id DESC LIMIT 1', (cycle['id'],), one=True) if cycle else None
            fc_id = active_fish_cycle['id'] if active_fish_cycle else None
            rows = parse_bulk_rows(request.form)
            group_ref = f"FB-{as_date(request.form.get('entry_date'))}-{int(fc_id or 0)}-{len(rows)}"
            receipt_file = save_uploaded_receipt(request.files.get('receipt_file'), group_ref.lower())
            for item in rows:
                execute('INSERT INTO fish_expenses(fish_cycle_id,entry_date,category,amount,description,group_ref,receipt_file) VALUES(?,?,?,?,?,?,?)', (fc_id, as_date(request.form.get('entry_date')), item['category'], item['amount'], f"{item['description']} | Qty: {item['qty']} | Unit Cost: {item['unit_cost']}", group_ref, receipt_file))
            flash(f'Fish bulk expense saved with {len(rows)} items.', 'success')
            return redirect(url_for('fish_page', tab='finance'))
    tx = query('SELECT * FROM fish_transactions ORDER BY id DESC')
    expenses = query('SELECT * FROM fish_expenses ORDER BY id DESC')
    capital = query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id WHERE module_name="FISH" ORDER BY ce.id DESC')
    finance = finance_summary_for_module('FISH', active_only=True)
    cashflow_rows, cashflow_summary = module_cashflow('FISH', active_only=True)
    finance_history = unified_finance_history('FISH', active_only=True)
    visuals = module_visuals(finance, cashflow_summary)
    parts, share = participant_shares(cycle['id'], finance['profit']) if cycle else ([], 0)
    return render_template('fish.html', tab=tab, cycle=cycle, cycle_history=cycle_history('FISH'), tx=tx, expenses=expenses, capital=capital, finance=finance, finance_history=finance_history, participants=query('SELECT * FROM participants WHERE active=1'), share=share, share_participants=parts, expense_categories=EXPENSE_CATEGORIES, bank_accounts=query('SELECT * FROM bank_accounts ORDER BY account_name'), cashflow_rows=cashflow_rows, cashflow_summary=cashflow_summary, visuals=visuals)


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
        'bank_tx': {'table':'bank_transactions','fields':['entry_date','module_name','tx_type','amount','reference_no','purpose','notes'],'redirect':'bank_page','tab':None},
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
        return redirect(url_for('dashboard' if is_admin() else 'poultry_page'))
    row = query(f'SELECT * FROM {cfg["table"]} WHERE id=?', (record_id,), one=True)
    if not row:
        flash('Record not found.', 'danger')
        return redirect(url_for('dashboard' if is_admin() else 'poultry_page'))
    if request.method == 'POST':
        fields = []
        values = []
        for f in cfg['fields']:
            val = request.form.get(f)
            if 'date' in f:
                val = as_date(val)
            elif f in ['birds_count','deaths','birds_sold','heads']:
                val = as_int(val, 0)
            elif f in ['cost','bags','amount','kilos','price_per_kilo','total_amount']:
                val = as_float(val, 0)
            else:
                val = str_or_none(val, '')
            fields.append(f'{f}=?')
            values.append(val)
        if record_type == 'poultry_sale':
            kilos = as_float(request.form.get('kilos'), 0)
            ppk = as_float(request.form.get('price_per_kilo'), 0)
            idx = cfg['fields'].index('price_per_kilo')
            # set computed total_amount separately
            fields.append('total_amount=?')
            values.append(kilos * ppk)
        if record_type == 'fish_tx':
            kilos = as_float(request.form.get('kilos'), 0)
            ppk = as_float(request.form.get('price_per_kilo'), 0)
            fields.append('total_amount=?')
            values.append(kilos * ppk)
        values.append(record_id)
        execute(f'UPDATE {cfg["table"]} SET {", ".join(fields)} WHERE id=?', tuple(values))
        log_audit('EDIT', record_type, record_id, 'Record updated')
        flash('Record updated.', 'success')
        if cfg['redirect'] == 'bank_page':
            return redirect(url_for('bank_page'))
        if cfg['redirect'] == 'finance_page':
            return redirect(url_for('finance_page'))
        return redirect(url_for(cfg['redirect'], tab=cfg['tab']))
    return render_template('record_edit.html', record=row, record_type=record_type, fields=cfg['fields'], expense_categories=EXPENSE_CATEGORIES, tx_types=TX_TYPES)


@app.route('/record/delete/<record_type>/<int:record_id>', methods=['GET', 'POST'])
@login_required
def record_delete(record_type, record_id):
    if not is_admin() and not record_type.startswith('poultry_'):
        flash('You do not have access to delete that record.', 'danger')
        return redirect(url_for('poultry_page'))
    cfg = get_edit_config(record_type)
    if not cfg:
        flash('Unknown record type.', 'danger')
        return redirect(url_for('dashboard'))
    log_audit('DELETE', record_type, record_id, 'Record deleted')
    execute(f'DELETE FROM {cfg["table"]} WHERE id=?', (record_id,))
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
    return render_template('finance.html', poultry=poultry, hog=hog, fish=fish, farm={'cash_in':global_cash_in,'cash_out':global_cash_out,'remaining':global_cash_in-global_cash_out}, cycles=query('SELECT * FROM cycles ORDER BY id DESC LIMIT 100'), capital=query('SELECT ce.*, ba.account_name FROM capital_entries ce LEFT JOIN bank_accounts ba ON ba.id=ce.destination_account_id ORDER BY ce.id DESC LIMIT 100'), bank_accounts=query('SELECT * FROM bank_accounts ORDER BY account_name'))


init_db()
migrate_db()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', '5000')), debug=os.getenv('FLASK_DEBUG', '0') == '1')
