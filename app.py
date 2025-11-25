import time
from datetime import date
import os

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

# --------------- Page config ---------------
st.set_page_config(
    page_title="KO Repairs — Footfall",
    page_icon="🏪",
    layout="centered"
)

# --------------- DB config ---------------
# Prefer Streamlit secret, fall back to env var (for local dev)
DB_URL = None
try:
    if "DB_URL" in st.secrets:
        DB_URL = st.secrets["DB_URL"]
except Exception:
    # st.secrets might not be available outside Streamlit Cloud
    DB_URL = os.getenv("DB_URL")

# Batch/flush settings
FLUSH_SECONDS = 600   # flush every 10 minutes
FLUSH_MAX = 50        # or when queue reaches this many rows

# --------------- DB (pooled) ---------------
@st.cache_resource(show_spinner=False)
def get_db():
    """Keep a persistent DB connection for speed."""
    if not DB_URL:
        st.error(
            "❌ Database URL is not configured.\n\n"
            "Set `DB_URL` in your Streamlit secrets or `DB_URL` environment variable."
        )
        st.stop()

    try:
        conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"❌ Could not connect to the database:\n\n`{e}`")
        st.stop()


def init_db():
    """Ensure table exists; add 'day' if missing; add indexes."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS footfall (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP NOT NULL DEFAULT NOW(),
                type VARCHAR(20) NOT NULL CHECK (type IN ('total','operational')),
                count INTEGER NOT NULL DEFAULT 1
            );
        """)
        cur.execute("""
            ALTER TABLE footfall
            ADD COLUMN IF NOT EXISTS day DATE NOT NULL DEFAULT CURRENT_DATE;
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_footfall_day ON footfall(day);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_footfall_ts_date ON footfall((DATE(ts)));")

def has_day_column():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_name='footfall' AND column_name='day'
            ) AS has_day;
        """)
        return bool(cur.fetchone()["has_day"])

def db_summary_for_day(d):
    """Return (total, operational) for a given day."""
    conn = get_db()
    with conn.cursor() as cur:
        if has_day_column():
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE WHEN type='total' THEN count END),0) AS total,
                  COALESCE(SUM(CASE WHEN type='operational' THEN count END),0) AS operational
                FROM footfall
                WHERE day = %s;
            """, (d,))
        else:
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE WHEN type='total' THEN count END),0) AS total,
                  COALESCE(SUM(CASE WHEN type='operational' THEN count END),0) AS operational
                FROM footfall
                WHERE DATE(ts) = %s;
            """, (d,))
        r = cur.fetchone() or {"total": 0, "operational": 0}
        return (r["total"] or 0, r["operational"] or 0)

def db_undo_last_for_day(d):
    """Remove most recent flushed DB entry for the selected day."""
    conn = get_db()
    with conn.cursor() as cur:
        if has_day_column():
            cur.execute("""
                DELETE FROM footfall
                WHERE id = (
                    SELECT id FROM footfall
                    WHERE day = %s
                    ORDER BY id DESC
                    LIMIT 1
                )
                RETURNING id;
            """, (d,))
        else:
            cur.execute("""
                DELETE FROM footfall
                WHERE id = (
                    SELECT id FROM footfall
                    WHERE DATE(ts) = %s
                    ORDER BY id DESC
                    LIMIT 1
                )
                RETURNING id;
            """, (d,))
        return cur.fetchone() is not None

def db_flush_batch(rows):
    """Bulk insert queued rows: (type, day, count)."""
    if not rows:
        return
    conn = get_db()
    with conn.cursor() as cur:
        execute_values(cur, "INSERT INTO footfall (type, day, count) VALUES %s", rows)

# --------------- App state / DB init ---------------
if "db_initialised" not in st.session_state:
    try:
        init_db()
        st.session_state["db_initialised"] = True
    except Exception as e:
        st.error(f"Failed to initialise database: `{e}`")
        st.stop()

if "selected_day" not in st.session_state:
    st.session_state.selected_day = date.today()

if "queue" not in st.session_state:
    # queue holds tuples: (type, day, count)
    st.session_state.queue = []

if "last_flush" not in st.session_state:
    st.session_state.last_flush = time.time()

def pending_adjustments_for_day(d):
    tot = sum(c for (t, day_val, c) in st.session_state.queue if day_val == d and t == "total")
    op = sum(c for (t, day_val, c) in st.session_state.queue if day_val == d and t == "operational")
    return tot, op

def get_summary(d):
    db_total, db_op = db_summary_for_day(d)
    q_total, q_op = pending_adjustments_for_day(d)
    total = db_total + q_total
    operational = db_op + q_op
    return {
        "total": total,
        "operational": operational,
        "opportunities": max(0, total - operational)
    }

def enqueue(event_type, d, count=1):
    st.session_state.queue.append((event_type, d, count))

def flush_if_needed(force=False):
    now = time.time()
    due = (now - st.session_state.last_flush >= FLUSH_SECONDS) or (len(st.session_state.queue) >= FLUSH_MAX)
    if force or due:
        try:
            db_flush_batch(st.session_state.queue)
            st.session_state.queue.clear()
            st.session_state.last_flush = now
            if force:
                st.toast("Synced to database", icon="✅")
        except Exception:
            st.toast("Database sync failed — will retry automatically.", icon="⚠️")

# auto-refresh UI so timers/summary update (15s)
st_autorefresh(interval=15000, key="tick")

# --------------- Styles (BIG COLOURED CIRCLES) ---------------
st.markdown("""
<style>
div.stButton > button {
  width: 180px !important;
  height: 180px !important;
  border-radius: 9999px !important;
  font-size: 18px !important;
  font-weight: 700 !important;
  line-height: 1.2 !important;
  white-space: normal !important;
  padding: 16px !important;
  border: none !important;
  box-shadow: 0 6px 14px rgba(0,0,0,0.25) !important;
  transition: transform 0.02s ease-in;
  color: #fff !important;
}
div.stButton > button:active { transform: scale(0.97); }

/* hard override colours (works in dark theme) */
[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-of-type(1) div.stButton > button {
  background-color: #16a34a !important; /* green */
}
[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-of-type(2) div.stButton > button {
  background-color: #f59e0b !important; color: #1f2937 !important; /* amber */
}
[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-of-type(3) div.stButton > button {
  background-color: #dc2626 !important; /* red */
}
[data-testid="stHorizontalBlock"] div[data-testid="column"]:nth-of-type(4) div.stButton > button {
  background-color: #2563eb !important; /* blue */
}

/* keep admin buttons normal */
details div.stButton > button {
  width:auto !important; height:auto !important;
  border-radius:8px !important; box-shadow:none !important;
  font-size:14px !important; padding:8px 12px !important;
  background: var(--background-color) !important;
  color: var(--text-color) !important;
}
</style>
""", unsafe_allow_html=True)

# --------------- UI ---------------
st.title("KO Repairs — Footfall")

selected = st.date_input("📅 Select Date", value=st.session_state.selected_day, max_value=date.today())
if selected != st.session_state.selected_day:
    st.session_state.selected_day = selected

col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("👣\nSomeone\nWalks In", key="walkin_btn"):
        enqueue("total", st.session_state.selected_day, 1)
        st.toast("Walk-in queued", icon="👣")

with col2:
    if st.button("🛠️\nDrop-off /\nPick-up /\nPop-in", key="oper_btn"):
        enqueue("operational", st.session_state.selected_day, 1)
        st.toast("Operational queued", icon="🛠️")

with col3:
    if st.button("↩️\nUndo\nLast", key="undo_btn"):
        # Undo from queue first (unflushed), else from DB
        idx = next((i for i in range(len(st.session_state.queue)-1, -1, -1)
                    if st.session_state.queue[i][1] == st.session_state.selected_day), None)
        if idx is not None:
            st.session_state.queue.pop(idx)
            st.toast("Undid unflushed click", icon="↩️")
        else:
            if db_undo_last_for_day(st.session_state.selected_day):
                st.toast("Undid last flushed entry", icon="↩️")
            else:
                st.toast("No entries to remove", icon="ℹ️")

with col4:
    if st.button("🔄\nSync\nNow", key="sync_btn"):
        flush_if_needed(force=True)

st.divider()

s = get_summary(st.session_state.selected_day)
a, b, c = st.columns(3)
a.metric("Total", s["total"])
b.metric("Operational", s["operational"])
c.metric("Opportunities", s["opportunities"])

# background flush if due (non-blocking)
flush_if_needed(force=False)

# --------------- Admin ---------------
with st.expander("🛠️ Admin"):
    colA, colB = st.columns([1, 1])
    with colA:
        st.write(
            "Schema status:",
            "✅ `day` column present" if has_day_column() else "❌ `day` column missing"
        )
    with colB:
        if st.button("Fix DB schema (add `day`)"):
            try:
                init_db()
                st.success("Schema updated. Reload the page.")
            except Exception:
                st.error("Failed to update schema. Check DB_URL/permissions.")

st.caption(
    "Green = everyone entering; Amber = repair visits. "
    "Clicks are queued for speed and synced every 10 min (or when queue is large / Sync Now). "
    "Opportunities = Total − Operational."
)
