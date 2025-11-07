import time
from datetime import date

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

# ---------------- Config ----------------
st.set_page_config(page_title="KO Repairs Footfall", layout="centered")

DB_URL = st.secrets["DB_URL"]  # e.g. postgresql://.../railway?sslmode=require

# Batching settings
FLUSH_SECONDS = 600        # 10 minutes
FLUSH_MAX = 50             # flush sooner if queue reaches this size

# ---------------- DB (pooled) ----------------
@st.cache_resource(show_spinner=False)
def get_db():
    conn = psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)
    conn.autocommit = True
    return conn

def init_db():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS footfall (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMP NOT NULL DEFAULT NOW(),
                day DATE NOT NULL DEFAULT CURRENT_DATE,
                type VARCHAR(20) NOT NULL CHECK (type IN ('total','operational')),
                count INTEGER NOT NULL DEFAULT 1
            );
        """)

def db_summary_for_day(d: date):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
              COALESCE(SUM(CASE WHEN type='total' THEN count END),0) AS total,
              COALESCE(SUM(CASE WHEN type='operational' THEN count END),0) AS operational
            FROM footfall
            WHERE day = %s;
        """, (d,))
        r = cur.fetchone()
        return (r["total"] or 0, r["operational"] or 0)

def db_undo_last_for_day(d: date) -> bool:
    """Remove most recent DB entry for the selected day (only affects flushed items)."""
    conn = get_db()
    with conn.cursor() as cur:
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
        return cur.fetchone() is not None

def db_flush_batch(rows: list[tuple[str, date, int]]):
    """Bulk insert queued rows: (type, day, count)."""
    if not rows:
        return
    conn = get_db()
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO footfall (type, day, count) VALUES %s",
            rows
        )

# ---------------- State ----------------
init_db()

if "selected_day" not in st.session_state:
    st.session_state.selected_day = date.today()

if "queue" not in st.session_state:
    # queue holds tuples: (type, day, count)
    st.session_state.queue = []

if "last_flush" not in st.session_state:
    st.session_state.last_flush = time.time()

def pending_adjustments_for_day(d: date):
    """Sum queued (unflushed) events for day d."""
    tot = sum(c for (t, day, c) in st.session_state.queue if day == d and t == "total")
    op  = sum(c for (t, day, c) in st.session_state.queue if day == d and t == "operational")
    return tot, op

def get_summary(d: date):
    db_total, db_operational = db_summary_for_day(d)
    q_total, q_operational = pending_adjustments_for_day(d)
    total = db_total + q_total
    operational = db_operational + q_operational
    return {
        "total": total,
        "operational": operational,
        "opportunities": max(0, total - operational)
    }

def enqueue(event_type: str, d: date, count: int = 1):
    st.session_state.queue.append((event_type, d, count))

def flush_if_needed(force: bool = False):
    now = time.time()
    if force or (now - st.session_state.last_flush >= FLUSH_SECONDS) or (len(st.session_state.queue) >= FLUSH_MAX):
        try:
            db_flush_batch(st.session_state.queue)
            st.session_state.queue.clear()
            st.session_state.last_flush = now
            if force:
                st.toast("Synced to database", icon="✅")
        except Exception:
            # keep queue intact if flush fails
            st.toast("Database sync failed — will retry automatically.", icon="⚠️")

# small auto-rerun to help timers/flushes tick even without interaction
st.experimental_rerun  # keep reference for linters
st_autorefresh = st.experimental_rerun  # no-op alias (compat)
st.autorefresh(interval=15000, key="tick")  # 15s; flush still respects FLUSH_SECONDS

# ---------------- Styles (big circles) ----------------
st.markdown("""
<style>
div.circle-btn > button {
  width: 180px !important;
  height: 180px !important;
  border-radius: 9999px !important;
  font-size: 18px !important;
  font-weight: 700 !important;
  line-height: 1.2 !important;
  white-space: normal !important;
  padding: 16px !important;
  border: 0 !important;
  box-shadow: 0 6px 14px rgba(0,0,0,0.15);
  transition: transform 0.02s ease-in;
}
div.circle-btn > button:active { transform: scale(0.98); }
div.walkin > button { background: #22c55e; color: white; }        /* green */
div.operational > button { background: #f59e0b; color: #1f2937; } /* amber */
div.undo > button { background: #ef4444; color: white; }          /* red */
div.sync > button { background: #3b82f6; color: white; }          /* blue */
</style>
""", unsafe_allow_html=True)

# ---------------- UI ----------------
st.title("🏪 KO Repairs — Footfall")

selected = st.date_input("📅 Select Date", value=st.session_state.selected_day, max_value=date.today())
if selected != st.session_state.selected_day:
    st.session_state.selected_day = selected

# Buttons row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="circle-btn walkin">', unsafe_allow_html=True)
    if st.button("👣\nSomeone\nWalks In", key="walkin_btn"):
        # optimistic: update local summary via queue
        enqueue("total", st.session_state.selected_day, 1)
        st.toast("Walk-in queued", icon="👣")
    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    st.markdown('<div class="circle-btn operational">', unsafe_allow_html=True)
    if st.button("🛠️\nDrop-off /\nPick-up /\nPop-in", key="oper_btn"):
        enqueue("operational", st.session_state.selected_day, 1)
        st.toast("Operational queued", icon="🛠️")
    st.markdown("</div>", unsafe_allow_html=True)

with col3:
    st.markdown('<div class="circle-btn undo">', unsafe_allow_html=True)
    if st.button("↩️\nUndo\nLast", key="undo_btn"):
        # Prefer to undo from queue (unflushed) if any; else undo from DB
        # Find last queued item for this day
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
    st.markdown("</div>", unsafe_allow_html=True)

with col4:
    st.markdown('<div class="circle-btn sync">', unsafe_allow_html=True)
    if st.button("🔄\nSync\nNow", key="sync_btn"):
        flush_if_needed(force=True)
    st.markdown("</div>", unsafe_allow_html=True)

st.divider()

# Show summary (DB + queued)
s = get_summary(st.session_state.selected_day)
a, b, c = st.columns(3)
a.metric("Total", s["total"])
b.metric("Operational", s["operational"])
c.metric("Opportunities", s["opportunities"])

# Attempt timed/threshold flush (non-blocking)
flush_if_needed(force=False)

st.caption(
    "Green for everyone entering; amber for repair visits. "
    "Clicks are queued for speed and synced every 10 min (or when queue is large). "
    "Opportunities = Total − Operational."
)
