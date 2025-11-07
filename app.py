import streamlit as st
import psycopg2
from datetime import date

st.set_page_config(page_title="KO Repairs Footfall Tracker", layout="centered")

# Use your Railway secret variable
DB_URL = st.secrets["DB_URL"]
# ---------- Database helpers ----------
def get_conn():
    return psycopg2.connect(DB_URL)

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS footfall (
                    id SERIAL PRIMARY KEY,
                    ts TIMESTAMP NOT NULL DEFAULT NOW(),
                    type VARCHAR(20) NOT NULL CHECK (type IN ('total','operational')),
                    count INTEGER NOT NULL DEFAULT 1
                );
            """)
            conn.commit()

def log_event(event_type, count=1):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO footfall (type, count) VALUES (%s, %s)", (event_type, count))
            conn.commit()

def undo_last():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM footfall
                WHERE id = (SELECT id FROM footfall ORDER BY id DESC LIMIT 1)
                RETURNING id;
            """)
            deleted = cur.fetchone()
            conn.commit()
            return deleted is not None

def get_summary_for(selected_date: date):
    """Return summary totals for a specific date."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  COALESCE(SUM(CASE WHEN type='total' THEN count END),0) AS total,
                  COALESCE(SUM(CASE WHEN type='operational' THEN count END),0) AS operational
                FROM footfall
                WHERE DATE(ts) = %s;
            """, (selected_date,))
            total, operational = cur.fetchone()
            return {
                "total": total,
                "operational": operational,
                "opportunities": max(0, total - operational)
            }

# ---------- UI ----------
st.title("🏪 KO Repairs — Footfall Tracker")

init_db()

# Select date
st.subheader("📅 Select a Date to View")
selected_date = st.date_input("Date", date.today(), max_value=date.today())
st.divider()

# Action buttons
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("👣 Someone Walks In", use_container_width=True):
        log_event("total")
        st.toast("Logged: Walk-in", icon="👣")

with col2:
    if st.button("🛠️ Drop-off / Pick-up / Pop-in", use_container_width=True):
        log_event("operational")
        st.toast("Logged: Operational Visit", icon="🛠️")

with col3:
    if st.button("↩️ Undo Last Click", use_container_width=True):
        if undo_last():
            st.toast("Last click removed", icon="↩️")
        else:
            st.toast("No entries to remove", icon="⚠️")

# Summary for chosen date
st.divider()
summary = get_summary_for(selected_date)
st.subheader(f"📊 Summary for {selected_date.strftime('%d %b %Y')}")

colA, colB, colC = st.columns(3)
colA.metric("Total", summary["total"])
colB.metric("Operational", summary["operational"])
colC.metric("Opportunities", summary["opportunities"])

st.caption("Tip: Press for everyone on entry; press Operational for repairs. Opportunities = Total − Operational.")
