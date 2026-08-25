import time
from datetime import date
import os
import uuid
from urllib.parse import urlparse

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import psycopg2
from psycopg2.extras import RealDictCursor

# --------------- Page config ---------------
st.set_page_config(
    page_title="KO Repairs — Footfall",
    page_icon="🏪",
    layout="centered"
)

# --------------- DB config ---------------
DB_URL = None
try:
    if "DB_URL" in st.secrets:
        DB_URL = st.secrets["DB_URL"]
    elif "DATABASE_URL" in st.secrets:
        DB_URL = st.secrets["DATABASE_URL"]
except Exception:
    pass

if not DB_URL:
    DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")

# --------------- DB ---------------
@st.cache_resource(show_spinner=False)
def get_db():
    """Keep a persistent DB connection for speed."""
    if not DB_URL:
        st.error(
            "❌ Database URL is not configured.\n\n"
            "Set `DB_URL` or `DATABASE_URL` in your Streamlit secrets or environment."
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
    """
    Safely extend the existing footfall table.

    Existing rows and the existing total/operational model are preserved.
    New clicks can additionally carry a category and source.
    """
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
        cur.execute("""
            ALTER TABLE footfall
            ADD COLUMN IF NOT EXISTS category VARCHAR(30);
        """)
        cur.execute("""
            ALTER TABLE footfall
            ADD COLUMN IF NOT EXISTS source VARCHAR(30) NOT NULL DEFAULT 'legacy';
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_footfall_day ON footfall(day);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_footfall_ts_date ON footfall((DATE(ts)));")
        cur.execute("""
            ALTER TABLE footfall
            ADD COLUMN IF NOT EXISTS event_group_id UUID;
        """)
        cur.execute("""
            ALTER TABLE footfall
            ADD COLUMN IF NOT EXISTS parent_event_id INTEGER REFERENCES footfall(id);
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_footfall_category ON footfall(category);")


def db_add_interaction(category, d):
    """Add one front-counter interaction; drop/pick companion is linked atomically."""
    operational_categories = {"drop_off", "pick_up"}
    conn = get_db()
    event_group_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        if category in operational_categories:
            cur.execute(
                """
                WITH primary_event AS (
                    INSERT INTO footfall
                        (type, day, count, category, source, event_group_id)
                    VALUES ('total', %s, 1, %s, 'front_counter', %s::uuid)
                    RETURNING id
                )
                INSERT INTO footfall
                    (type, day, count, category, source, event_group_id, parent_event_id)
                SELECT 'operational', %s, 1, NULL, 'front_counter', %s::uuid, id
                FROM primary_event
                """,
                (d, category, event_group_id, d, event_group_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO footfall
                    (type, day, count, category, source, event_group_id)
                VALUES ('total', %s, 1, %s, 'front_counter', %s::uuid)
                """,
                (d, category, event_group_id),
            )


def db_add_staff_interaction(staff_member, d):
    """Record staff coverage only; this never changes footfall."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO staff_interaction_events
                (interaction_date, staff_member, count, source_system, notes)
            VALUES (%s, %s, 1, 'front_counter', NULL)
            """,
            (d, staff_member),
        )


def db_staff_summary_for_day(d):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT staff_member, COALESCE(SUM(count), 0) AS total
            FROM staff_interaction_events
            WHERE interaction_date = %s
              AND staff_member IN ('Jordan', 'Laura')
            GROUP BY staff_member
            """,
            (d,),
        )
        rows = cur.fetchall()
    totals = {"Jordan": 0, "Laura": 0}
    for row in rows:
        totals[row["staff_member"]] = row["total"] or 0
    return totals


def db_undo_last_staff_interaction(staff_member, d):
    """Undo the most recent front-counter staff interaction for one staff member."""
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM staff_interaction_events
            WHERE id = (
                SELECT id
                FROM staff_interaction_events
                WHERE interaction_date = %s
                  AND staff_member = %s
                  AND source_system = 'front_counter'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            )
            RETURNING id
            """,
            (d, staff_member),
        )
        return cur.fetchone() is not None


def db_summary_for_day(d):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN type='total' THEN count ELSE 0 END), 0) AS total,
                COALESCE(SUM(CASE WHEN type='operational' THEN count ELSE 0 END), 0) AS operational,
                COALESCE(SUM(CASE WHEN category='retail_enquiry' THEN count ELSE 0 END), 0) AS retail_enquiry,
                COALESCE(SUM(CASE WHEN category='repair_enquiry' THEN count ELSE 0 END), 0) AS repair_enquiry,
                COALESCE(SUM(CASE WHEN category='trade_in_enquiry' THEN count ELSE 0 END), 0) AS trade_in_enquiry,
                COALESCE(SUM(CASE WHEN category='repair_status_update' THEN count ELSE 0 END), 0) AS repair_status_update,
                COALESCE(SUM(CASE WHEN category='drop_off' THEN count ELSE 0 END), 0) AS drop_off,
                COALESCE(SUM(CASE WHEN category='pick_up' THEN count ELSE 0 END), 0) AS pick_up,
                COALESCE(SUM(CASE WHEN category='general' THEN count ELSE 0 END), 0) AS general
            FROM footfall
            WHERE day = %s;
        """, (d,))
        r = cur.fetchone() or {}

    total = r.get("total", 0) or 0
    operational = r.get("operational", 0) or 0
    drop_off = r.get("drop_off", 0) or 0
    pick_up = r.get("pick_up", 0) or 0
    repair_status_update = r.get("repair_status_update", 0) or 0
    return {
        "total": total,
        "operational": operational,
        "opportunities": max(0, total - drop_off - pick_up - repair_status_update),
        "retail_enquiry": r.get("retail_enquiry", 0) or 0,
        "repair_enquiry": r.get("repair_enquiry", 0) or 0,
        "trade_in_enquiry": r.get("trade_in_enquiry", 0) or 0,
        "repair_status_update": repair_status_update,
        "drop_off": drop_off,
        "pick_up": pick_up,
        "general": r.get("general", 0) or 0,
    }


def db_undo_last_interaction(d):
    """Undo the most recent new-style front-counter interaction for the day."""
    conn = get_db()
    with conn.cursor() as cur:
        # Find the latest categorised total row.
        cur.execute("""
            SELECT id, category, event_group_id
            FROM footfall
            WHERE day = %s
              AND type = 'total'
              AND category IS NOT NULL
              AND source = 'front_counter'
            ORDER BY id DESC
            LIMIT 1;
        """, (d,))
        row = cur.fetchone()
        if not row:
            return False

        total_id = row["id"]
        category = row["category"]
        event_group_id = row.get("event_group_id")

        if event_group_id:
            # Delete linked companion first because parent_event_id references primary.
            cur.execute(
                "DELETE FROM footfall WHERE parent_event_id = %s OR (event_group_id = %s AND type = 'operational');",
                (total_id, event_group_id),
            )
        elif category in {"drop_off", "pick_up"}:
            # Legacy fallback for older unlinked Streamlit pairs only.
            cur.execute("""
                DELETE FROM footfall
                WHERE id = (
                    SELECT id FROM footfall
                    WHERE day = %s AND type = 'operational'
                      AND source = 'front_counter' AND id > %s
                    ORDER BY id ASC LIMIT 1
                );
            """, (d, total_id))

        cur.execute("DELETE FROM footfall WHERE id = %s;", (total_id,))
        return True


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

# Auto-refresh summaries every 15 seconds.
st_autorefresh(interval=15000, key="tick")

# --------------- Styles ---------------
st.markdown("""
<style>
.block-container {
  padding-top: 1.5rem;
  max-width: 1050px;
}

/* Main category buttons */
div[data-testid="stButton"] > button[kind="secondary"] {
  width: 100% !important;
  min-height: 120px !important;
  border-radius: 24px !important;
  font-size: 21px !important;
  font-weight: 750 !important;
  line-height: 1.15 !important;
  white-space: normal !important;
  padding: 14px !important;
  box-shadow: 0 5px 12px rgba(0,0,0,0.18) !important;
}

/* Admin/undo area stays compact */
details div[data-testid="stButton"] > button {
  min-height: 0 !important;
  width: auto !important;
  border-radius: 8px !important;
  font-size: 14px !important;
  padding: 8px 12px !important;
  box-shadow: none !important;
}
</style>
""", unsafe_allow_html=True)

# --------------- UI ---------------
st.title("KO Repairs — Front Counter")

selected = st.date_input(
    "📅 Select Date",
    value=st.session_state.selected_day,
    max_value=date.today()
)
if selected != st.session_state.selected_day:
    st.session_state.selected_day = selected


def category_button(label, category, toast_text):
    if st.button(label, key=f"btn_{category}", use_container_width=True):
        try:
            db_add_interaction(category, st.session_state.selected_day)
            st.toast(toast_text, icon="✅")
            st.rerun()
        except Exception as e:
            st.toast(f"Not saved: {e}", icon="⚠️")


# Two rows of three large buttons — comfortable on an iPad.
r1c1, r1c2, r1c3 = st.columns(3)
with r1c1:
    category_button("🛍️\nRetail Enquiry", "retail_enquiry", "Retail enquiry saved")
with r1c2:
    category_button("🔧\nRepair Enquiry", "repair_enquiry", "Repair enquiry saved")
with r1c3:
    category_button("💷\nTrade-in Enquiry", "trade_in_enquiry", "Trade-in enquiry saved")

r2c1, r2c2, r2c3 = st.columns(3)
with r2c1:
    category_button("📥\nDrop Off", "drop_off", "Drop-off saved")
with r2c2:
    category_button("📤\nPick Up", "pick_up", "Pick-up saved")
with r2c3:
    category_button("💬\nGeneral", "general", "General visit saved")

r3c1, r3c2, r3c3 = st.columns(3)
with r3c1:
    category_button("🔄\nRepair Status Update", "repair_status_update", "Repair status update saved")

st.subheader("Staff interactions")
st.caption("Use these only when Jordan or Laura independently deals with someone. They do not add to footfall.")
staff_counts = db_staff_summary_for_day(st.session_state.selected_day)
jc, lc = st.columns(2)
with jc:
    if st.button(f"👤 Jordan +1  ·  {staff_counts['Jordan']} handled", key="staff_jordan", use_container_width=True):
        try:
            db_add_staff_interaction("Jordan", st.session_state.selected_day)
            st.toast("Jordan interaction saved", icon="✅")
            st.rerun()
        except Exception as e:
            st.toast(f"Not saved: {e}", icon="⚠️")
with lc:
    if st.button(f"👤 Laura +1  ·  {staff_counts['Laura']} handled", key="staff_laura", use_container_width=True):
        try:
            db_add_staff_interaction("Laura", st.session_state.selected_day)
            st.toast("Laura interaction saved", icon="✅")
            st.rerun()
        except Exception as e:
            st.toast(f"Not saved: {e}", icon="⚠️")

st.divider()

s = db_summary_for_day(st.session_state.selected_day)

m1, m2, m3 = st.columns(3)
m1.metric("Total In", s["total"])
m2.metric("Operational", s["operational"])
m3.metric("Opportunities", s["opportunities"])

st.subheader("Today by type")
a, b, c = st.columns(3)
a.metric("Retail Enquiries", s["retail_enquiry"])
b.metric("Repair Enquiries", s["repair_enquiry"])
c.metric("Trade-in Enquiries", s["trade_in_enquiry"])

d, e, f = st.columns(3)
d.metric("Drop Offs", s["drop_off"])
e.metric("Pick Ups", s["pick_up"])
f.metric("General", s["general"])

g, _, _ = st.columns(3)
g.metric("Repair Status Updates", s["repair_status_update"])

with st.expander("🛠️ Admin"):
    st.write("Clicks are written to PostgreSQL immediately — no 10-minute queue.")
    if st.button("↩️ Undo last front-counter entry"):
        if db_undo_last_interaction(st.session_state.selected_day):
            st.success("Last entry removed.")
            st.rerun()
        else:
            st.info("No new-style front-counter entries to remove for this date.")

    st.write("Staff interaction corrections")
    u1, u2 = st.columns(2)
    with u1:
        if st.button("↩️ Undo last Jordan interaction", use_container_width=True):
            if db_undo_last_staff_interaction("Jordan", st.session_state.selected_day):
                st.success("Last Jordan interaction removed.")
                st.rerun()
            else:
                st.info("No Jordan interactions to undo for this date.")
    with u2:
        if st.button("↩️ Undo last Laura interaction", use_container_width=True):
            if db_undo_last_staff_interaction("Laura", st.session_state.selected_day):
                st.success("Last Laura interaction removed.")
                st.rerun()
            else:
                st.info("No Laura interactions to undo for this date.")

    if DB_URL:
        parsed = urlparse(DB_URL)
        st.caption(f"DB host: {parsed.hostname or 'UNKNOWN'}")

st.caption(
    "Every button counts one person through the door. "
    "Drop Off and Pick Up are also counted as operational visits. "
    "Repair Status Updates count as interactions but not opportunities. "
    "Retail, Repair, Trade-in and General count as opportunities."
)
