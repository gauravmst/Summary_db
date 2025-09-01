import streamlit as st
import io
import os
import threading
import pandas as pd
from typing import Dict, Tuple
from sqlalchemy import create_engine, text

ALLOWED_EXT = {".csv", ".xlsx", ".xls"}
MASTER_FILE = "master_summary.xlsx"
DB_FILE = "user.db"
master_lock = threading.Lock()

# -------------------- SQLite Setup --------------------
engine = create_engine(f"sqlite:///{DB_FILE}", echo=False)

def create_users_table():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                UserID TEXT,
                Alias TEXT,
                "MTM (All)" REAL,
                ALLOCATION REAL,
                MAX_LOSS REAL,
                SERVER TEXT,
                ALGO TEXT,
                OPERATOR TEXT,
                EXPIRY TEXT,
                REMARK TEXT
            )
        """))

create_users_table()

def save_to_db(df: pd.DataFrame):
    columns_to_save = ["UserID", "Alias", "MTM (All)", "ALLOCATION", "MAX_LOSS",
                       "SERVER", "ALGO", "OPERATOR", "EXPIRY", "REMARK"]
    df_to_save = df[columns_to_save].copy()
    try:
        df_to_save.to_sql("users", con=engine, if_exists="append", index=False)
        st.success("Data saved to SQLite successfully!")
    except Exception as e:
        st.error(f"Failed to save to SQLite: {e}")

# -------------------- Helpers --------------------
CANONICAL_US = ["User Alias", "User ID", "Max Loss", "Telegram"]

def _norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    return "".join(ch for ch in s.strip().lower() if ch.isalnum())

SYNONYMS_US: Dict[str, str] = {
    _norm("User Alias"): "User Alias",
    _norm("User ID"): "User ID",
    _norm("Max Loss"): "Max Loss",
    _norm("Telegram ID(s)"): "Telegram",
}

DESIRED_ORDER = [
    "SNO","Enabled","UserID","Alias","LoggedIn","SqOff Done","Broker","Qty Multiplier",
    "MTM (All)","ALLOCATION","MAX_LOSS","Available Margin","Total Orders","Total Lots",
    "SERVER","ALGO","REMARK","OPERATOR","EXPIRY"
]

def _read_raw(file_bytes, filename) -> pd.DataFrame:
    name = filename.lower()
    if name.endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes), header=6, dtype=str, keep_default_na=False, low_memory=False)
    return pd.read_excel(io.BytesIO(file_bytes), header=6, dtype=str)

def _select_usersetting_columns(df: pd.DataFrame) -> pd.DataFrame:
    norm_to_orig = {_norm(c): c for c in df.columns}
    resolved = {}
    for canonical in CANONICAL_US:
        candidates = [k for k, v in SYNONYMS_US.items() if v == canonical]
        found = None
        for cand in candidates:
            if cand in norm_to_orig:
                found = norm_to_orig[cand]
                break
        if not found:
            raise ValueError(f"Usersetting missing column: {canonical}")
        resolved[canonical] = found
    cleaned = df[[resolved[c] for c in CANONICAL_US]].copy()
    cleaned.columns = CANONICAL_US
    return cleaned

def _build_lookup(clean_us: pd.DataFrame) -> Dict[str, Tuple[str, str]]:
    return {_norm(row["User ID"]): (row["Telegram"], row["Max Loss"])
            for _, row in clean_us.iterrows() if row.get("User ID")}

def _insert_allocation_maxloss(df: pd.DataFrame, lookup: Dict[str, Tuple[str, str]], user_id_colname: str) -> pd.DataFrame:
    out = df.copy()
    insert_at = min(9, len(out.columns))
    def fetch(uid):
        tel, mls = lookup.get(_norm(uid), ("", ""))
        return pd.Series({"ALLOCATION": tel, "MAX_LOSS": mls})
    new_cols = out[user_id_colname].apply(fetch)
    out.insert(insert_at, "ALLOCATION", new_cols["ALLOCATION"])
    out.insert(insert_at + 1, "MAX_LOSS", new_cols["MAX_LOSS"])
    return out

def _append_constants(df: pd.DataFrame, consts: Dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    for k in ["SERVER","ALGO","OPERATOR","EXPIRY"]:
        out[k] = consts.get(k, "")
    if "REMARK" not in out.columns:
        out["REMARK"] = ""
    out["REMARK"] = pd.Series(out["REMARK"].astype(str).fillna('')) + consts.get("REMARK", "")
    return out

def _read_all_sheets(file_bytes, filename) -> Dict[str, pd.DataFrame]:
    name = filename.lower()
    if name.endswith(".csv"):
        return {"Sheet1": pd.read_csv(io.BytesIO(file_bytes), low_memory=False)}
    xl = pd.ExcelFile(io.BytesIO(file_bytes))
    return {sheet: xl.parse(sheet_name=sheet) for sheet in xl.sheet_names}

def _server_from_filename(name: str) -> str:
    base = os.path.splitext(name or "")[0].strip()
    token = base.replace("_"," ").replace("-"," ").split()
    return token[0] if token else ""

def _coerce_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_convert = [
        "ALLOCATION", "MAX_LOSS", "ALGO", "Total Orders", "Total Lots",
        "Available Margin", "MTM (All)", "Qty Multiplier"
    ]
    out_df = df.copy()
    for col in cols_to_convert:
        if col in out_df.columns:
            out_df[col] = pd.to_numeric(out_df[col], errors='coerce')
    return out_df

def apply_remarks(df: pd.DataFrame) -> pd.DataFrame:
    remark_col = "REMARK"
    def remark_logic(row):
        try:
            max_loss_allocation = pd.to_numeric(row['MAX_LOSS'], errors='coerce') / pd.to_numeric(row['ALLOCATION'], errors='coerce') + 0.1
            mtm_allocation = -(pd.to_numeric(row['MTM (All)'], errors='coerce') / pd.to_numeric(row['ALLOCATION'], errors='coerce'))
            if pd.notna(max_loss_allocation) and pd.notna(mtm_allocation) and max_loss_allocation <= mtm_allocation:
                existing = str(row.get(remark_col, '')).strip()
                return (existing + (" " if existing else "") + "Slippage")
            return row.get(remark_col, '')
        except (ValueError, TypeError, ZeroDivisionError):
            return row.get(remark_col, '')
    df[remark_col] = df.apply(remark_logic, axis=1)
    return df

# -------------------- Streamlit App --------------------
st.set_page_config(page_title="Summary Enricher", layout="wide")
if 'stage' not in st.session_state:
    st.session_state.stage = 'upload'
    st.session_state.show_bulk = False

# ---- Upload Stage ----
if st.session_state.stage == 'upload':
    st.title("Summary Enricher")
    usersetting = st.file_uploader("Usersetting file (.csv / .xlsx)", type=["csv", "xlsx", "xls"])
    summary = st.file_uploader("Summary file (.xlsx recommended; multi-sheet supported)", type=["csv", "xlsx", "xls"])

    col1, col2 = st.columns(2)
    with col1:
        algo = st.selectbox("ALGO", options=["", "1", "2", "5", "7", "8", "12", "15", "102"])
    with col2:
        operator = st.selectbox("OPERATOR", options=["", "GAURAVK","CHETANB","SAHILM","BANSHIP","VIKASA","GULSHANS","PRADYUMANS","ASHUTOSHM","JITESHS"])

    col3, col4 = st.columns(2)
    with col3:
        expiry = st.selectbox("EXPIRY", options=["", "NF 0DTE", "NF 1DTE", "SX 0DTE", "SX 1DTE", "BNF 1DTE", "BNF 0DTE"])
    with col4:
        remark = st.text_input("REMARK (optional)", placeholder="Fill same remark for all users")

    if st.button("Run"):
        if not usersetting or not summary:
            st.error("Please upload both files.")
        else:
            try:
                consts = {"ALGO": algo,"OPERATOR": operator,"EXPIRY": expiry,"REMARK": remark}
                consts["SERVER"] = _server_from_filename(usersetting.name) or _server_from_filename(summary.name)

                raw_us = _read_raw(usersetting.read(), usersetting.name)
                us_clean = _select_usersetting_columns(raw_us)

                sm_sheets = _read_all_sheets(summary.read(), summary.name)
                first_name = list(sm_sheets.keys())[0]
                first_df = sm_sheets[first_name]

                lookup = _build_lookup(us_clean)
                uid_col = "UserID" if "UserID" in first_df.columns else ("User ID" if "User ID" in first_df.columns else None)

                enriched_first = first_df.copy()
                if uid_col:
                    enriched_first = _insert_allocation_maxloss(enriched_first, lookup, uid_col)
                else:
                    insert_at = min(9, len(enriched_first.columns))
                    enriched_first.insert(insert_at, "ALLOCATION", "")
                    enriched_first.insert(insert_at + 1, "MAX_LOSS", "")

                enriched_first = _append_constants(enriched_first, consts)
                enriched_first = apply_remarks(enriched_first)
                enriched_first = _coerce_numeric_columns(enriched_first)

                st.session_state.enriched_first = enriched_first
                st.session_state.sheets = sm_sheets
                st.session_state.first_sheet_name = first_name
                st.session_state.original_summary_filename = summary.name
                st.session_state.consts = consts
                st.session_state.uid_col = uid_col
                st.session_state.stage = 'preview'
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

# ---- Preview & Edit Stage ----
elif st.session_state.stage == 'preview':
    st.title("Preview & Edit")
    enriched_first = st.session_state.enriched_first.copy()

    column_config = {}
    for col in enriched_first.columns:
        if pd.api.types.is_bool_dtype(enriched_first[col]):
            column_config[col] = st.column_config.CheckboxColumn(col)
        elif pd.api.types.is_numeric_dtype(enriched_first[col]):
            column_config[col] = st.column_config.NumberColumn(col)
        else:
            column_config[col] = st.column_config.TextColumn(col)

    if 'edited_df' not in st.session_state:
        st.session_state.edited_df = enriched_first.copy()

    edited_df = st.data_editor(
        st.session_state.edited_df,
        column_config=column_config,
        hide_index=False,
        use_container_width=True
    )
    st.session_state.edited_df = edited_df

    # --- Bulk Remark Section ---
    if st.session_state.show_bulk:
        bulk_remark = st.text_area("Remark to Apply")
        if "bulk_select" not in st.session_state:
            st.session_state.bulk_select = pd.Series([False]*len(st.session_state.edited_df), index=st.session_state.edited_df.index)

        bulk_table = st.session_state.edited_df.copy()
        bulk_table["Select"] = st.session_state.bulk_select
        bulk_table = st.data_editor(
            bulk_table,
            column_config={"Select": st.column_config.CheckboxColumn("Select")},
            hide_index=False,
            use_container_width=True
        )
        st.session_state.bulk_select = bulk_table["Select"]

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("Apply Bulk Remark"):
                selected_users = bulk_table[bulk_table["Select"]==True][st.session_state.uid_col].tolist()
                mask = st.session_state.edited_df[st.session_state.uid_col].isin(selected_users)
                st.session_state.edited_df.loc[mask, "REMARK"] = bulk_remark
                st.session_state.show_bulk = False
                st.rerun()
        with col2:
            if st.button("Select All"):
                st.session_state.bulk_select[:] = True
                st.rerun()
        with col3:
            if st.button("Clear All"):
                st.session_state.bulk_select[:] = False
                st.rerun()
        with col4:
            if st.button("Cancel Bulk"):
                st.session_state.show_bulk = False
                st.rerun()

    if st.button("Bulk Remark"):
        st.session_state.show_bulk = not st.session_state.show_bulk
        st.rerun()

    if st.button("Save & Go to Final"):
        st.session_state.enriched_first = st.session_state.edited_df.copy()
        st.session_state.stage = 'final'
        st.rerun()

# ---- Final Stage ----
elif st.session_state.stage == 'final':
    st.title("Final Stage")
    enriched_first = st.session_state.enriched_first
    enriched_first = _coerce_numeric_columns(enriched_first)

    # Download Excel
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        enriched_first.to_excel(xw, index=False, sheet_name=st.session_state.first_sheet_name[:31])
        for name, df in st.session_state.sheets.items():
            if name != st.session_state.first_sheet_name:
                df.to_excel(xw, index=False, sheet_name=name[:31])
    out.seek(0)
    original_filename = st.session_state.original_summary_filename
    if not original_filename.lower().endswith('.xlsx'):
        original_filename += '.xlsx'
    st.download_button("Download Enriched Summary", data=out, file_name=original_filename)

    # Save to SQLite
    if st.button("Save to DB"):
        save_to_db(enriched_first)

    # Append to master
    with master_lock:
        try:
            if os.path.exists(MASTER_FILE):
                master_df = pd.read_excel(MASTER_FILE)
                master_df = pd.concat([master_df, enriched_first], ignore_index=True)
            else:
                master_df = enriched_first
            master_df.to_excel(MASTER_FILE, index=False)
        except Exception as e:
            st.warning(f"Could not append to master file: {e}")

    if st.button("Start Over"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()
