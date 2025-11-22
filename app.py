import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO

# ---------------- CONFIG ----------------
TEMPLATE_PATH = "sku-template (4).xlsx"
MAPPING_PATH = "Mapping - Automation.xlsx"

# internal keys
ATTR_KEY = "attributes"
TARGET_KEY = "fieldname"
MAND_KEY = "mandatoryornot"
TYPE_KEY = "fieldtype"
DUP_KEY = "duplicatestobecreated"

# helpers
def norm(s) -> str:
    if pd.isna(s):
        return ""
    return "".join(str(s).split()).lower()

def clean_header(header) -> str:
    if pd.isna(header):
        return ""
    header_str = str(header).replace(".", " ")
    header_str = re.sub(r"[^A-Za-z0-9\s]", "", header_str)
    header_str = re.sub(r"\s+", " ", header_str).strip()
    return header_str

IMAGE_EXT_RE = re.compile(r"(?i)\.(jpe?g|png|gif|bmp|webp|tiff?)$")
IMAGE_KEYWORDS = {"image","img","picture","photo","thumbnail","thumb","hero","front","back","url"}

def is_image_column(col_header_norm: str, series: pd.Series) -> bool:
    header_hit = any(k in col_header_norm for k in IMAGE_KEYWORDS)
    sample = series.dropna().astype(str).head(20)
    ratio = sample.str.contains(IMAGE_EXT_RE).mean() if not sample.empty else 0.0
    return header_hit or ratio >= 0.30

@st.cache_data
def load_mapping():
    try:
        xl = pd.ExcelFile(MAPPING_PATH)
    except Exception:
        return pd.DataFrame(), []
    map_sheet = next((s for s in xl.sheet_names if "mapping" in norm(s)), xl.sheet_names[0])
    mapping_df = xl.parse(map_sheet)
    mapping_df.rename(columns={c: norm(c) for c in mapping_df.columns}, inplace=True)
    mapping_df["__attr_key"] = mapping_df.get(ATTR_KEY, pd.Series()).apply(norm) if ATTR_KEY in mapping_df.columns else ""
    return mapping_df, []

def process_file(input_file,
                 marketplace: str,
                 mapping_df: pd.DataFrame | None = None,
                 header_row_override: int | None = None,
                 data_row_override: int | None = None):
    """
    Processes the input Excel file and writes mapped/auto-mapped columns.
    THIS PATCHED VERSION WILL NOT APPEND variantId, productId OR SKU Code under any condition.
    """

    marketplace_configs = {
        "Amazon": {"sheet": "Template", "header_row": 2, "data_row": 4, "sheet_index": None},
        "Flipkart": {"sheet": None, "header_row": 1, "data_row": 5, "sheet_index": 2},
        "Myntra": {"sheet": None, "header_row": 3, "data_row": 4, "sheet_index": 1},
        "Ajio": {"sheet": None, "header_row": 2, "data_row": 3, "sheet_index": 2},
        "TataCliq": {"sheet": None, "header_row": 4, "data_row": 6, "sheet_index": 0},
        "General": {"sheet": None, "header_row": 1, "data_row": 2, "sheet_index": 0},
        "Zivame": {"sheet": None, "header_row": 1, "data_row": 2, "sheet_index": 0},
        "Celio": {"sheet": None, "header_row": 1, "data_row": 2, "sheet_index": 0},
    }

    cfg = marketplace_configs.get(marketplace, marketplace_configs["General"])
    header_row = header_row_override if header_row_override is not None else cfg["header_row"]
    data_row = data_row_override if data_row_override is not None else cfg["data_row"]

    # read input
    try:
        if marketplace == "Flipkart":
            xl = pd.ExcelFile(input_file)
            temp = xl.parse(xl.sheet_names[cfg["sheet_index"]], header=None)
            headers = temp.iloc[header_row-1].tolist()
            src_df = temp.iloc[data_row-1:].copy()
            src_df.columns = headers
        elif cfg["sheet"] is not None:
            src_df = pd.read_excel(input_file, sheet_name=cfg["sheet"],
                                   header=header_row-1,
                                   skiprows=data_row-header_row-1,
                                   dtype=str, engine="openpyxl")
        else:
            xl = pd.ExcelFile(input_file)
            src_df = xl.parse(xl.sheet_names[cfg["sheet_index"]],
                              header=header_row-1,
                              skiprows=data_row-header_row-1)
    except Exception as e:
        st.error(f"Error reading file for {marketplace}: {e}")
        return None

    src_df.dropna(axis=1, how="all", inplace=True)

    # Build columns_meta using mapping where available
    columns_meta = []
    mapping_present = mapping_df is not None and "__attr_key" in mapping_df.columns
    for col in src_df.columns:
        col_key = norm(col)
        if mapping_present:
            matches = mapping_df[mapping_df["__attr_key"] == col_key]
        else:
            matches = pd.DataFrame()

        if not matches.empty:
            row3 = matches.iloc[0].get(MAND_KEY, "mandatory")
            row4 = matches.iloc[0].get(TYPE_KEY, "string")
        else:
            row3 = "mandatory"
            row4 = "imageurlarray" if is_image_column(col_key, src_df[col]) else "string"

        columns_meta.append({"src": col, "out": col, "row3": row3, "row4": row4})

        if not matches.empty:
            for _, row in matches.iterrows():
                if str(row.get(DUP_KEY, "")).lower().startswith("yes"):
                    new_header = row.get(TARGET_KEY, col) if pd.notna(row.get(TARGET_KEY, None)) else col
                    if new_header != col:
                        columns_meta.append({"src": col, "out": new_header,
                                             "row3": row.get(MAND_KEY, "mandatory"),
                                             "row4": row.get(TYPE_KEY, "string")})

    # Detect options
    color_cols = [c for c in src_df.columns if "color" in norm(c) or "colour" in norm(c)]
    size_cols  = [c for c in src_df.columns if "size" in norm(c)]

    option1_data = pd.Series([""] * len(src_df), dtype=str)
    option2_data = pd.Series([""] * len(src_df), dtype=str)

    if size_cols:
        option1_data = src_df[size_cols[0]].fillna('').astype(str).str.strip()
        if color_cols and color_cols[0] != size_cols[0]:
            option2_data = src_df[color_cols[0]].fillna('').astype(str).str.strip()
    elif color_cols:
        option2_data = src_df[color_cols[0]].fillna('').astype(str).str.strip()

    # Build workbook
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

    # Write main columns
    for j, meta in enumerate(columns_meta, start=1):
        header_display = clean_header(meta["out"])
        ws_vals.cell(row=1, column=j, value=header_display)
        for i, v in enumerate(src_df[meta["src"]].tolist(), start=2):
            if pd.isna(v) or v == "":
                ws_vals.cell(row=i, column=j, value=None)
            else:
                cell = ws_vals.cell(row=i, column=j, value=str(v))
                cell.number_format = "@"
        tcol = j + 2
        ws_types.cell(row=1, column=tcol, value=header_display)
        ws_types.cell(row=2, column=tcol, value=header_display)
        ws_types.cell(row=3, column=tcol, value=meta["row3"])
        ws_types.cell(row=4, column=tcol, value=meta["row4"])

    # Append Option 1 & Option 2 to Values
    opt1_col = len(columns_meta) + 1
    opt2_col = len(columns_meta) + 2
    ws_vals.cell(row=1, column=opt1_col, value="Option 1")
    ws_vals.cell(row=1, column=opt2_col, value="Option 2")
    for i, v in enumerate(option1_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt1_col, value=v if v else None)
    for i, v in enumerate(option2_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt2_col, value=v if v else None)

    # Append Option 1 & Option 2 to Types
    t1_col = opt1_col + 2
    t2_col = opt2_col + 2
    ws_types.cell(row=1, column=t1_col, value="Option 1")
    ws_types.cell(row=2, column=t1_col, value="Option 1")
    ws_types.cell(row=3, column=t1_col, value="non mandatory")
    ws_types.cell(row=4, column=t1_col, value="select")
    ws_types.cell(row=1, column=t2_col, value="Option 2")
    ws_types.cell(row=2, column=t2_col, value="Option 2")
    ws_types.cell(row=3, column=t2_col, value="non mandatory")
    ws_types.cell(row=4, column=t2_col, value="select")

    unique_opt1 = option1_data.dropna().unique().tolist()
    unique_opt2 = option2_data.dropna().unique().tolist()
    for i, v in enumerate(unique_opt1, start=5):
        ws_types.cell(row=i, column=t1_col, value=v)
    for i, v in enumerate(unique_opt2, start=5):
        ws_types.cell(row=i, column=t2_col, value=v)

    # IMPORTANT: This patched version DOES NOT append variantId/productId/SKU Code for ANY marketplace,
    # including General. No auto-generation will occur.

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="SKU Template Automation", layout="wide")
st.title("📊 SKU Template Automation Tool")

mapping_df, _ = load_mapping()  # mapping workbook loaded but not shown in UI

marketplace_options = ["General", "Amazon", "Flipkart", "Myntra", "Ajio", "TataCliq", "Zivame", "Celio"]
marketplace_type = st.selectbox("Select Template Type", marketplace_options)

marketplace_defaults = {
    "Amazon": (2, 4),
    "Flipkart": (1, 5),
    "Myntra": (3, 4),
    "Ajio": (2, 3),
    "TataCliq": (4, 6),
    "General": (1, 2),
    "Celio": (1, 2),
    "Zivame": (1, 2),
}

if marketplace_type == "General":
    st.markdown("### Header & Data rows\nSpecify which line contains the header and which line data starts (1-indexed).")
    col1, col2 = st.columns(2)
    with col1:
        header_row = st.number_input("Header row (line number containing column headers)", min_value=1, value=marketplace_defaults["General"][0])
    with col2:
        data_row = st.number_input("Data start row (first line of actual data)", min_value=1, value=marketplace_defaults["General"][1])
else:
    header_row, data_row = marketplace_defaults.get(marketplace_type, (1,2))

input_file = st.file_uploader("Upload Input Excel File", type=["xlsx", "xls", "xlsm"])

if input_file:
    with st.spinner("Processing…"):
        result = process_file(
            input_file,
            marketplace_type,
            mapping_df=mapping_df if not mapping_df.empty else None,
            header_row_override=int(header_row),
            data_row_override=int(data_row),
        )
    if result:
        st.success("✅ Output Generated!")
        st.download_button("📥 Download Output", data=result, file_name="output_template.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.markdown("---")
st.caption("Built for Rubick.ai | By Vishnu Sai")
