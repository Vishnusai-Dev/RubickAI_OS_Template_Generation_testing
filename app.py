import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO

# ---------------- CONFIG / SAMPLE PATH ----------------
TEMPLATE_PATH = "sku-template (4).xlsx"
MAPPING_PATH = "Mapping - Automation.xlsx"

# (optional) sample uploaded file from this session for quick local testing:
SAMPLE_INPUT_PATH = "/mnt/data/raymond input.xlsx"

# internal mapping workbook keys
ATTR_KEY = "attributes"
TARGET_KEY = "fieldname"
MAND_KEY = "mandatoryornot"
TYPE_KEY = "fieldtype"
DUP_KEY = "duplicatestobecreated"

# ---------------- HELPERS ----------------
def norm(s) -> str:
    if pd.isna(s):
        return ""
    return "".join(str(s).split()).lower()

def clean_header(header) -> str:
    if pd.isna(header):
        return ""
    header_str = str(header)
    header_str = header_str.replace(".", " ")
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

# ---------------- MAPPING LOADER ----------------
@st.cache_data
def load_mapping():
    try:
        xl = pd.ExcelFile(MAPPING_PATH)
    except Exception:
        # mapping workbook missing or unreadable
        return pd.DataFrame(), []
    # try find mapping sheet
    map_sheet = next((s for s in xl.sheet_names if "mapping" in norm(s)), xl.sheet_names[0])
    mapping_df = xl.parse(map_sheet)
    mapping_df.rename(columns={c: norm(c) for c in mapping_df.columns}, inplace=True)
    if ATTR_KEY in mapping_df.columns:
        mapping_df["__attr_key"] = mapping_df[ATTR_KEY].apply(norm)
    else:
        mapping_df["__attr_key"] = ""
    # attempt to read optional client sheet but do NOT display in UI
    client_names = []
    client_sheet = next((s for s in xl.sheet_names if "mappedclientname" in norm(s)), None)
    if client_sheet:
        raw = xl.parse(client_sheet, header=None)
        client_names = [str(x).strip() for x in raw.values.flatten() if pd.notna(x) and str(x).strip()]
    return mapping_df, client_names

# ---------------- PROCESSOR ----------------
def process_file(input_file,
                 marketplace: str,
                 mapping_df: pd.DataFrame | None = None,
                 header_row_override: int | None = None,
                 data_row_override: int | None = None,
                 general_style_col: str | None = None,
                 general_seller_sku_col: str | None = None):
    """
    - Uses mapping_df automatically when available per-column; falls back to auto-detect.
    - NEVER auto-creates variantId or SKU Code for non-General templates.
    - For General: writes variantId/productId only when user supplies exact source column names and they exist.
    """

    # marketplace defaults (sheet/header/data row conventions)
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

    # read input into DataFrame
    try:
        if marketplace == "Flipkart":
            xl = pd.ExcelFile(input_file)
            temp_df = xl.parse(xl.sheet_names[cfg["sheet_index"]], header=None)
            header_idx = header_row - 1
            data_idx = data_row - 1
            headers = temp_df.iloc[header_idx].tolist()
            src_df = temp_df.iloc[data_idx:].copy()
            src_df.columns = headers
        elif cfg["sheet"] is not None:
            # specific sheet (e.g., Amazon 'Template')
            src_df = pd.read_excel(input_file,
                                   sheet_name=cfg["sheet"],
                                   header=header_row-1,
                                   skiprows=data_row-header_row-1,
                                   dtype=str,
                                   engine="openpyxl")
        else:
            xl = pd.ExcelFile(input_file)
            src_df = xl.parse(xl.sheet_names[cfg["sheet_index"]],
                              header=header_row-1,
                              skiprows=data_row-header_row-1)
    except Exception as e:
        st.error(f"Error reading file for {marketplace} template: {e}")
        return None

    # drop completely empty columns
    src_df.dropna(axis=1, how="all", inplace=True)

    # build columns_meta using mapping_df per-column (if mapping present)
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

        # duplicate target expansion (if mapping workbook asks to duplicate)
        if not matches.empty:
            for _, row in matches.iterrows():
                if str(row.get(DUP_KEY, "")).lower().startswith("yes"):
                    new_header = row.get(TARGET_KEY, col) if pd.notna(row.get(TARGET_KEY, None)) else col
                    if new_header != col:
                        columns_meta.append({"src": col, "out": new_header,
                                             "row3": row.get(MAND_KEY, "mandatory"),
                                             "row4": row.get(TYPE_KEY, "string")})

    # detect option columns
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

    # build workbook from template
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

    # write mapped/auto-mapped columns
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

    # append Option 1 & Option 2 columns to Values
    opt1_col = len(columns_meta) + 1
    opt2_col = len(columns_meta) + 2
    ws_vals.cell(row=1, column=opt1_col, value="Option 1")
    ws_vals.cell(row=1, column=opt2_col, value="Option 2")
    for i, v in enumerate(option1_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt1_col, value=v if v else None)
    for i, v in enumerate(option2_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt2_col, value=v if v else None)

    # append Option columns to Types
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

    # ---------------- GENERAL: write variantId / productId ONLY if user provided exact source columns ----------------
    def match_exact(name, cols):
        if not name:
            return None
        target_norm = norm(name)
        return next((c for c in cols if norm(c) == target_norm), None)

    if marketplace.strip() == "General":
        variant_src_col = match_exact(general_style_col, src_df.columns) if general_style_col else None
        product_src_col = match_exact(general_seller_sku_col, src_df.columns) if general_seller_sku_col else None

        # write columns only if user supplied AND the column exists (exact match)
        if variant_src_col:
            start_col = ws_vals.max_column + 1
            vcol = start_col
            ws_vals.cell(row=1, column=vcol, value="variantId")
            for i, v in enumerate(src_df[variant_src_col].fillna("").astype(str).tolist(), start=2):
                val = v if v and str(v).strip() else None
                ws_vals.cell(row=i, column=vcol, value=val)
                if val is not None:
                    ws_vals.cell(row=i, column=vcol).number_format = "@"
            # Types
            tv = vcol + 2
            ws_types.cell(row=1, column=tv, value="variantId")
            ws_types.cell(row=2, column=tv, value="variantId")
            ws_types.cell(row=3, column=tv, value="mandatory")
            ws_types.cell(row=4, column=tv, value="string")

        if product_src_col:
            start_col = ws_vals.max_column + 1
            pcol = start_col
            ws_vals.cell(row=1, column=pcol, value="productId")
            for i, v in enumerate(src_df[product_src_col].fillna("").astype(str).tolist(), start=2):
                val = v if v and str(v).strip() else None
                ws_vals.cell(row=i, column=pcol, value=val)
                if val is not None:
                    ws_vals.cell(row=i, column=pcol).number_format = "@"
            # Types
            tp = pcol + 2
            ws_types.cell(row=1, column=tp, value="productId")
            ws_types.cell(row=2, column=tp, value="productId")
            ws_types.cell(row=3, column=tp, value="mandatory")
            ws_types.cell(row=4, column=tp, value="string")

    # intentionally DO NOT append variantId/productId for non-General templates

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

# General-only UI fields
if marketplace_type == "General":
    st.markdown("### Header & Data rows\nSpecify which line contains the header and which line data starts (1-indexed).")
    col1, col2 = st.columns(2)
    with col1:
        header_row = st.number_input("Header row (line number containing column headers)", min_value=1, value=marketplace_defaults["General"][0])
    with col2:
        data_row = st.number_input("Data start row (first line of actual data)", min_value=1, value=marketplace_defaults["General"][1])

    st.markdown("### General template: provide exact source column names (exact match required)")
    st.caption("Provide the exact column name from your input file. If left blank, that output column will not be created.")
    col3, col4 = st.columns(2)
    with col3:
        general_style_col = st.text_input("Style Code column name (this will be written to 'variantId') (optional)")
    with col4:
        general_seller_sku_col = st.text_input("Seller SKU column name (this will be written to 'productId') (optional)")
else:
    header_row, data_row = marketplace_defaults.get(marketplace_type, (1,2))
    general_style_col = None
    general_seller_sku_col = None

input_file = st.file_uploader("Upload Input Excel File", type=["xlsx", "xls", "xlsm"])

if input_file:
    with st.spinner("Processing…"):
        result = process_file(
            input_file,
            marketplace_type,
            mapping_df=mapping_df if not mapping_df.empty else None,
            header_row_override=int(header_row),
            data_row_override=int(data_row),
            general_style_col=general_style_col if general_style_col and general_style_col.strip() else None,
            general_seller_sku_col=general_seller_sku_col if general_seller_sku_col and general_seller_sku_col.strip() else None,
        )
    if result:
        st.success("✅ Output Generated!")
        st.download_button("📥 Download Output", data=result, file_name="output_template.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.markdown("---")
st.caption("Built for Rubick.ai | By Vishnu Sai")


