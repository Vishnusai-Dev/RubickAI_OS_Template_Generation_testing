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
    header_str = str(header).replace('.', ' ')
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
    xl = pd.ExcelFile(MAPPING_PATH)
    sheet = next((s for s in xl.sheet_names if "mapping" in norm(s)), xl.sheet_names[0])
    df = xl.parse(sheet)
    df.rename(columns={c: norm(c) for c in df.columns}, inplace=True)
    df["__attr_key"] = df[ATTR_KEY].apply(norm)
    return df

# processor
def process_file(input_file,
                 marketplace: str,
                 mapping_df: pd.DataFrame | None = None,
                 header_row_override: int | None = None,
                 data_row_override: int | None = None,
                 general_style_col: str | None = None,
                 general_seller_sku_col: str | None = None):

    marketplace_configs = {
        "Amazon": {"sheet": "Template", "header_row": 2, "data_row": 4, "sheet_index": None},
        "Flipkart": {"sheet": None, "header_row": 1, "data_row": 5, "sheet_index": 2},
        "Myntra": {"sheet": None, "header_row": 3, "data_row": 4, "sheet_index": 1},
        "Ajio": {"sheet": None, "header_row": 2, "data_row": 3, "sheet_index": 2},
        "TataCliq": {"sheet": None, "header_row": 4, "data_row": 6, "sheet_index": 0},
        "General": {"sheet": None, "header_row": 1, "data_row": 2, "sheet_index": 0},
        "Celio": {"sheet": None, "header_row": 1, "data_row": 2, "sheet_index": 0},
        "Zivame": {"sheet": None, "header_row": 1, "data_row": 2, "sheet_index": 0},
    }

    cfg = marketplace_configs.get(marketplace, marketplace_configs["General"])
    header_row = header_row_override if header_row_override is not None else cfg["header_row"]
    data_row = data_row_override if data_row_override is not None else cfg["data_row"]

    try:
        if marketplace == "Flipkart":
            xl = pd.ExcelFile(input_file)
            temp = xl.parse(xl.sheet_names[cfg["sheet_index"]], header=None)
            headers = temp.iloc[header_row-1].tolist()
            src_df = temp.iloc[data_row-1:].copy()
            src_df.columns = headers
        elif cfg["sheet"] is not None:
            src_df = pd.read_excel(input_file, sheet_name=cfg["sheet"], header=header_row-1, skiprows=data_row-header_row-1, dtype=str, engine="openpyxl")
        else:
            xl = pd.ExcelFile(input_file)
            src_df = xl.parse(xl.sheet_names[cfg["sheet_index"]], header=header_row-1, skiprows=data_row-header_row-1)
    except Exception as e:
        st.error(f"Error reading file for {marketplace}: {e}")
        return None

    src_df.dropna(axis=1, how='all', inplace=True)

    # build columns meta
    columns_meta = []
    use_mapping = mapping_df is not None
    if use_mapping:
        for col in src_df.columns:
            col_key = norm(col)
            matches = mapping_df[mapping_df["__attr_key"] == col_key]
            if not matches.empty:
                row3 = matches.iloc[0][MAND_KEY]
                row4 = matches.iloc[0][TYPE_KEY]
            else:
                row3 = "mandatory"
                row4 = "imageurlarray" if is_image_column(col_key, src_df[col]) else "string"
            columns_meta.append({"src": col, "out": col, "row3": row3, "row4": row4})
            for _, row in matches.iterrows():
                if str(row[DUP_KEY]).lower().startswith("yes"):
                    new_header = row[TARGET_KEY] if pd.notna(row[TARGET_KEY]) else col
                    if new_header != col:
                        columns_meta.append({"src": col, "out": new_header, "row3": row[MAND_KEY], "row4": row[TYPE_KEY]})
    else:
        for col in src_df.columns:
            dtype = "imageurlarray" if is_image_column(norm(col), src_df[col]) else "string"
            columns_meta.append({"src": col, "out": col, "row3": "mandatory", "row4": dtype})

    # detect options
    color_cols = [c for c in src_df.columns if "color" in norm(c) or "colour" in norm(c)]
    size_cols = [c for c in src_df.columns if "size" in norm(c)]

    option1 = pd.Series([""]*len(src_df), dtype=str)
    option2 = pd.Series([""]*len(src_df), dtype=str)
    if size_cols:
        option1 = src_df[size_cols[0]].fillna('').astype(str).str.strip()
        if color_cols and color_cols[0] != size_cols[0]:
            option2 = src_df[color_cols[0]].fillna('').astype(str).str.strip()
    elif color_cols:
        option2 = src_df[color_cols[0]].fillna('').astype(str).str.strip()

    # build workbook
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

    for j, meta in enumerate(columns_meta, start=1):
        header_display = clean_header(meta["out"])
        ws_vals.cell(row=1, column=j, value=header_display)
        for i, v in enumerate(src_df[meta["src"]].tolist(), start=2):
            ws_vals.cell(row=i, column=j, value=(str(v) if (v is not None and v != "") else None))
            if v is not None and v != "":
                ws_vals.cell(row=i, column=j).number_format = "@"
        tcol = j + 2
        ws_types.cell(row=1, column=tcol, value=header_display)
        ws_types.cell(row=2, column=tcol, value=header_display)
        ws_types.cell(row=3, column=tcol, value=meta["row3"])
        ws_types.cell(row=4, column=tcol, value=meta["row4"])

    # option cols
    opt1_col = len(columns_meta)+1
    opt2_col = opt1_col+1
    ws_vals.cell(row=1, column=opt1_col, value="Option 1")
    ws_vals.cell(row=1, column=opt2_col, value="Option 2")
    for i, v in enumerate(option1.tolist(), start=2): ws_vals.cell(row=i, column=opt1_col, value=v or None)
    for i, v in enumerate(option2.tolist(), start=2): ws_vals.cell(row=i, column=opt2_col, value=v or None)

    t1 = opt1_col+2
    t2 = opt2_col+2
    ws_types.cell(row=1, column=t1, value="Option 1")
    ws_types.cell(row=2, column=t1, value="Option 1")
    ws_types.cell(row=3, column=t1, value="non mandatory")
    ws_types.cell(row=4, column=t1, value="select")
    ws_types.cell(row=1, column=t2, value="Option 2")
    ws_types.cell(row=2, column=t2, value="Option 2")
    ws_types.cell(row=3, column=t2, value="non mandatory")
    ws_types.cell(row=4, column=t2, value="select")

    unique_opt1 = option1.dropna().unique().tolist()
    unique_opt2 = option2.dropna().unique().tolist()
    for i, v in enumerate(unique_opt1, start=5): ws_types.cell(row=i, column=t1, value=v)
    for i, v in enumerate(unique_opt2, start=5): ws_types.cell(row=i, column=t2, value=v)

    # product/variant mapping
    marketplace_id_map = {
        "Amazon": ("Seller SKU", "Parent SKU"),
        "Myntra": ("styleId", "styleGroupId"),
        "Ajio": ("*Item SKU", "*Style Code"),
        "Flipkart": ("Seller SKU ID", "Style Code"),
        "TataCliq": ("Seller Article SKU", "*Style Code"),
        "Zivame": ("Style Code", "SKU Code"),
        "Celio": ("Style Code", "SKU Code"),
    }

    def match_header(preferred, cols):
        if not preferred:
            return None
        if "*" in preferred:
            needle = norm(preferred.replace("*", ""))
            return next((c for c in cols if needle in norm(c)), None)
        return next((c for c in cols if norm(c) == norm(preferred)), None)

    if marketplace.strip() == "General":
        style_col = match_header(general_style_col, src_df.columns) if general_style_col else None
        sku_col = match_header(general_seller_sku_col, src_df.columns) if general_seller_sku_col else None
        append_ids = bool(style_col or sku_col)
    else:
        p_prod, p_var = marketplace_id_map.get(marketplace, (None, None))
        style_col = match_header(p_prod, src_df.columns)
        sku_col = match_header(p_var, src_df.columns)
        append_ids = True

    if append_ids:
        prod_vals = src_df[style_col].fillna('').astype(str) if style_col else pd.Series(['']*len(src_df))
        var_vals = src_df[sku_col].fillna('').astype(str) if sku_col else pd.Series(['']*len(src_df))

        if prod_vals.str.strip().replace('', pd.NA).notna().any() or var_vals.str.strip().replace('', pd.NA).notna().any():
            start = ws_vals.max_column + 1
            vcol = start
            pcol = start + 1
            if var_vals.str.strip().replace('', pd.NA).notna().any(): ws_vals.cell(row=1, column=vcol, value="variantId")
            if prod_vals.str.strip().replace('', pd.NA).notna().any(): ws_vals.cell(row=1, column=pcol, value="productId")
            for i, v in enumerate(var_vals.tolist(), start=2): ws_vals.cell(row=i, column=vcol, value=v or None)
            for i, v in enumerate(prod_vals.tolist(), start=2): ws_vals.cell(row=i, column=pcol, value=v or None)
            tv = vcol + 2
            tp = pcol + 2
            if var_vals.str.strip().replace('', pd.NA).notna().any():
                ws_types.cell(row=1, column=tv, value="variantId")
                ws_types.cell(row=2, column=tv, value="variantId")
                ws_types.cell(row=3, column=tv, value="mandatory")
                ws_types.cell(row=4, column=tv, value="string")
            if prod_vals.str.strip().replace('', pd.NA).notna().any():
                ws_types.cell(row=1, column=tp, value="productId")
                ws_types.cell(row=2, column=tp, value="productId")
                ws_types.cell(row=3, column=tp, value="mandatory")
                ws_types.cell(row=4, column=tp, value="string")

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ---------------- UI ----------------
st.set_page_config(page_title="SKU Template Automation", layout="wide")
st.title("📊 SKU Template Automation Tool")

mapping_df = load_mapping()

marketplace_options = ["General", "Amazon", "Flipkart", "Myntra", "Ajio", "TataCliq", "Zivame", "Celio"]
marketplace_type = st.selectbox("Select Template Type", marketplace_options)

marketplace_defaults = {
    "Amazon": (2,4),
    "Flipkart": (1,5),
    "Myntra": (3,4),
    "Ajio": (2,3),
    "TataCliq": (4,6),
    "General": (1,2),
    "Celio": (1,2),
    "Zivame": (1,2)
}

# show header/data and general mappings ONLY for General
if marketplace_type == "General":
    st.markdown("""
    ### Header & Data rows
    Specify which line contains the header and which line data starts (1-indexed).
    """)
    col1, col2 = st.columns(2)
    with col1:
        header_row = st.number_input("Header row (line number containing column headers)", min_value=1, value=marketplace_defaults["General"][0])
    with col2:
        data_row = st.number_input("Data start row (first line of actual data)", min_value=1, value=marketplace_defaults["General"][1])

    st.markdown("""
We created the clean file; now must respond telling user file created and fixed. The create_textdoc succeeded. Now reply summarizing fix and instruct user to run. Also note that developer message said refer to latest state when user requests edits; but we created new clean file. Now respond: file created, name, explain fixes, how to test, offer to replace original corrupted file. No further clarifying questions. Let's produce final message. Ensure warm, concise. Provide next steps to run and confirm. Also mention that the old corrupted file remains and can be deleted. Done.
