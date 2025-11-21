import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO

# ----------------- Config -----------------
TEMPLATE_PATH = "sku-template (4).xlsx"
MAPPING_PATH = "Mapping - Automation.xlsx"

# Internal keys
ATTR_KEY = "attributes"
TARGET_KEY = "fieldname"
MAND_KEY = "mandatoryornot"
TYPE_KEY = "fieldtype"
DUP_KEY = "duplicatestobecreated"

# Helpers
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
IMAGE_KEYWORDS = {"image", "img", "picture", "photo", "thumbnail", "thumb", "hero", "front", "back", "url"}

def is_image_column(col_header_norm: str, series: pd.Series) -> bool:
    header_hit = any(k in col_header_norm for k in IMAGE_KEYWORDS)
    sample = series.dropna().astype(str).head(20)
    ratio = sample.str.contains(IMAGE_EXT_RE).mean() if not sample.empty else 0.0
    return header_hit or ratio >= 0.30

@st.cache_data
def load_mapping():
    xl = pd.ExcelFile(MAPPING_PATH)
    map_sheet = next((s for s in xl.sheet_names if "mapping" in norm(s)), xl.sheet_names[0])
    mapping_df = xl.parse(map_sheet)
    mapping_df.rename(columns={c: norm(c) for c in mapping_df.columns}, inplace=True)
    mapping_df["__attr_key"] = mapping_df[ATTR_KEY].apply(norm)
    return mapping_df

# ----------------- Processor -----------------
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

    config = marketplace_configs.get(marketplace, marketplace_configs["General"])
    header_row = header_row_override if header_row_override is not None else config["header_row"]
    data_row = data_row_override if data_row_override is not None else config["data_row"]

    # Read input into DataFrame
    try:
        if marketplace == "Flipkart":
            xl = pd.ExcelFile(input_file)
            temp_df = xl.parse(xl.sheet_names[config["sheet_index"]], header=None)
            headers = temp_df.iloc[header_row - 1].tolist()
            src_df = temp_df.iloc[data_row - 1:].copy()
            src_df.columns = headers
        elif config["sheet"] is not None:
            src_df = pd.read_excel(input_file, sheet_name=config["sheet"], header=header_row - 1, skiprows=data_row - header_row - 1, dtype=str, engine="openpyxl")
        else:
            xl = pd.ExcelFile(input_file)
            src_df = xl.parse(xl.sheet_names[config["sheet_index"]], header=header_row - 1, skiprows=data_row - header_row - 1)
    except Exception as e:
        st.error(f"Error reading file for {marketplace} template: {e}")
        return None

    src_df.dropna(axis=1, how='all', inplace=True)

    # Build columns_meta using mapping_df when available
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
                row4 = "imageurlarray" if is_image_column(norm(col), src_df[col]) else "string"
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

    # option columns
    color_cols = [col for col in src_df.columns if "color" in norm(col) or "colour" in norm(col)]
    size_cols  = [col for col in src_df.columns if "size"  in norm(col)]
    option1_data = pd.Series([""] * len(src_df), dtype=str)
    option2_data = pd.Series([""] * len(src_df), dtype=str)
    if size_cols:
        option1_data = src_df[size_cols[0]].fillna('').astype(str).str.strip()
        if color_cols and color_cols[0] != size_cols[0]:
            option2_data = src_df[color_cols[0]].fillna('').astype(str).str.strip()
    elif color_cols:
        option2_data = src_df[color_cols[0]].fillna('').astype(str).str.strip()

    # build workbook
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

    # write mapped columns
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

    # append option columns
    opt1_col = len(columns_meta) + 1
    opt2_col = len(columns_meta) + 2
    ws_vals.cell(row=1, column=opt1_col, value="Option 1")
    ws_vals.cell(row=1, column=opt2_col, value="Option 2")
    for i, v in enumerate(option1_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt1_col, value=v if v else None)
    for i, v in enumerate(option2_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt2_col, value=v if v else None)

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

    # ───────────── PRODUCTID / VARIANTID MAPPING (FINAL) ─────────────
    marketplace_id_map = {
        "Amazon":   ("Seller SKU", "Parent SKU"),
        "Myntra":   ("styleId", "styleGroupId"),
        "Ajio":     ("*Item SKU", "*Style Code"),
        "Flipkart": ("Seller SKU ID", "Style Code"),
        "TataCliq": ("Seller Article SKU", "*Style Code"),
        "Zivame":   ("Style Code", "SKU Code"),
        "Celio":    ("Style Code", "SKU Code"),
    }

    def match_header(preferred, src_columns):
        if not preferred:
            return None
        if "*" in preferred:
            needle = norm(preferred.replace("*", ""))
            return next((c for c in src_columns if needle in norm(c)), None)
        return next((c for c in src_columns if norm(c) == norm(preferred)), None)

    # Important: General should ONLY use user input, no fallback
    if marketplace.strip() == "General":
        style_code_col = match_header(general_style_col, src_df.columns) if general_style_col else None
        seller_sku_col = match_header(general_seller_sku_col, src_df.columns) if general_seller_sku_col else None
        append_ids = bool(style_code_col or seller_sku_col)
    else:
        preferred_prod, preferred_var = marketplace_id_map.get(marketplace, (None, None))
        style_code_col = match_header(preferred_prod, src_df.columns)
        seller_sku_col = match_header(preferred_var, src_df.columns)
        if style_code_col is None:
            st.warning(f"{marketplace}: Could not find '{preferred_prod}' in input. productId will be blank.")
        if seller_sku_col is None:
            st.warning(f"{marketplace}: Could not find '{preferred_var}' in input. variantId will be blank.")
        append_ids = True

    if append_ids:
        product_values = src_df[style_code_col].fillna("").astype(str) if style_code_col else pd.Series([""]*len(src_df))
        variant_values = src_df[seller_sku_col].fillna("").astype(str) if seller_sku_col else pd.Series([""]*len(src_df))

        # write only when at least one has meaningful values
        if product_values.str.strip().replace('', pd.NA).notna().any() or variant_values.str.strip().replace('', pd.NA).notna().any():
            start_col = ws_vals.max_column + 1
            variant_col = start_col
            product_col = start_col + 1

            # write headers only if that column actually has data
            if variant_values.str.strip().replace('', pd.NA).notna().any():
                ws_vals.cell(row=1, column=variant_col, value="variantId")
            if product_values.str.strip().replace('', pd.NA).notna().any():
                ws_vals.cell(row=1, column=product_col, value="productId")

            for i, v in enumerate(variant_values.tolist(), start=2):
                val = v if (v and str(v).strip()) else None
                ws_vals.cell(row=i, column=variant_col, value=val)
                if val is not None:
                    ws_vals.cell(row=i, column=variant_col).number_format = "@"

            for i, v in enumerate(product_values.tolist(), start=2):
                val = v if (v and str(v).strip()) else None
                ws_vals.cell(row=i, column=product_col, value=val)
                if val is not None:
                    ws_vals.cell(row=i, column=product_col).number_format = "@"

            t_variant_col = variant_col + 2
            t_product_col = product_col + 2

            if variant_values.str.strip().replace('', pd.NA).notna().any():
                ws_types.cell(row=1, column=t_variant_col, value="variantId")
                ws_types.cell(row=2, column=t_variant_col, value="variantId")
                ws_types.cell(row=3, column=t_variant_col, value="mandatory")
                ws_types.cell(row=4, column=t_variant_col, value="string")

            if product_values.str.strip().replace('', pd.NA).notna().any():
                ws_types.cell(row=1, column=t_product_col, value="productId")
                ws_types.cell(row=2, column=t_product_col, value="productId")
                ws_types.cell(row=3, column=t_product_col, value="mandatory")
                ws_types.cell(row=4, column=t_product_col, value="string")

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ----------------- STREAMLIT UI -----------------
st.set_page_config(page_title="SKU Template Automation", layout="wide")
st.title("📊 SKU Template Automation Tool")

mapping_df, client_names = load_mapping()
# intentionally do not display mapped clients in the UI

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
We need to finish content? The developer message showed latest canvas content truncated. But in previous canmore.update_textdoc we replaced file. Now user requested Option A earlier; developer gave latest state of "Streamlit Sku Automation Updated" content which appears broken (has earlier injected UI lines inside process_file causing syntax error). But now user chose Option A, expecting new file created. We already created new canvas "SKU_Template_Automation_Final". The assistant should respond confirming creation and link. But the last message shows developer tool with latest state; we have to respond: created new file. Use friendly tone, confirm file name, and actions next (replace original or copy). Must not ask clarifying questions. Provide instructions to run. So reply: created file "SKU_Template_Automation_Final" in canvas; it's complete; can replace original if they want. Also mention that I fixed the General extra columns issue: variantId/SKU Code won't be added when General inputs blank. Offer to replace original file—user already asked replace earlier; they chose option A. They asked create new file; we did. Now tell them we created it and provide next steps: replace original? But earlier user said "Replace file now" and then later "option a" meaning create new file. They chose option a earlier. Now they said "Replace file now" then "option a". It's okay; we proceed. Provide path: it's on canvas and name. Offer to overwrite original canvas if desired. But user already asked earlier to replace; but now has chosen option a. So say created file and that we can now replace original if they want; ask? But instructions say don't ask clarifying questions unless necessary. Offer action: I can replace original; ask to confirm? But user earlier selected replace file now. Hmm.

To be safe: state that new file created and I replaced original? Wait canmore.create_textdoc executed and created new file. So tell user it's created and where; also mention how to test and that variantId/SKU Code issue fixed. Offer to overwrite original if they'd like (prompt). That is okay.

Let's respond concisely. Mention file name and that it's in canvas. Provide next steps: run in Streamlit, test with General with blank inputs — no variant/product columns. Offer to replace original canvas file; user earlier chose replace, but we should ask now? But requirement: if user wants replacement say "Replace original" earlier they chose; they've changed to option a. I'll simply confirm file created and offer to replace original if they want. Keep it short.

Let's finalize.

