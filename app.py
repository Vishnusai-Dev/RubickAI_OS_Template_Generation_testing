import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO

# ───────────────────────── FILE PATHS ─────────────────────────
TEMPLATE_PATH = "sku-template (4).xlsx"
MAPPING_PATH = "Mapping - Automation.xlsx"

# ─────────────────── INTERNAL COLUMN KEYS ───────────────────
ATTR_KEY = "attributes"
TARGET_KEY = "fieldname"
MAND_KEY = "mandatoryornot"
TYPE_KEY = "fieldtype"
DUP_KEY = "duplicatestobecreated"

# substrings used to find worksheets
MAPPING_SHEET_KEY = "mapping"
CLIENT_SHEET_KEY = "mappedclientname"
# ──────────────────────────────────────────────────────────────

# ╭───────────────── NORMALISERS & HELPERS ─────────────────╮
def norm(s) -> str:
    if pd.isna(s):
        return ""
    return "".join(str(s).split()).lower()

# Improved header cleaner: remove special characters (keep letters, numbers and spaces),
# collapse multi spaces, strip leading/trailing spaces
def clean_header(header) -> str:
    if pd.isna(header):
        return ""
    header_str = str(header)
    # replace dots with space first (original behavior)
    # then remove any character that is not a letter, digit or space
    header_str = header_str.replace(".", " ")
    # keep letters, numbers and spaces only
    header_str = re.sub(r"[^A-Za-z0-9\s]", "", header_str)
    # collapse multiple spaces to a single space and strip
    header_str = re.sub(r"\s+", " ", header_str).strip()
    return header_str

IMAGE_EXT_RE = re.compile(r"(?i)\.(jpe?g|png|gif|bmp|webp|tiff?)$")
IMAGE_KEYWORDS = {
    "image", "img", "picture", "photo", "thumbnail", "thumb",
    "hero", "front", "back", "url"
}

def is_image_column(col_header_norm: str, series: pd.Series) -> bool:
    header_hit = any(k in col_header_norm for k in IMAGE_KEYWORDS)
    sample = series.dropna().astype(str).head(20)
    ratio = sample.str.contains(IMAGE_EXT_RE).mean() if not sample.empty else 0.0
    return header_hit or ratio >= 0.30
# ╰───────────────────────────────────────────────────────────╯

@st.cache_data
def load_mapping():
    xl = pd.ExcelFile(MAPPING_PATH)
    map_sheet = next((s for s in xl.sheet_names if MAPPING_SHEET_KEY in norm(s)), xl.sheet_names[0])
    mapping_df = xl.parse(map_sheet)
    mapping_df.rename(columns={c: norm(c) for c in mapping_df.columns}, inplace=True)
    mapping_df["__attr_key"] = mapping_df[ATTR_KEY].apply(norm)

    client_names = []
    client_sheet = next((s for s in xl.sheet_names if CLIENT_SHEET_KEY in norm(s)), None)
    if client_sheet:
        raw = xl.parse(client_sheet, header=None)
        client_names = [str(x).strip() for x in raw.values.flatten() if pd.notna(x) and str(x).strip()]

    return mapping_df, client_names


def process_file(input_file,
                 marketplace: str,
                 mapping_df: pd.DataFrame | None = None,
                 header_row_override: int | None = None,
                 data_row_override: int | None = None,
                 general_style_col: str | None = None,
                 general_seller_sku_col: str | None = None):
    """
    Processes the input Excel file based on the selected marketplace.

    - Removed explicit Mapping/Auto-Mapping switch: the function will try to use the mapping
      workbook if available (mapping_df not None) and falls back to auto-mapping when no
      mapping is found for a column.
    - header_row_override/data_row_override allow the UI to control from which line the
      header is read and from which line data starts (1-indexed).
    - For General template the UI can supply exact column names to use as Style Code
      and Seller SKU ID for generating productId/variantId.
    """

    # 📝 Define marketplace-specific sheet, header, and data row configurations
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

    # Apply overrides if provided (UI supplies these for General or custom flows)
    header_row = header_row_override if header_row_override is not None else config["header_row"]
    data_row = data_row_override if data_row_override is not None else config["data_row"]

    try:
        if marketplace == "Flipkart":
            xl = pd.ExcelFile(input_file)
            temp_df = xl.parse(xl.sheet_names[config["sheet_index"]], header=None)
            header_idx = header_row - 1
            data_start_idx = data_row - 1

            headers = temp_df.iloc[header_idx].tolist()
            src_df = temp_df.iloc[data_start_idx:].copy()
            src_df.columns = headers

        elif config["sheet"] is not None:
            src_df = pd.read_excel(
                input_file,
                sheet_name=config["sheet"],
                header=header_row - 1,
                skiprows=data_row - header_row - 1
            )
        else:
            xl = pd.ExcelFile(input_file)
            src_df = xl.parse(
                xl.sheet_names[config["sheet_index"]],
                header=header_row - 1,
                skiprows=data_row - header_row - 1
            )

    except Exception as e:
        st.error(f"Error reading file for {marketplace} template: {e}")
        return None

    # ────────── DROP COMPLETELY EMPTY COLUMNS ──────────
    src_df.dropna(axis=1, how='all', inplace=True)

    columns_meta = []

    # Determine whether to use mapping: if mapping_df is provided we prefer it, otherwise auto-map
    use_mapping = mapping_df is not None

    # ────────── BUILD columns_meta ──────────
    if use_mapping:
        # mapping keys are normalized in load_mapping
        for col in src_df.columns:
            col_key = norm(col)
            matches = mapping_df[mapping_df["__attr_key"] == col_key]
            if not matches.empty:
                row3 = matches.iloc[0][MAND_KEY]
                row4 = matches.iloc[0][TYPE_KEY]
            else:
                # fallback to auto-type when column not found in mapping
                row3 = "mandatory"
                row4 = "imageurlarray" if is_image_column(norm(col), src_df[col]) else "string"
            columns_meta.append({"src": col, "out": col, "row3": row3, "row4": row4})
            for _, row in matches.iterrows():
                if str(row[DUP_KEY]).lower().startswith("yes"):
                    new_header = row[TARGET_KEY] if pd.notna(row[TARGET_KEY]) else col
                    if new_header != col:
                        columns_meta.append({
                            "src": col,
                            "out": new_header,
                            "row3": row[MAND_KEY],
                            "row4": row[TYPE_KEY]
                        })
    else:  # Auto-Mapping
        for col in src_df.columns:
            dtype = "imageurlarray" if is_image_column(norm(col), src_df[col]) else "string"
            columns_meta.append({"src": col, "out": col, "row3": "mandatory", "row4": dtype})

    # ────────── Identify and Extract Color & Size Columns ──────────
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

    # ────────── BUILD THE WORKBOOK ──────────
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

    # Write main mapped/auto-mapped columns to Values and Types
    for j, meta in enumerate(columns_meta, start=1):
        header_display = clean_header(meta["out"])
        ws_vals.cell(row=1, column=j, value=header_display)
        for i, v in enumerate(src_df[meta["src"]].tolist(), start=2):
            cell = ws_vals.cell(row=i, column=j)
            if pd.isna(v):
                cell.value = None
                continue
            if str(meta["row4"]).lower() in ("string", "imageurlarray"):
                cell.value = str(v)
                cell.number_format = "@"
            else:
                cell.value = v
        tcol = j + 2
        ws_types.cell(row=1, column=tcol, value=header_display)
        ws_types.cell(row=2, column=tcol, value=header_display)
        ws_types.cell(row=3, column=tcol, value=meta["row3"])
        ws_types.cell(row=4, column=tcol, value=meta["row4"])

    # ────────── APPEND OPTION 1 & OPTION 2 TO VALUES ──────────
    opt1_col = len(columns_meta) + 1
    opt2_col = len(columns_meta) + 2
    ws_vals.cell(row=1, column=opt1_col, value="Option 1")
    ws_vals.cell(row=1, column=opt2_col, value="Option 2")
    for i, v in enumerate(option1_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt1_col, value=v if v else None)
    for i, v in enumerate(option2_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt2_col, value=v if v else None)

    # ────────── APPEND OPTION 1 & OPTION 2 TO TYPES ──────────
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

    # Get unique values to add to the 'Types' sheet for validation
    unique_opt1 = option1_data.dropna().unique().tolist()
    unique_opt2 = option2_data.dropna().unique().tolist()
    for i, v in enumerate(unique_opt1, start=5):
        ws_types.cell(row=i, column=t1_col, value=v)
    for i, v in enumerate(unique_opt2, start=5):
        ws_types.cell(row=i, column=t2_col, value=v)

    # ────────── Flipkart/Celio/Zivame: append variantId/productId AT THE VERY END ──────────
    # For General template, user can supply column names to use for productId/variantId as well
    if marketplace.strip() in {"Flipkart", "Celio", "Zivame", "General"}:
        style_code_col  = None
        seller_sku_col  = None

        # Prefer exact column names supplied for General template
        if marketplace.strip() == "General":
            if general_style_col:
                style_code_col = next((c for c in src_df.columns if str(c).strip() == str(general_style_col).strip()), None)
            if general_seller_sku_col:
                seller_sku_col = next((c for c in src_df.columns if str(c).strip() == str(general_seller_sku_col).strip()), None)

        # Fallback detection for other marketplaces: exact header match as before
        if style_code_col is None:
            style_code_col  = next((c for c in src_df.columns if str(c).strip() == "Style Code"), None)
        if seller_sku_col is None:
            seller_sku_col  = next((c for c in src_df.columns if str(c).strip() == "Seller SKU ID"), None)

        if style_code_col is None:
            st.warning(f"{marketplace}: 'Style Code' column not found in input. 'productId' will be blank.")
            product_values = pd.Series([""] * len(src_df), dtype=str)
        else:
            product_values = src_df[style_code_col].fillna("").astype(str)

        if seller_sku_col is None:
            st.warning(f"{marketplace}: 'Seller SKU ID' column not found in input. 'variantId' will be blank.")
            variant_values = pd.Series([""] * len(src_df), dtype=str)
        else:
            variant_values = src_df[seller_sku_col].fillna("").astype(str)

        # Append strictly at the end of Values
        start_col = ws_vals.max_column + 1
        variant_col = start_col
        product_col = start_col + 1

        # Values headers
        ws_vals.cell(row=1, column=variant_col, value="variantId")
        ws_vals.cell(row=1, column=product_col, value="productId")

        # Values data (as text)
        for i, v in enumerate(variant_values.tolist(), start=2):
            cell = ws_vals.cell(row=i, column=variant_col, value=v if v else None)
            cell.number_format = "@"
        for i, v in enumerate(product_values.tolist(), start=2):
            cell = ws_vals.cell(row=i, column=product_col, value=v if v else None)
            cell.number_format = "@"

        # Types alignment: same +2 offset convention
        t_variant_col = variant_col + 2
        t_product_col = product_col + 2

        ws_types.cell(row=1, column=t_variant_col, value="variantId")
        ws_types.cell(row=2, column=t_variant_col, value="variantId")
        ws_types.cell(row=3, column=t_variant_col, value="mandatory")
        ws_types.cell(row=4, column=t_variant_col, value="string")

        ws_types.cell(row=1, column=t_product_col, value="productId")
        ws_types.cell(row=2, column=t_product_col, value="productId")
        ws_types.cell(row=3, column=t_product_col, value="mandatory")
        ws_types.cell(row=4, column=t_product_col, value="string")

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ───────────────────────── STREAMLIT UI ─────────────────────────
st.set_page_config(page_title="SKU Template Automation", layout="wide")
st.title("📊 SKU Template Automation Tool")

mapping_df, client_names = load_mapping()
# Removed display of mapped clients (Celio, Zivame, etc.)("⚠️ No client list found in the mapping workbook.")

# 📝 Add a dropdown for marketplace selection (added Zivame & Celio)
marketplace_options = ["General", "Amazon", "Flipkart", "Myntra", "Ajio", "TataCliq", "Zivame", "Celio"]
marketplace_type = st.selectbox("Select Template Type", marketplace_options)

# Removed explicit Mapping/Auto-Mapping - the app now automatically uses mapping workbook when available
# Provide header & data start inputs (1-indexed) ONLY for General template. For other marketplaces we
# use sensible defaults from marketplace presets (so the UI stays simple).
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

# If user selects General, show the extra controls to override header/data rows and
# optionally supply Style Code / Seller SKU column names. Otherwise keep defaults.
if marketplace_type == "General":
    st.markdown("### Header & Data rows
Specify which line contains the header and which line data starts (1-indexed).")
    col1, col2 = st.columns(2)
    with col1:
        header_row = st.number_input("Header row (line number containing column headers)", min_value=1, value=marketplace_defaults["General"][0])
    with col2:
        data_row = st.number_input("Data start row (first line of actual data)", min_value=1, value=marketplace_defaults["General"][1])

    # For General template ask for exact column names to map productId/variantId (optional)
    st.markdown("### General template: optional mappings for productId / variantId")
    st.caption("If your input file uses custom column names for Style Code / Seller SKU ID, provide them here (exact match).
If left blank the app will look for standard 'Style Code'/'Seller SKU ID' headers.")
    col3, col4 = st.columns(2)
    with col3:
        general_style_col = st.text_input("Style Code column name (optional)")
    with col4:
        general_seller_sku_col = st.text_input("Seller SKU ID column name (optional)")
else:
    # Use marketplace defaults and leave general mappings empty
    header_row, data_row = marketplace_defaults.get(marketplace_type, (1, 2))
    general_style_col = ""
    general_seller_sku_col = ""

# File uploader (always visible)
input_file = st.file_uploader("Upload Input Excel File", type=["xlsx", "xls", "xlsm"])

# The 'if input_file' block now automatically generates the output using mapping workbook when available.
if input_file:
    with st.spinner("Processing…"):
        result = process_file(
            input_file,
            marketplace_type,
            mapping_df=mapping_df,
            header_row_override=int(header_row),
            data_row_override=int(data_row),
            general_style_col=general_style_col if general_style_col.strip() else None,
            general_seller_sku_col=general_seller_sku_col if general_seller_sku_col.strip() else None,
        )

    if result:
        st.success("✅ Output Generated!")
        st.download_button(
            "📥 Download Output",
            data=result,
            file_name="output_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_button"
        )

st.markdown("---")
st.caption("Built for Rubick.ai | By Vishnu Sai")
