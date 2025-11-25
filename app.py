import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO
import os

# ───────────────────────── FILE PATHS ─────────────────────────
# Default template filename (keeps your original)
DEFAULT_TEMPLATE = "sku-template (4).xlsx"
# If the uploaded template used in this session exists on disk, prefer it
FALLBACK_UPLOADED_TEMPLATE = "/mnt/data/output_template (62).xlsx"

if os.path.exists(FALLBACK_UPLOADED_TEMPLATE):
    TEMPLATE_PATH = FALLBACK_UPLOADED_TEMPLATE
else:
    TEMPLATE_PATH = DEFAULT_TEMPLATE

# ╭───────────────── NORMALISERS & HELPERS ─────────────────╮
def norm(s) -> str:
    if pd.isna(s):
        return ""
    return "".join(str(s).split()).lower()

def clean_header(header) -> str:
    if pd.isna(header):
        return ""
    header_str = str(header)
    header_str = re.sub(r"[^0-9A-Za-z ]+", " ", header_str)
    header_str = re.sub(r"\s+", " ", header_str).strip()
    return header_str

IMAGE_EXT_RE = re.compile(r"(?i)\.(jpe?g|png|gif|bmp|webp|tiff?)$")
IMAGE_KEYWORDS = {"image", "img", "picture", "photo", "thumbnail", "thumb", "hero", "front", "back", "url"}

def is_image_column(col_header_norm: str, series: pd.Series) -> bool:
    header_hit = any(k in col_header_norm for k in IMAGE_KEYWORDS)
    sample = series.dropna().astype(str).head(20)
    ratio = sample.str.contains(IMAGE_EXT_RE).mean() if not sample.empty else 0.0
    return header_hit or ratio >= 0.30
# ╰───────────────────────────────────────────────────────────╯

# Marketplace -> (productId source header, variantId source header)
MARKETPLACE_ID_MAP = {
    "Amazon": ("Seller SKU", "Parent SKU"),
    "Myntra": ("styleId", "styleGroupId"),
    "Ajio": ("*Item SKU", "*Style Code"),
    "Flipkart": ("Seller SKU ID", "Style Code"),
    "TataCliq": ("Seller Article SKU", "*Style Code"),
    "Zivame": ("Style Code", "SKU Code"),
    "Celio": ("Style Code", "SKU Code"),
}

def find_column_by_name_like(src_df: pd.DataFrame, name: str):
    if not name:
        return None
    name = str(name).strip()
    for c in src_df.columns:
        if str(c).strip() == name:
            return c
    nname = norm(name)
    for c in src_df.columns:
        if norm(c) == nname:
            return c
    for c in src_df.columns:
        if nname in norm(c):
            return c
    return None

def read_input_to_df(input_file, marketplace, header_row=1, data_row=2, sheet_name=None):
    """
    Read uploaded excel into a dataframe using marketplace config or supplied header/data rows (1-indexed).
    If `sheet_name` is provided for General, parse that sheet directly. Returns dataframe or raises exception.
    """
    marketplace_configs = {
        "Amazon": {"sheet": "Template", "header_row": 2, "data_row": 4, "sheet_index": None},
        "Flipkart": {"sheet": None, "header_row": 1, "data_row": 5, "sheet_index": 2},
        "Myntra": {"sheet": None, "header_row": 3, "data_row": 4, "sheet_index": 1},
        "Ajio": {"sheet": None, "header_row": 2, "data_row": 3, "sheet_index": 2},
        "TataCliq": {"sheet": None, "header_row": 4, "data_row": 6, "sheet_index": 0},
        "General": {"sheet": None, "header_row": header_row, "data_row": data_row, "sheet_index": 0}
    }
    config = marketplace_configs.get(marketplace, marketplace_configs["General"])

    # If user supplied an explicit sheet_name for General, prefer that
    if marketplace == "General" and sheet_name:
        xl = pd.ExcelFile(input_file)
        src_df = xl.parse(sheet_name, header=header_row - 1, skiprows=data_row - header_row - 1)
    elif marketplace == "Flipkart":
        xl = pd.ExcelFile(input_file)
        temp_df = xl.parse(xl.sheet_names[config["sheet_index"]], header=None)
        header_idx = config["header_row"] - 1
        data_idx = config["data_row"] - 1
        headers = temp_df.iloc[header_idx].tolist()
        src_df = temp_df.iloc[data_idx:].copy()
        src_df.columns = headers
    elif config["sheet"] is not None:
        src_df = pd.read_excel(
            input_file,
            sheet_name=config["sheet"],
            header=config["header_row"] - 1,
            skiprows=config["data_row"] - config["header_row"] - 1
        )
    else:
        xl = pd.ExcelFile(input_file)
        src_df = xl.parse(
            xl.sheet_names[config["sheet_index"]],
            header=config["header_row"] - 1,
            skiprows=config["data_row"] - config["header_row"] - 1
        )
    src_df.dropna(axis=1, how='all', inplace=True)
    return src_df

# -------------------------
# Robust process_file()
# -------------------------
def process_file(
    input_file,
    marketplace: str,
    selected_variant_col: str | None = None,
    selected_product_col: str | None = None,
    general_header_row: int = 1,
    general_data_row: int = 2,
    general_sheet_name: str | None = None,
):
    """
    Read src_df and write a clean, contiguous output into TEMPLATE_PATH workbook.
    Writes columns contiguously into Values and Types starting from the first truly empty column.
    Places Option 1/2 immediately after data columns and variantId/productId immediately after options.
    Returns a BytesIO with the saved workbook.
    """
    # Read source dataframe using your existing reader
    src_df = read_input_to_df(input_file, marketplace, header_row=general_header_row, data_row=general_data_row, sheet_name=general_sheet_name)

    # auto-map every column
    columns_meta = []
    for col in src_df.columns:
        dtype = "imageurlarray" if is_image_column(norm(col), src_df[col]) else "string"
        columns_meta.append({"src": col, "out": col, "row3": "mandatory", "row4": dtype})

    # identify color/size
    color_cols = [col for col in src_df.columns if "color" in norm(col) or "colour" in norm(col)]
    size_cols = [col for col in src_df.columns if "size" in norm(col)]

    option1_data = pd.Series([""] * len(src_df), dtype=str)
    option2_data = pd.Series([""] * len(src_df), dtype=str)

    if size_cols:
        option1_data = src_df[size_cols[0]].fillna('').astype(str).str.strip()
        if color_cols and color_cols[0] != size_cols[0]:
            option2_data = src_df[color_cols[0]].fillna('').astype(str).str.strip()
    elif color_cols:
        option2_data = src_df[color_cols[0]].fillna('').astype(str).str.strip()

    unique_opt1 = option1_data.replace("", pd.NA).dropna().unique().tolist()
    unique_opt2 = option2_data.replace("", pd.NA).dropna().unique().tolist()

    # load workbook and target sheets
    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

    # ------------------- helper: find first empty column -------------------
    def first_empty_col(ws, header_rows=(1,)):
        """
        Returns first column index (1-based) where all header_rows are empty.
        Scans up to 200 columns to be safe.
        """
        for col_idx in range(1, 201):
            empty = True
            for r in header_rows:
                if ws.cell(row=r, column=col_idx).value not in (None, ""):
                    empty = False
                    break
            if empty:
                return col_idx
        return ws.max_column + 1

    # ---------- Determine contiguous starting columns in Values & Types ----------
    vals_start_col = first_empty_col(ws_vals, header_rows=(1,))
    types_start_col = first_empty_col(ws_types, header_rows=(1,2,3,4))

    # ===================== WRITE columns_meta CLEANLY =====================
    for idx, meta in enumerate(columns_meta):
        # aligned positions
        vcol = vals_start_col + idx
        tcol = types_start_col + idx

        header_display = clean_header(meta["out"])

        # --- Write to VALUES ---
        ws_vals.cell(row=1, column=vcol, value=header_display)
        for r_idx, value in enumerate(src_df[meta["src"]].tolist(), start=2):
            cell = ws_vals.cell(row=r_idx, column=vcol)
            if pd.isna(value):
                cell.value = None
            else:
                if str(meta["row4"]).lower() in ("string", "imageurlarray"):
                    cell.value = str(value)
                    cell.number_format = "@"
                else:
                    cell.value = value

        # --- Write to TYPES ---
        ws_types.cell(row=1, column=tcol, value=header_display)
        ws_types.cell(row=2, column=tcol, value=header_display)
        ws_types.cell(row=3, column=tcol, value=meta["row3"])
        ws_types.cell(row=4, column=tcol, value=meta["row4"])

    # ===================== APPEND Option1 & Option2 =====================
    opt1_vcol = vals_start_col + len(columns_meta)
    opt2_vcol = opt1_vcol + 1

    ws_vals.cell(row=1, column=opt1_vcol, value="Option 1")
    ws_vals.cell(row=1, column=opt2_vcol, value="Option 2")

    for i, v in enumerate(option1_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt1_vcol, value=v if v else None)
    for i, v in enumerate(option2_data.tolist(), start=2):
        ws_vals.cell(row=i, column=opt2_vcol, value=v if v else None)

    # Types aligned
    opt1_tcol = types_start_col + len(columns_meta)
    opt2_tcol = opt1_tcol + 1

    ws_types.cell(row=1, column=opt1_tcol, value="Option 1")
    ws_types.cell(row=2, column=opt1_tcol, value="Option 1")
    ws_types.cell(row=3, column=opt1_tcol, value="non mandatory")
    ws_types.cell(row=4, column=opt1_tcol, value="select")

    ws_types.cell(row=1, column=opt2_tcol, value="Option 2")
    ws_types.cell(row=2, column=opt2_tcol, value="Option 2")
    ws_types.cell(row=3, column=opt2_tcol, value="non mandatory")
    ws_types.cell(row=4, column=opt2_tcol, value="select")

    # Unique values start from row 5
    for i, val in enumerate(unique_opt1, start=5):
        ws_types.cell(row=i, column=opt1_tcol, value=val)
    for i, val in enumerate(unique_opt2, start=5):
        ws_types.cell(row=i, column=opt2_tcol, value=val)

    # ===================== APPEND variantId & productId immediately after options =====================
    def append_id_columns(variant_series: pd.Series | None, product_series: pd.Series | None):
        nonlocal ws_vals, ws_types, vals_start_col, types_start_col, columns_meta
        has_var = variant_series is not None and variant_series.replace("", pd.NA).dropna().shape[0] > 0
        has_prod = product_series is not None and product_series.replace("", pd.NA).dropna().shape[0] > 0
        if not (has_var or has_prod):
            return

        # column immediately after data columns + Option1 + Option2
        after_written_vals = vals_start_col + len(columns_meta) + 2
        after_written_types = types_start_col + len(columns_meta) + 2

        cur_v = after_written_vals
        cur_t = after_written_types

        # Write variantId first (if present)
        if has_var:
            ws_vals.cell(row=1, column=cur_v, value="variantId")
            for i, v in enumerate(variant_series.tolist(), start=2):
                cell = ws_vals.cell(row=i, column=cur_v, value=v if v else None)
                cell.number_format = "@"
            ws_types.cell(row=1, column=cur_t, value="variantId")
            ws_types.cell(row=2, column=cur_t, value="variantId")
            ws_types.cell(row=3, column=cur_t, value="mandatory")
            ws_types.cell(row=4, column=cur_t, value="string")
            cur_v += 1
            cur_t += 1

        # Then write productId (if present)
        if has_prod:
            ws_vals.cell(row=1, column=cur_v, value="productId")
            for i, v in enumerate(product_series.tolist(), start=2):
                cell = ws_vals.cell(row=i, column=cur_v, value=v if v else None)
                cell.number_format = "@"
            ws_types.cell(row=1, column=cur_t, value="productId")
            ws_types.cell(row=2, column=cur_t, value="productId")
            ws_types.cell(row=3, column=cur_t, value="mandatory")
            ws_types.cell(row=4, column=cur_t, value="string")

    # General marketplace: UI-driven column selection already occurred in the app; here we just append if series provided
    if marketplace == "General":
        variant_series = None
        product_series = None
        if selected_variant_col and selected_variant_col != "(none)":
            if selected_variant_col in src_df.columns:
                variant_series = src_df[selected_variant_col].fillna("").astype(str)
        if selected_product_col and selected_product_col != "(none)":
            if selected_product_col in src_df.columns:
                product_series = src_df[selected_product_col].fillna("").astype(str)
        append_id_columns(variant_series, product_series)
    else:
        # other marketplaces: auto-find based on MARKETPLACE_ID_MAP and append if found
        mapping = MARKETPLACE_ID_MAP.get(marketplace, None)
        if mapping:
            prod_src_name, var_src_name = mapping
            prod_col = find_column_by_name_like(src_df, prod_src_name)
            var_col = find_column_by_name_like(src_df, var_src_name)
            prod_series = src_df[prod_col].fillna("").astype(str) if prod_col else None
            var_series = src_df[var_col].fillna("").astype(str) if var_col else None
            append_id_columns(var_series, prod_series)

    # Save workbook to BytesIO and return
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf

# ───────────────────────── STREAMLIT UI ─────────────────────────
st.set_page_config(page_title="SKU Template Automation", layout="wide")

# Title only (logo removed)
st.title("Rubick OS Template Conversion")

# If template file exists, show a small note and a download link
if os.path.exists(TEMPLATE_PATH):
    st.info(f"Using template: {os.path.basename(TEMPLATE_PATH)}")
    try:
        with open(TEMPLATE_PATH, "rb") as f:
            st.download_button("Download current template (for reference)", data=f.read(),
                               file_name=os.path.basename(TEMPLATE_PATH),
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception:
        pass

marketplace_options = ["General", "Amazon", "Flipkart", "Myntra", "Ajio", "TataCliq", "Zivame", "Celio"]
marketplace_type = st.selectbox("Select Template Type", marketplace_options)

# General header/data inputs (number inputs)
general_header_row = 1
general_data_row = 2
if marketplace_type == "General":
    st.info("Callout: If header/data rows are left as default we will assume Header row = 1 and Data row = 2.")
    general_header_row = st.number_input("Header row (1-indexed)", min_value=1, value=1, step=1)
    general_data_row = st.number_input("Data row (1-indexed)", min_value=1, value=2, step=1)

input_file = st.file_uploader("Upload Input Excel File", type=["xlsx", "xls", "xlsm"])

selected_variant_col = "(none)"
selected_product_col = "(none)"

if input_file:
    selected_sheet = None
    if marketplace_type == "General":
        try:
            xl = pd.ExcelFile(input_file)
            sheets = xl.sheet_names
            selected_sheet = st.selectbox("Select sheet", sheets)
        except Exception as e:
            st.error(f"Failed to read sheets from uploaded file: {e}")
            selected_sheet = None

    try:
        src_df = read_input_to_df(input_file, marketplace_type, header_row=general_header_row, data_row=general_data_row, sheet_name=selected_sheet)
    except Exception as e:
        st.error(f"Failed to parse uploaded file: {e}")
        src_df = None

    if src_df is not None:
        if marketplace_type == "General":
            st.markdown("**Sample data (first 3 rows)**")
            st.dataframe(src_df.head(3))

            cols = ["(none)"] + [str(c) for c in src_df.columns]
            col1, col2 = st.columns(2)
            with col1:
                selected_variant_col = st.selectbox("Style Code → productId (leave '(none)' to skip)", options=cols, index=0)
            with col2:
                selected_product_col = st.selectbox("Seller SKU → variantId (leave '(none)' to skip)", options=cols, index=0)
        else:
            st.subheader("Preview (first 5 rows)")
            st.dataframe(src_df.head(5))

        st.markdown("---")
        if marketplace_type == "General":
            if st.button("Generate Output"):
                with st.spinner("Processing…"):
                    result = process_file(
                        input_file,
                        marketplace_type,
                        selected_variant_col=selected_variant_col,
                        selected_product_col=selected_product_col,
                        general_header_row=general_header_row,
                        general_data_row=general_data_row,
                        general_sheet_name=selected_sheet,
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
        else:
            # auto-run for non-General
            with st.spinner("Processing…"):
                result = process_file(
                    input_file,
                    marketplace_type,
                    selected_variant_col=None,
                    selected_product_col=None,
                    general_header_row=general_header_row,
                    general_data_row=general_data_row,
                    general_sheet_name=None,
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
else:
    st.info("Upload a file to enable header-detection and column selection dropdowns (General only).")

st.markdown("---")
st.caption("Built for Rubick.ai | By Vishnu Sai")
