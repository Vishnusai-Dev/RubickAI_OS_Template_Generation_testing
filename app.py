import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO

# ───────────────────────── FILE PATHS ─────────────────────────
TEMPLATE_PATH = "sku-template (4).xlsx"

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


def process_file(
    input_file,
    marketplace: str,
    selected_variant_col: str | None = None,
    selected_product_col: str | None = None,
    general_header_row: int = 1,
    general_data_row: int = 2,
    general_sheet_name: str | None = None,
):
    # read src_df using header/data rows for General
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

    wb = openpyxl.load_workbook(TEMPLATE_PATH)
    ws_vals = wb["Values"]
    ws_types = wb["Types"]

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

    # append Option 1 & 2
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

    # helper to append id columns
    def append_id_columns(variant_series: pd.Series | None, product_series: pd.Series | None):
        nonlocal ws_vals, ws_types
        have_variant = variant_series is not None and variant_series.replace("", pd.NA).dropna().shape[0] > 0
        have_product = product_series is not None and product_series.replace("", pd.NA).dropna().shape[0] > 0
        if not (have_variant or have_product):
            return
        start_col = ws_vals.max_column + 1
        v_col = start_col if have_variant else None
        p_col = start_col + 1 if have_variant and have_product else (start_col if have_product and not have_variant else None)
        if have_variant:
            ws_vals.cell(row=1, column=v_col, value="variantId")
            for i, v in enumerate(variant_series.tolist(), start=2):
                cell = ws_vals.cell(row=i, column=v_col, value=v if v else None)
                cell.number_format = "@"
            t_v_col = v_col + 2
            ws_types.cell(row=1, column=t_v_col, value="variantId")
            ws_types.cell(row=2, column=t_v_col, value="variantId")
            ws_types.cell(row=3, column=t_v_col, value="mandatory")
            ws_types.cell(row=4, column=t_v_col, value="string")
        if have_product:
            p_col_actual = p_col if p_col is not None else (v_col + 1 if v_col is not None else start_col)
            ws_vals.cell(row=1, column=p_col_actual, value="productId")
            for i, v in enumerate(product_series.tolist(), start=2):
                cell = ws_vals.cell(row=i, column=p_col_actual, value=v if v else None)
                cell.number_format = "@"
            t_p_col = p_col_actual + 2
            ws_types.cell(row=1, column=t_p_col, value="productId")
            ws_types.cell(row=2, column=t_p_col, value="productId")
            ws_types.cell(row=3, column=t_p_col, value="mandatory")
            ws_types.cell(row=4, column=t_p_col, value="string")

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

    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# ───────────────────────── STREAMLIT UI ─────────────────────────
st.set_page_config(page_title="SKU Template Automation", layout="wide")
# Title only (logo removed)
# --- Centered Logo and Title ---
import base64
logo_path = "/mnt/data/rubick Logo transparent (2).png"
with open(logo_path, "rb") as f:
    encoded = base64.b64encode(f.read()).decode()
st.markdown(f"""
<div style='text-align:center;'>
    <img src='data:image/png;base64,{encoded}' width='240'/>
</div>
""", unsafe_allow_html=True)
st.markdown("<h1 style='text-align:center;'>Rubick OS Template Conversion</h1>", unsafe_allow_html=True)

marketplace_options = ["General", "Amazon", "Flipkart", "Myntra", "Ajio", "TataCliq", "Zivame", "Celio"]
marketplace_type = st.selectbox("Select Template Type", marketplace_options)

# General header/data inputs (number inputs)
general_header_row = 1
general_data_row = 2
if marketplace_type == "General":
    st.info("Callout: If header/data rows are left as default we will assume Header row = 1 and Data row = 2.")
    # Show header/data row inputs directly
    general_header_row = st.number_input("Header row (1-indexed)", min_value=1, value=1, step=1)
    general_data_row = st.number_input("Data row (1-indexed)", min_value=1, value=2, step=1)

input_file = st.file_uploader("Upload Input Excel File", type=["xlsx", "xls", "xlsm"])

selected_variant_col = "(none)"
selected_product_col = "(none)"

if input_file:
    # For General, ask user to choose the sheet to read from and reuse it for all operations
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
        # Parse the file using the selected sheet for General (if any)
        src_df = read_input_to_df(input_file, marketplace_type, header_row=general_header_row, data_row=general_data_row, sheet_name=selected_sheet)
    except Exception as e:
        st.error(f"Failed to parse uploaded file: {e}")
        src_df = None

    if src_df is not None:
        # Show the header row values and first 3 data rows directly for General only
        if marketplace_type == "General":
            # Removed detected header row values display
            st.markdown("**Sample data (first 3 rows)**")
            st.dataframe(src_df.head(3))

            # Build dropdowns from detected headers for selecting Style / Seller columns
            cols = ["(none)"] + [str(c) for c in src_df.columns]
            col1, col2 = st.columns(2)
            with col1:
                selected_variant_col = st.selectbox("Style Code → variantId (leave '(none)' to skip)", options=cols, index=0)
            with col2:
                selected_product_col = st.selectbox("Seller SKU → productId (leave '(none)' to skip)", options=cols, index=0)

        else:
            # Non-General: don't show any header/data UI, but display a small preview
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
