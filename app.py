import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO
from pathlib import Path

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
    header_str = header_str.replace(".", " ")
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
# ╰───────────────────────────────────────────────────────────╯

@st.cache_data
def load_mapping():
    xl = pd.ExcelFile(MAPPING_PATH)
    map_sheet = next((s for s in xl.sheet_names if MAPPING_SHEET_KEY in norm(s)), xl.sheet_names[0])
    mapping_df = xl.parse(map_sheet)
    mapping_df.rename(columns={c: norm(c) for c in mapping_df.columns}, inplace=True)
    mapping_df["__attr_key"] = mapping_df[ATTR_KEY].apply(norm)
    return mapping_df


def process_file(input_file,
                 marketplace: str,
                 mapping_df: pd.DataFrame | None = None,
                 header_row_override: int | None = None,
                 data_row_override: int | None = None,
                 general_style_col: str | None = None,
                 general_seller_sku_col: str | None = None):
    """
    Processes the input Excel file based on the selected marketplace.

    - Uses mapping workbook when available and falls back to auto-mapping for unknown columns.
    - header_row_override/data_row_override control from which line header/data are read (1-indexed).
    - For General template the UI can supply exact column names to use as Style Code and Seller SKU ID.
    - For all non-General marketplaces productId/variantId mapping is hard-coded in the function.
    """

    # marketplace presets
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
                skiprows=data_row - header_row - 1,
                dtype=str,
                engine="openpyxl"
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

    # Drop empty columns
    src_df.dropna(axis=1, how='all', inplace=True)

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
                        columns_meta.append({
                            "src": col,
                            "out": new_header,
                            "row3": row[MAND_KEY],
                            "row4": row[TYPE_KEY]
                        })
    else:
        for col in src_df.columns:
            dtype = "imageurlarray" if is_image_column(norm(col), src_df[col]) else "string"
            columns_meta.append({"src": col, "out": col, "row3": "mandatory", "row4": dtype})

    # Identify option columns
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

    # Build workbook
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

    # Append Option columns
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

    # Append variantId/productId for marketplaces including General
    if marketplace.strip() in {"Flipkart", "Celio", "Zivame", "General"}:
        style_code_col  = None
        seller_sku_col  = None

        if marketplace.strip() == "General":
            if general_style_col:
                style_code_col = next((c for c in src_df.columns if str(c).strip() == str(general_style_col).strip()), None)
            if general_seller_sku_col:
                seller_sku_col = next((c for c in src_df.columns if str(c).strip() == str(general_seller_sku_col).strip()), None)

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

        start_col = ws_vals.max_column + 1
        variant_col = start_col
        product_col = start_col + 1

        ws_vals.cell(row=1, column=variant_col, value="variantId")
        ws_vals.cell(row=1, column=product_col, value="productId")

        for i, v in enumerate(variant_values.tolist(), start=2):
            cell = ws_vals.cell(row=i, column=variant_col, value=v if v else None)
            cell.number_format = "@"
        for i, v in enumerate(product_values.tolist(), start=2):
            cell = ws_vals.cell(row=i, column=product_col, value=v if v else None)
            cell.number_format = "@"

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

We need to update mapping to user's provided. Create a new canvas? Developer note indicates existing canvas id. We created a new document earlier "Streamlit Sku Automation - Mapping Updated" — created. Now user pasted current code and asked ensure regex replacement valid. They want mapping refreshed. We should update existing canvas file 'Streamlit Sku Automation Updated' referenced in developer details. But easier: update the new doc with code that matches current content and includes mapping. They asked "it didn't add variantid & productid mapping for all marketplaces. below is the mapping refresh the current mapping" So we must update the canvas file to include mapping for all marketplaces.

We already created "Streamlit Sku Automation - Mapping Updated" earlier; now need to inform user it's updated. But per actions, we created that file. Now respond: tell them we've updated mapping in the new file, explain changes and show the mapping table. Offer to replace original file. Provide details about where code uses mapping and how to adjust asterisk entries (*Item SKU means contains?). They included entries with asterisks — clarify interpretation: currently code will match exact including '*' — probably they meant wildcard. Need to ask? But per system: don't ask clarifying question; make best effort. We should treat '*' as literal? Better to interpret '*' as wildcard meaning contains. We'll implement mapping such that if preferred header contains '*' we do contains-match by stripping '*' and checking substring. In our created file we didn't implement that previously — need to ensure new file handles '*' wildcard. Let's update the created file to include marketplace_id_map and matching logic that supports '*' meaning contains. Did our created file include mapping? It included earlier marketplace_id_map? Let's check content: In created file, in previous code I didn't include variant block mapping — I did earlier in create? Looking at the created content above, it seems generic and earlier didn't include mapping block. Wait created file content was the cleaned full code earlier? It had not included mapping update. We need to ensure new file includes updated mapping logic. But we already created "Streamlit Sku Automation - Mapping Updated" earlier with full code before; I included mapping? I need to check but can't. Simpler: create another canvas file with final code implementing everything and mapping logic. Use canmore.create_textdoc again with name "Streamlit Sku Automation - Final Mapping" and content full code that includes marketplace_id_map with wildcard handling. Then tell user it's created and where. Let's produce final code: base from earlier v2 but add mapping block: marketplace_id_map with given mappings; implement matching function match_header(preferred, src_df) that if preferred contains '*' treat as substring match of preferred stripped of '*' using norm; else exact norm match. Then append columns for non-General using mapping; for General use user-supplied only.

I'll create that file. Then reply that file created and mapping refreshed. Provide summary of mapping table. Let's proceed. }
