# AN-10866 Dashboard Modularization

## Goal

Split the monolithic `AN-10866.py` (910 lines) into four focused files to make the codebase easier to navigate and edit. All existing functionality is preserved exactly; no new features or behavioral changes.

## Target Structure

```
streamlit/
├── AN-10866.py        # entrypoint: page config, sidebar, data load calls, tab rendering
├── config.py          # constants only, no imports
├── queries.py         # all SQL + data-access functions, no UI logic
└── components.py      # reusable Streamlit widgets
```

All files live in the same Snowflake stage so relative imports (`from config import ...`) work without any path manipulation.

## Module Breakdown

### config.py
Pure constants — no imports required.
- `DEALER_LIST` (string)
- `C_BLUE`, `C_GREEN`, `C_AMBER`, `C_PURPLE`
- `ACCEPT_STAFF`

### queries.py
All data access. Imports: `streamlit` (for `@st.cache_data`), `pandas`, `snowflake.snowpark.context`. No Streamlit UI calls.
- `_sql_in_list(values) -> str`
- `common_ctes(ew, days, account_categories, accept_staff, ddi_only, dealer_sizes) -> str`
- `load_account_categories(_session) -> list[str]`
- `load_dealer_sizes(_session) -> list[str]`
- `load_overview(_session, ew, days, ...) -> tuple[DataFrame, DataFrame]`
- `load_performance(_session, ew, days, ...) -> tuple[DataFrame, DataFrame]`
- `load_competitors(_session, ew, days, ...) -> tuple[DataFrame, DataFrame]`
- `load_feature_breakdown(_session, ew, days, ...) -> DataFrame`
- `load_data_freshness(_session) -> DataFrame`
- `load_user_breakdown(_session, dealer_id, product_section, days, engagement_only) -> DataFrame`

### components.py
Reusable UI helpers. Imports: `streamlit`, `pandas`, `altair`, and `queries` (for `load_user_breakdown`).
- `normalize_cols(df) -> DataFrame`
- `trend_chart(df, y_col, color_col, title, color_map) -> alt.Chart`
- `dealer_table(df, pct_cols)`
- `user_breakdown_section(tab_key, product_section, days, df_dealer, session, engagement_only)`
  - **Signature change:** adds `session` and `engagement_only` parameters (previously read as globals).

### AN-10866.py (entrypoint)
Imports from the three modules above. Contains:
- `get_active_session()` call
- `st.set_page_config`
- Sidebar widget code
- Filter state derivation (`engagement_where`, `ddi_filter_active`, etc.)
- Data loading block (`with st.spinner(...)`)
- Tab definitions and all rendering logic

## Key Constraint

`user_breakdown_section` currently closes over `session` and `engagement_only` as module-level globals. Moving it to `components.py` requires passing them explicitly. The call sites in `AN-10866.py` (currently commented out) will be updated to match the new signature.

## What Does Not Change

- All function names and return types
- All SQL queries
- All `@st.cache_data` decorators and their parameters
- All rendering logic, column configs, and chart definitions
- `ACCEPT_STAFF = False` default
- The commented-out `user_breakdown_section` calls remain commented out
