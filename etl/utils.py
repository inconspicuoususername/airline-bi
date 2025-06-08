import sqlalchemy
from sqlalchemy import or_
from sqlalchemy.orm import class_mapper

def generate_diff_condition(
        warehouse_alias: sqlalchemy.Table,
        operational_alias: sqlalchemy.Table,
        exclude_cols: set[str] = set(),
):
    exclude_cols = exclude_cols or {"start_date", "end_date", "pilot_sk", "insert_id", "update_id"}

    assert isinstance(warehouse_alias, sqlalchemy.Table), "warehouse_alias must be a sqlalchemy.Table"
    assert isinstance(operational_alias, sqlalchemy.Table), "operational_alias must be a sqlalchemy.Table"

    conditions = []
    for prop in class_mapper(type(warehouse_alias)).iterate_properties:
        if not hasattr(prop, "columns"):
            continue  # Skip relationships or hybrid properties
        col_name = prop.key
        if col_name in exclude_cols:
            continue
        wh_col = getattr(warehouse_alias, col_name)
        op_col = getattr(operational_alias, col_name)
        conditions.append(wh_col != op_col)
    
    return or_(*conditions) if conditions else None