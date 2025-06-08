#!/usr/bin/env python3

"""
Utility functions for ETL.
"""

from datetime import datetime
import sqlalchemy
from sqlalchemy import or_
from sqlalchemy.orm import class_mapper, DeclarativeBase

pk_remaps = {
    'pilots': {
        'pilot_id': 'id',
    },
    'cabin_crew': {
        'cabin_crew_id': 'id',
    },
    'customers': {
        'customer_id': 'id',
    },
    'airports': {
        'airport_id': 'id',
    },
    'airplanes': {
        'airplane_id': 'id',
    },
    'flights': {
        'flight_id': 'id',
    },
    'flight_cabin_crew': {
        'flight_cabin_crew_id': 'id',
    },
    'flight_bookings': {
        'flight_booking_id': 'id',
    },
    'airline_reviews': {}
}


#used for scd t2
def generate_diff_condition(
        warehouse_alias: sqlalchemy.Table,
        operational_alias: sqlalchemy.Table,
        exclude_cols: set[str] = set(),
):
    base_exclude_cols = {"start_date", "end_date","insert_id", "update_id", "source_id"}
    exclude_cols = exclude_cols | base_exclude_cols

    assert isinstance(warehouse_alias, sqlalchemy.Table), "warehouse_alias must be a sqlalchemy.Table"
    assert isinstance(operational_alias, sqlalchemy.Table), "operational_alias must be a sqlalchemy.Table"

    # conditions = []

    # goal: if wh represents id as e.g. pilot_id, and op represents id as id, then we need to remap the columns
    # keeping pks is not necessary, but other columns may be called the same, which may represent fks, and those
    # must be kept in mind to keep scd working
    comparison_cols = []

    for wh_col in warehouse_alias.columns.keys():
        if wh_col not in exclude_cols:
            if wh_col in pk_remaps[warehouse_alias.name]:
                op_col = operational_alias.c[pk_remaps[warehouse_alias.name][wh_col]]
            else:
                op_col = operational_alias.c[wh_col]

            comparison_cols.append((warehouse_alias.c[wh_col], op_col))

    # conditions = [
    #     warehouse_alias.c[col] != operational_alias.c[col]
    #     for col in warehouse_alias.columns.keys()
    #     if col not in exclude_cols
    # ]

    conditions = [
        wh_col != op_col
        for wh_col, op_col in comparison_cols
    ]

    return or_(*conditions) if conditions else None

def create_warehouse_insert_stmt(
        insert_id: int,
        source_id: int,
        base_select: sqlalchemy.Select,
        warehouse_table: sqlalchemy.Table,
):
    # define constants
    insert_id_l = sqlalchemy.literal(insert_id).label("insert_id")
    # update_id_l = sqlalchemy.text("NULL as update_id")
    update_id_l = sqlalchemy.null().label("update_id")
    # update_id_l = sqlalchemy.cast(sqlalchemy.null(), sqlalchemy.Integer).label("update_id")
    # update_id_l = sqlalchemy.literal_column("NULL").label("update_id")
    source_id_l = sqlalchemy.literal(source_id).label("source_id")
    start_date_l = sqlalchemy.literal(datetime.now()).label("start_date")
    end_date_l = sqlalchemy.literal(datetime.max).label("end_date")

    select_stmt = base_select.add_columns(
        insert_id_l,
        update_id_l,
        source_id_l,
        start_date_l,
        end_date_l
    )

    names = [
        *base_select.selected_columns,
        warehouse_table.columns.get('insert_id'),
        warehouse_table.columns.get('update_id'),
        warehouse_table.columns.get('source_id'),
        warehouse_table.columns.get('start_date'),
        warehouse_table.columns.get('end_date'),
    ]
    
    insert_stmt = sqlalchemy.insert(warehouse_table).from_select(
        names,
        select_stmt,
    )

    return insert_stmt