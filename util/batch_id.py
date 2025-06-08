#!/usr/bin/env python3

"""
Utility functions for batch id management.
"""

def get_batch_id() -> int:
    with open("batch", "r") as f:
        return int(f.read())

def set_batch_id(batch_id: int):
    with open("batch", "w") as f:
        f.write(str(batch_id))