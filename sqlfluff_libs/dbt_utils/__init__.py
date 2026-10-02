"""Lint-only stand-ins for dbt_utils macros, used by sqlfluff's jinja templater.

They only need to render syntactically valid SQL; dbt uses the real package at build time.
"""


def generate_surrogate_key(field_list):
    return "md5(" + " || '-' || ".join(f"coalesce(cast({f} as varchar), '')" for f in field_list) + ")"
