del from main_table as a where a._key in {{ block_output("join_table", parse=lambda data, _vars:  tuple(data["key"])) }}
