"""Synthetic SQLFlow responses for lineage-mapper tests.

Each response models the output of::

    CREATE VIEW v AS SELECT id FROM Customers

under different parse-time defaults.
"""


def populated_tree_response() -> dict:
    """defaultServer/Database/Schema were supplied, so the dbobjs tree
    carries the fully qualified names."""
    return {
        "code": 200,
        "data": {
            "dbobjs": {
                "servers": [
                    {
                        "name": "srv01",
                        "databases": [
                            {
                                "name": "SalesDB",
                                "schemas": [
                                    {
                                        "name": "dbo",
                                        "tables": [
                                            {"id": 4, "name": "Customers"},
                                        ],
                                        "views": [
                                            {"id": 10, "name": "v"},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "relationships": [
                {
                    "type": "fdd",
                    "effectType": "select",
                    "target": {"parentName": "RS-1", "parentId": 7, "column": "id"},
                    "sources": [
                        {"parentName": "CUSTOMERS", "parentId": 4, "column": "id"}
                    ],
                },
                {
                    "type": "fdd",
                    "effectType": "create_view",
                    "target": {"parentName": "V", "parentId": 10, "column": "id"},
                    "sources": [
                        {"parentName": "RS-1", "parentId": 7, "column": "id"}
                    ],
                },
            ],
        },
    }


def placeholder_tree_response() -> dict:
    """No defaults supplied, so every segment above the entity is a SQLFlow
    placeholder (``DEFAULT_SERVER`` / ``DEFAULT``).

    Relationships use the same bare ``parentName`` values as the populated
    variant — the only difference is the tree, so the mapper must fall back
    to bare ``parentName`` here."""
    return {
        "code": 200,
        "data": {
            "dbobjs": {
                "servers": [
                    {
                        "name": "DEFAULT_SERVER",
                        "databases": [
                            {
                                "name": "DEFAULT",
                                "schemas": [
                                    {
                                        "name": "DEFAULT",
                                        "tables": [
                                            {"id": 4, "name": "Customers"},
                                        ],
                                        "views": [
                                            {"id": 10, "name": "v"},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "relationships": [
                {
                    "type": "fdd",
                    "effectType": "select",
                    "target": {"parentName": "RS-1", "parentId": 7, "column": "id"},
                    "sources": [
                        {"parentName": "CUSTOMERS", "parentId": 4, "column": "id"}
                    ],
                },
                {
                    "type": "fdd",
                    "effectType": "create_view",
                    "target": {"parentName": "V", "parentId": 10, "column": "id"},
                    "sources": [
                        {"parentName": "RS-1", "parentId": 7, "column": "id"}
                    ],
                },
            ],
        },
    }


def _mini_response(server: str, database: str, schema: str) -> dict:
    """Build a one-relationship response with the tree prefixes under test."""
    return {
        "code": 200,
        "data": {
            "dbobjs": {
                "servers": [
                    {
                        "name": server,
                        "databases": [
                            {
                                "name": database,
                                "schemas": [
                                    {
                                        "name": schema,
                                        "tables": [{"id": 4, "name": "T"}],
                                        "views": [{"id": 10, "name": "out"}],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            "relationships": [
                {
                    "type": "fdd",
                    "effectType": "create_view",
                    "target": {"parentName": "OUT", "parentId": 10, "column": "a"},
                    "sources": [
                        {"parentName": "T", "parentId": 4, "column": "a"}
                    ],
                },
            ],
        },
    }


def real_db_and_schema_only_response() -> dict:
    """Placeholder server, real database, real schema. Valid — emits 3-part."""
    return _mini_response("DEFAULT_SERVER", "SalesDB", "dbo")


def placeholder_schema_real_db_response() -> dict:
    """Real database but placeholder schema — ambiguous, must skip."""
    return _mini_response("DEFAULT_SERVER", "SalesDB", "DEFAULT")


def placeholder_db_real_server_and_schema_response() -> dict:
    """Real server and schema but placeholder database — ambiguous, must skip."""
    return _mini_response("srv01", "DEFAULT", "dbo")


def placeholder_schema_real_server_and_db_response() -> dict:
    """Real server and database but placeholder schema — ambiguous, must skip."""
    return _mini_response("srv01", "SalesDB", "DEFAULT")
