"""Tests for the dbobjs-resolved qualified-name path in lineage_mapper.

Verifies that the id-resolved FQN is preferred when the tree is populated,
placeholder-only trees fall back to bare ``parentName`` (so status-quo users
see byte-identical output), and partial placeholder trees strip only the
placeholder segments.
"""

import copy

from gsp_openmetadata_sidecar.lineage_mapper import (
    _build_id_to_fqn,
    _qualified_parent_name,
    extract_lineage,
)

from tests.fixtures.sqlflow_responses import (
    placeholder_db_real_server_and_schema_response,
    placeholder_schema_real_db_response,
    placeholder_schema_real_server_and_db_response,
    placeholder_tree_response,
    populated_tree_response,
    real_db_and_schema_only_response,
)


def _normalize(lineages):
    """Compare lineages order-insensitively with column mappings as sets."""
    return sorted(
        (tl.upstream_table, tl.downstream_table, frozenset(tl.column_mappings))
        for tl in lineages
    )


def test_mapper_id_resolved_fqn_when_tree_populated():
    response = populated_tree_response()
    lineages = extract_lineage(response, db_vendor="dbvmssql")
    assert len(lineages) == 1
    tl = lineages[0]
    assert tl.upstream_table == "srv01.SalesDB.dbo.Customers"
    assert tl.downstream_table == "srv01.SalesDB.dbo.v"
    assert ("id", "id") in tl.column_mappings


def test_mapper_falls_back_to_parent_name_for_placeholder_tree():
    """Status-quo behavior: placeholder-only tree => bare parentName lineage."""
    response = placeholder_tree_response()
    lineages = extract_lineage(response, db_vendor="dbvmssql")
    assert len(lineages) == 1
    tl = lineages[0]
    # parentName casing is preserved (SQLFlow uppercases it in this fixture).
    assert tl.upstream_table == "CUSTOMERS"
    assert tl.downstream_table == "V"


def test_mapper_output_identical_with_or_without_dbobjs_tree():
    """Placeholder tree and no-tree must produce the same lineage — the
    backstop invariant for users who don't set the new defaults."""
    with_tree = extract_lineage(placeholder_tree_response(), db_vendor="dbvmssql")

    no_tree_response = placeholder_tree_response()
    no_tree_response["data"].pop("dbobjs")
    without_tree = extract_lineage(no_tree_response, db_vendor="dbvmssql")

    assert _normalize(with_tree) == _normalize(without_tree)


def test_build_id_to_fqn_emits_three_part_name_when_db_and_schema_real():
    """Placeholder server but real db+schema: emit ``db.schema.T`` so
    _build_fqn treats it as 3-part (db, schema, table) correctly."""
    mapping = _build_id_to_fqn(real_db_and_schema_only_response())
    assert mapping["4"] == "SalesDB.dbo.T"
    assert mapping["10"] == "SalesDB.dbo.out"


def test_build_id_to_fqn_skips_when_schema_is_placeholder_even_with_real_db():
    """Real db + placeholder schema: emitting ``SalesDB.T`` would be misread
    by _build_fqn (schema=SalesDB). Must skip and fall back to parentName."""
    mapping = _build_id_to_fqn(placeholder_schema_real_db_response())
    assert mapping == {}


def test_build_id_to_fqn_skips_when_db_is_placeholder_even_with_real_server_and_schema():
    """Real server + schema but placeholder db: emitting ``srv01.dbo.T``
    would be misread by _build_fqn (db=srv01). Must skip."""
    mapping = _build_id_to_fqn(placeholder_db_real_server_and_schema_response())
    assert mapping == {}


def test_build_id_to_fqn_skips_when_schema_is_placeholder_even_with_real_server_and_db():
    """Real server + db but placeholder schema: emitting ``srv01.SalesDB.T``
    would be misread by _build_fqn (db=srv01, schema=SalesDB). Must skip."""
    mapping = _build_id_to_fqn(placeholder_schema_real_server_and_db_response())
    assert mapping == {}


def test_mapper_ambiguous_tree_falls_back_to_parent_name():
    """End-to-end check: the ambiguity cases above must produce the same
    lineage as if no dbobjs tree were present."""
    for response_fn in (
        placeholder_schema_real_db_response,
        placeholder_db_real_server_and_schema_response,
        placeholder_schema_real_server_and_db_response,
    ):
        lineages = extract_lineage(response_fn(), db_vendor="dbvmssql")
        assert len(lineages) == 1
        # parentName values — not the tree's case-preserved names.
        assert lineages[0].upstream_table == "T"
        assert lineages[0].downstream_table == "OUT"


def test_build_id_to_fqn_skips_entries_with_only_placeholder_prefixes():
    """If every segment above the entity is a placeholder, no map entry is
    produced — forcing the caller to fall back to ``parentName`` and keep the
    pre-existing casing (SQLFlow uppercases parentName; dbobjs preserves case)."""
    response = placeholder_tree_response()
    mapping = _build_id_to_fqn(response)
    assert "4" not in mapping
    assert "10" not in mapping


def test_qualified_parent_name_missing_tree_entry_falls_back():
    """Nodes whose parentId is not in the tree (intermediates like RS-*) must
    resolve to their bare parentName."""
    mapping = {"4": "srv01.SalesDB.dbo.Customers"}
    intermediate = {"parentName": "RS-1", "parentId": 7, "column": "id"}
    assert _qualified_parent_name(intermediate, mapping) == "RS-1"


def test_qualified_parent_name_no_parent_id_falls_back():
    mapping = {"4": "srv01.SalesDB.dbo.Customers"}
    node = {"parentName": "T", "column": "id"}
    assert _qualified_parent_name(node, mapping) == "T"


def test_mapper_preserves_existing_intermediate_prefix_behavior():
    """RS-* intermediates still resolve through to real sources — the dbobjs
    rewrite must not break the transitive resolution path."""
    response = populated_tree_response()
    # Add a second intermediate hop to make sure the reverse_map still keys
    # on qualified names consistently.
    relationships = response["data"]["relationships"]
    relationships.append(
        {
            "type": "fdd",
            "effectType": "select",
            "target": {"parentName": "RS-2", "parentId": 8, "column": "id"},
            "sources": [
                {"parentName": "RS-1", "parentId": 7, "column": "id"}
            ],
        }
    )
    # And switch the view source to RS-2.
    relationships[1]["sources"] = [
        {"parentName": "RS-2", "parentId": 8, "column": "id"}
    ]
    lineages = extract_lineage(response, db_vendor="dbvmssql")
    assert len(lineages) == 1
    assert lineages[0].upstream_table == "srv01.SalesDB.dbo.Customers"
    assert lineages[0].downstream_table == "srv01.SalesDB.dbo.v"


def test_mapper_does_not_mutate_response():
    """Defensive: extract_lineage should not alter the dict it's handed."""
    original = populated_tree_response()
    snapshot = copy.deepcopy(original)
    extract_lineage(original, db_vendor="dbvmssql")
    assert original == snapshot
