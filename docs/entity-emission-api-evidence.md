# OpenMetadata entity-emission API reference

HTTP payloads the sidecar sends when `--auto-create-entities` is enabled.
If OpenMetadata rejects a create with HTTP 400 at preflight, compare
what your OM version expects against the minimal bodies documented here,
then file an issue with the OM version and the 400 response body.

## Assumptions baked into the shipped code

1. `POST /v1/databases`, `POST /v1/databaseSchemas`, and `POST /v1/tables`
   all accept **name-string references** in the `service` / `database` /
   `databaseSchema` fields (no `{id, type}` entity reference required).
2. `POST /v1/tables` accepts `columns: []` — an empty column array is a
   valid body. The sidecar never invents columns; column-level enrichment
   is left to a real ingestion connector.
3. `409 Conflict` on create indicates the entity already exists under the
   same FQN and can be re-fetched via `GET /v1/{type}/name/{fqn}`.
4. `PUT /v1/lineage` with `columnsLineage` entries referencing unknown
   column FQNs either silently drops them or returns 400. The sidecar
   filters unknown-column pairs unconditionally before emission so either
   behavior is safe.

The runtime **preflight probe** (executing the first planned write on
every live run) re-validates #1–#3 and fails fast with remediation text
if any assumption breaks. That guarantees the sidecar never pushes bad
bodies at volume.

## Minimal request bodies

### `POST /v1/databases`

```http
POST /api/v1/databases
Authorization: Bearer <bot-jwt>
Content-Type: application/json

{ "name": "SalesDB", "service": "mssql_prod" }
```

Expected success: `201` with an entity body carrying `id` and
`fullyQualifiedName: "mssql_prod.SalesDB"`.

### `POST /v1/databaseSchemas`

```http
POST /api/v1/databaseSchemas
Authorization: Bearer <bot-jwt>
Content-Type: application/json

{ "name": "dbo", "database": "mssql_prod.SalesDB" }
```

### `POST /v1/tables`

```http
POST /api/v1/tables
Authorization: Bearer <bot-jwt>
Content-Type: application/json

{
  "name": "customers",
  "databaseSchema": "mssql_prod.SalesDB.dbo",
  "columns": []
}
```

If an OM version rejects `columns: []`, the preflight probe will surface
a 400 with the validator message, and the sidecar aborts before any
catalog writes. There is no placeholder-column mode — adding synthesized
columns to an admin-curated catalog is out of scope by design.

### Conflict handling

On `409 Conflict` from any of the three endpoints, the sidecar issues a
follow-up `GET /api/v1/{type}/name/{fqn}` to re-fetch the existing
entity and treats it as existing (not created).

### Case-insensitive lookup

Before creating anything, each endpoint FQN is resolved against OM by:

1. Exact FQN `GET /api/v1/{type}/name/{fqn}`.
2. On 404, a fallback call to the search API, picking the best
   case-insensitive FQN match.

The fallback is what keeps the sidecar from duplicating entities when
SQLFlow uppercased identifiers for a case-insensitive source (MSSQL,
BigQuery) but OM stored them in the source system's original case.
