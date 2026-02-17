# target-precoro Configuration

Configuration reference for the Precoro Singer target. Use this to connect the target to the Precoro API and optionally set default field values per stream.

---

## Configuration options

### Authentication

#### `auth_token` (string, required)
API authentication token for Precoro. Sent as `X-AUTH-TOKEN` in request headers.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `email` (string, required)
Email address used for API authentication. Sent in request headers.
- **Example**: `"user@example.com"`

### API Configuration

#### `base_url` (string, optional)
Base URL for the Precoro API. If the value does not start with `https://`, it is prefixed automatically.
- **Default**: `"https://api.precoro.com"`
- **Example**: `"https://api.precoro.com"` or `"api.precoro.com"`

### Default values

#### `default_values` (array, optional)
List of default field values applied to records per stream before sending to the API. Each item must include `stream`, `field`, `value`, and `type`. Only entries whose `stream` matches the current stream name (case-insensitive) are applied.
- **Default**: `[]`
- **Item fields**:
  - `stream` (string): Stream name (e.g. `"invoices"`, `"purchaseorders"`).
  - `field` (string): Field name to set.
  - `value` (string): Value to set (converted according to `type`).
  - `type` (string): One of `"string"`, `"int"`, `"float"`, `"bool"` (or `"boolean"`).
- **Example**: `[{"stream": "invoices", "field": "status", "value": "draft", "type": "string"}]`

### Only update existing records

#### `only_update_existing_records` (array, optional)
When set, records for the listed tables are only updated (PUT); new records are skipped (no POST). Useful to avoid creating duplicates when syncing only changes to existing entities.
- **Default**: `[]`
- **Item fields**:
  - `table` (string): For regular streams, the stream name (e.g. `"invoices"`, `"items"`). For item/document custom fields, the **custom field name** as returned by the API (case-insensitive).
  - `is_dcf` (boolean): `true` for document custom fields; `false` otherwise.
  - `is_icf` (boolean): `true` for item custom fields; `false` otherwise.
- **Example (regular stream)**: `{"table": "items", "is_icf": false, "is_dcf": false}`
- **Example (item custom field)**: `{"table": "My Item Field", "is_icf": true, "is_dcf": false}`
- **Example (document custom field)**: `{"table": "Doc Field Name", "is_icf": false, "is_dcf": true}`

---

## Example: minimal config

Only the required options:

```json
{
  "auth_token": "ntn_xxxxxxxxxxxxxxxxxx",
  "email": "user@example.com"
}
```

---

## Example: complete config

All options with sample values:

```json
{
  "auth_token": "ntn_xxxxxxxxxxxxxxxxxx",
  "email": "user@example.com",
  "base_url": "https://api.precoro.com",
  "default_values": [
    {
      "stream": "invoices",
      "field": "status",
      "value": "draft",
      "type": "string"
    },
    {
      "stream": "purchaseorders",
      "field": "quantity",
      "value": "1",
      "type": "int"
    }
  ],
  "only_update_existing_records": [
    {"table": "items", "is_icf": false, "is_dcf": false},
    {"table": "My Item Field", "is_icf": true, "is_dcf": false}
  ]
}
```
