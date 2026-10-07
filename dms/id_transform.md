Yes, DMS can handle most of this -- but with limitations

DMS transformation rules use a SQLite expression engine that supports `CASE WHEN`, `UPPER()`, `REPLACE()`, `glob` pattern matching, and `hash_sha256()`. This covers most of what they need.

Source: [Using transformation rule expressions to define column content](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.Expressions.html)

### What DMS CAN do

| Requirement | DMS function | Supported? |
|------------|-------------|-----------|
| Uppercase | `upper($identity_field)` | Yes |
| Pattern match HKID | `glob` (SQLite pattern matching) | Partial -- see below |
| Pattern match CNID | `glob` | Partial |
| Remove `()` | `replace(replace($x, '(', ''), ')', '')` | Yes |
| SHA-256 | `hash_sha256($x)` | Yes |
| Conditional on id_type | `CASE WHEN $id_type = 'hkid' THEN ... END` | Yes |
| Empty on mismatch | `CASE WHEN ... THEN '' ELSE ... END` | Yes |

AWS published an almost identical use case (SSN masking with pattern matching + SHA-256): [Data masking using AWS DMS](https://aws.amazon.com/blogs/database/data-masking-using-aws-dms/)

### The limitation: `glob` pattern matching

`glob` is SQLite's pattern matcher. It supports `*` (any chars), `?` (one char), and `[0-9]` (character classes). But it's NOT full regex. Here's the gap:

**HKID format** (e.g. `A123456(7)` or `AB123456(7)`):
- Pattern: 1-2 uppercase letters + 6 digits + `(` + 1 digit + `)`
- `glob` attempt: `'[A-Z][0-9][0-9][0-9][0-9][0-9][0-9]([0-9])'` -- this works for the 1-letter prefix
- Problem: the 1-OR-2 letter prefix is hard to express in a single glob. You'd need two patterns OR'd together.

**CNID format** (18 digits, last char can be digit or X):
- Pattern: 17 digits + (1 digit or X)
- `glob` attempt: `'[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9X]'` -- this works but is ugly

### Draft rule

Here's a working rule. It uses `add-column` to create a new hashed column, then `remove-column` to drop the original:

```json
{
  "rules": [
    {
      "rule-type": "selection",
      "rule-id": "1",
      "rule-name": "1",
      "object-locator": {
        "schema-name": "public",
        "table-name": "your_table"
      },
      "rule-action": "include"
    },
    {
      "rule-type": "transformation",
      "rule-id": "2",
      "rule-name": "hash-identity-field",
      "rule-action": "add-column",
      "rule-target": "column",
      "object-locator": {
        "schema-name": "public",
        "table-name": "your_table"
      },
      "value": "identity_hashed",
      "expression": "CASE WHEN $id_type = 'hkid' AND (upper($identity_field) glob '[A-Z][0-9][0-9][0-9][0-9][0-9][0-9]([0-9])' OR upper($identity_field) glob '[A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9]([0-9])') THEN hash_sha256(replace(replace(upper($identity_field), '(', ''), ')', '')) WHEN $id_type = 'cnid' AND upper($identity_field) glob '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9X]' THEN hash_sha256(upper($identity_field)) ELSE '' END",
      "data-type": {
        "type": "string",
        "length": 64
      }
    },
    {
      "rule-type": "transformation",
      "rule-id": "3",
      "rule-name": "remove-original-identity",
      "rule-action": "remove-column",
      "rule-target": "column",
      "object-locator": {
        "schema-name": "public",
        "table-name": "your_table",
        "column-name": "identity_field"
      }
    }
  ]
}
```

### PS

The logic works in DMS transformation rules, with these caveats:

1. **Pattern matching is `glob`, not regex.** The HKID 1-or-2 letter prefix needs two patterns OR'd. More complex validation (e.g., HKID check digit calculation) is not possible in DMS expressions.

2. **The expression runs per-row during replication.** It applies to both full load and CDC. No performance concern for the hash itself, but the long CASE expression should be tested to confirm `glob` matching at their throughput.

3. **`hash_sha256` output is a 64-char hex string.** Target column needs `length: 64` minimum.
