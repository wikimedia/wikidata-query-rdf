### Version 2.0.0
 - Start using /fragment/mediawiki/revision/common/3.0.0# (add top-level dt field)
    - introduce the event-time dt field (mandatory)
    - strengthen the schema with additionalProperties: false
 - Declared rev_content_changed explicitly

### Version 1.1.0
 - Added `rev_is_revert` and `rev_revert_details` fields. `rev_revert_details` consists of `rev_revert_method`, `rev_is_exact_revert`, `rev_reverted_revs` and `rev_original_rev_id`.

### Version 1.0.0
 - Updated to draft-07 new schema conventions.

### Version 4
 - Added `chronology_id` field.

### Version 3
 - Added `minLength: 1` for required string properties.

### Version 2
 - Added `parsedcomment` optional field.
