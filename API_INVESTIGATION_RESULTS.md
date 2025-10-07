# dbt Cloud API v2 Date Filtering Investigation

## Date: October 7, 2025

## Summary
After exhaustive testing with the live dbt Cloud API v2, we confirmed that **date range query parameters are NOT supported** despite being mentioned in documentation.

## Tests Conducted

### 1. Array Parameter Format Tests
Tested multiple ways to pass array parameters:
- ❌ Comma-separated string: `created_at__range=2025-10-04T00:00:00Z,2025-10-05T23:59:59Z`
- ❌ Python list: `created_at__range=['2025-10-04T00:00:00Z', '2025-10-05T23:59:59Z']`
- ❌ Bracket notation: `created_at__range[]=...`
- ❌ Repeated parameters: `created_at__range=...&created_at__range=...`
- ❌ Indexed parameters: `created_at__range[0]=...&created_at__range[1]=...`

**Result**: All formats returned HTTP 400 with validation errors

### 2. Single Boundary Parameter Tests
Tested individual comparison operators:
- ❌ `created_at__gte` (greater than or equal)
- ❌ `created_at__lte` (less than or equal)
- ❌ `finished_at__gte`
- ❌ `finished_at__lte`
- ❌ Combinations of above

**Result**: All returned HTTP 400 with explicit message:
```json
{
  "status": {"code": 400, "user_message": "The request was invalid..."},
  "data": "Additional properties are not allowed ('created_at__gte' was unexpected)"
}
```

### 3. API Version Tests
Checked for alternative API versions:
- ❌ `/api/v3/` endpoints: 404 Not Found
- ❌ `/api/v2/schema`: 404 Not Found
- ❌ `/api/v2/openapi.json`: 404 Not Found

**Result**: Only v2 API exists, no schema discovery available

### 4. Baseline Test
Confirmed API works without date filters:
- ✅ `environment_id` + `limit` parameters work correctly
- ✅ Retrieved 100 runs successfully
- ✅ Data exists in target date ranges (6 runs in Oct 4-5 range)
- ✅ `order_by` parameter works correctly

## API Error Messages

When attempting to use date range parameters, the API consistently returns:

```json
{
  "status": {
    "code": 400,
    "is_success": false,
    "user_message": "The request was invalid. Please double check the provided data and try again.",
    "developer_message": ""
  },
  "data": "Additional properties are not allowed ('created_at__gte', 'created_at__lte' were unexpected)",
  "extra": {},
  "error_code": null
}
```

This explicitly confirms the API schema **does not allow** these parameters.

## Supported Parameters (Confirmed Working)

✅ `environment_id` - Filter by environment
✅ `limit` - Limit number of results  
✅ `order_by` - Sort results (e.g., `-created_at`)
✅ `offset` - Pagination offset

## Conclusion

The dbt Cloud API v2 definitively **does not support date range filtering** via query parameters. 

### Why Documentation Might Show These Parameters

Possible reasons:
1. Documentation refers to a future API version not yet released
2. Documentation describes enterprise/advanced tier features
3. Documentation is outdated or incorrect
4. Parameters exist in internal APIs but not public v2

### Our Implementation: Hybrid Approach

Given API limitations, we implemented an optimal hybrid solution:

**Efficiency Layer** (API-level):
- Use `limit` parameter to reduce payload size
- Use `order_by=-created_at` to get most recent runs first
- Minimize data transfer

**Precision Layer** (Client-side):
- Parse run timestamps locally
- Apply exact date range filters
- Support both `created_at` and `finished_at` filtering
- Enable complex filter combinations

### Performance Characteristics

**Compared to fetching ALL runs and filtering**:
- ✅ Reduced bandwidth via `limit` parameter
- ✅ Faster API responses (smaller payloads)
- ✅ Configurable limit based on needs

**Compared to ideal server-side filtering**:
- ⚠️ Requires fetching more data than needed
- ⚠️ Client-side processing overhead
- ✅ But provides exact filtering not possible otherwise

### Recommendation

**The hybrid approach is the best solution** given current API capabilities:
1. Use reasonable `limit` values (default: 100)
2. Apply precise date filtering client-side
3. Concurrent processing for performance
4. Clear documentation about the approach

## Test Environment

- **API**: dbt Cloud API v2 (cloud.getdbt.com)
- **Account**: 26712
- **Test Date**: October 7, 2025
- **Test Environment**: Production (ID: 379972)
- **Runs Available**: 100+ runs spanning Sept-Oct 2025

## Files

- `test_api_formats.py` - Tests array parameter formats
- `test_api_single_params.py` - Tests single boundary parameters
- Both scripts can be re-run to verify findings

## Recommendation for dbt Labs

If date filtering is needed at scale, consider:
1. Requesting feature addition to API v2
2. Using dbt Cloud Administrative API (if available)
3. Using dbt Cloud Metadata API (GraphQL) which may have different capabilities
4. Implementing cursor-based pagination with timestamps

