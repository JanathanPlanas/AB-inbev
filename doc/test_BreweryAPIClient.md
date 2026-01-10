

```md
# Testing Strategy

## Scope

This project currently includes **unit tests** for the `BreweryAPIClient` (API extraction layer).
The goal is to validate the client logic without external dependencies or real API calls.


## What is covered

### 1) Client initialization and configuration
- Default config values are applied when no custom config is provided.
- Custom `APIConfig` overrides are respected.

### 2) Successful responses
- `get_metadata()` returns expected metadata JSON.
- `get_breweries_page()` returns a list of breweries.
- `get_brewery_by_id()` returns the expected brewery record.

### 3) Pagination behavior
- `get_all_breweries()` keeps requesting pages until reaching the last page
  (detected when the page contains fewer records than `per_page`).
- Ensures records from all pages are aggregated correctly.

### 4) Error handling
- Timeouts are converted into a domain exception (`BreweryAPIError`).
- HTTP errors are also converted into `BreweryAPIError`.

### 5) Query parameters / filters
- Ensures filters such as `by_state` and `by_type` are passed correctly in request params.

### 6) Convenience function
- `fetch_all_breweries()` delegates correctly to the client.

## How to run

From the project root:

```bash
pytest -q
