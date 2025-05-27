
## 2025-05-27.a 

Add an API endpoint `/tilejson` with R [plumber](https://www.rplumber.io) to @plumber.R 
that accepts a table name (e.g., "public.ply_planareas_2025") as the input 
argument and returns a proper tilejson (see @public.ply_planareas_2025_tilejson.json)
for vector tile display by extracting and re-purposing all available
information from the Postgres Tile Server pg_tileserv endpoint for the table (e.g., at 
https://tile.marinesensitivity.org/public.ply_planareas_2025.json; see 
@public.ply_planareas_2025.json for output) using the 
[httr2](https://httr2.r-lib.org) R package.

### Response

The /tilejson endpoint has been added to plumber.R:100-158. It fetches data from 
the pg_tileserv endpoint, converts PostgreSQL field types to tilejson format, and 
returns a properly structured tilejson response that matches the expected format.
