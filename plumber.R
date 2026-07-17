library(DBI)
library(digest)
library(dplyr)
library(duckdb)
library(glue)
library(here)
library(httr2)
library(jsonlite)
library(plumber)
library(quarto)
library(stringr)

# postgres (retired stack) — connect lazily so the API boots without postgis.
# only /species_by_feature uses `con`; it errors at call time if unavailable.
con <- NULL
try(suppressWarnings(source(here("../workflows/libs/db.R"))), silent = TRUE)  # sets `con` if postgis up
if (!inherits(con, "DBIConnection"))
  message("postgres unavailable (retired stack); /species_by_feature disabled")

# obisindicators SQL builders (single source of truth) for the /h3 endpoint.
# source the two R files directly (need only glue) — lighter than installing the
# full package (gsl/h3). taxon.R first: obis_h3t_sql(aphiaid=) calls its helper.
obis_r <- Sys.glob(c(
  "/share/github/marinebon/obisindicators/R/h3t.R",
  "../../marinebon/obisindicators/R/h3t.R",
  "~/Github/marinebon/obisindicators/R/h3t.R"))[1]
if (!is.na(obis_r)) {
  source(file.path(dirname(obis_r), "taxon.R"))
  source(obis_r)
} else {
  message("obisindicators/R not found; /h3 endpoint disabled")
}
OBIS_DUCKDB <- Sys.getenv("MSENS_OBIS_DUCKDB", "/share/data/obis/obis_h3.duckdb")

#* @apiTitle MarineSensitivity Custom API

#* @filter cors
cors <- function(res) {
  res$setHeader("Access-Control-Allow-Origin", "*")
  plumber::forward()
}

# /species_by_feature ----
#* Output a table of species present from one or more spatial feature(s)
#* - The output is in comma-separated format (*.csv).
#* - Species are extracted from [AquaMaps.org](https://www.aquamaps.org) (and more specifically
#* [aquamapsdata](https://raquamaps.github.io/aquamapsdata/articles/intro.html#data-scope-and-content-1)).
#* - For available features on the server by which to query, see spatial **Table Layers** listed at
#* [tile.marinesensitivity.org](https://tile.marinesensitivity.org).
#* @param schema.table The SCHEMA.TABLE containing the feature(s).
#* @param where The WHERE clause selecting the feature(s) as the area of interest.
#* @serializer csv
#* @get /species_by_feature
function(
    schema.table = "raw.mr_eez",
    where        = "mrgid = 8442"){

  # schema.table = "public.ply_ecoareas"
  # where        = "ecoarea_key = 'erCAC-paCEC'"

  schema <- str_split(schema.table, fixed("."))[[1]][1]
  table  <- str_split(schema.table, fixed("."))[[1]][2]

  fld_geom <- dbGetQuery(
    con,
    glue("
      SELECT f_geometry_column
      FROM geometry_columns
      WHERE
        f_table_schema = '{schema}' AND
        f_table_name   = '{table}';")) |>
    pull(f_geometry_column)
  # message(glue("schema.table: '{schema}.{table}'; fld_geom: '{fld_geom}'"))

  q <- glue("
    WITH
      c AS (
        SELECT
        c.cell_id,
        c.area_km2 AS cell_km2,
        CASE
          WHEN ST_CoveredBy(c.geom, a.{fld_geom})
            THEN c.geom
          ELSE
            ST_Multi(ST_Intersection(c.geom, a.{fld_geom}))
        END AS geom
      FROM
        aquamaps.cells c
        INNER JOIN {table} a
          ON ST_Intersects(c.geom, a.{fld_geom})
            AND NOT ST_Touches(c.geom, a.{fld_geom})
      WHERE
        a.{where}
      ORDER BY
        c.cell_id),
    k AS (
      SELECT *,
        ST_AREA(geom::geography) / (1000 * 1000) AS aoi_km2
      FROM c),
    a AS (
      SELECT *,
        aoi_km2 / cell_km2 AS pct_cell
      FROM k),
    s AS (
      SELECT a.*,
        sc.sp_key, sc.probability
      FROM a
        LEFT JOIN aquamaps.spp_cells sc
          ON a.cell_id = sc.cell_id),
    g AS (
      SELECT
        sp_key,
        COUNT(*)                                  AS n_cells,
        AVG(pct_cell)                             AS avg_pct_cell,
        SUM(aoi_km2)                              AS area_km2,
        SUM(probability * aoi_km2) / SUM(aoi_km2) AS avg_suit
      FROM s
      GROUP BY sp_key)
    SELECT
      sp_key, n_cells, avg_pct_cell, area_km2, avg_suit,
      n_cells * avg_pct_cell * avg_suit AS amt,
      phylum, class, \"order\", family, genus, species
    FROM
      g
      LEFT JOIN aquamaps.spp USING (sp_key)")
  # message(q)

  dbGetQuery(con, q)
}

# /tilejson ----
#* Generate tilejson for a given table from pg_tileserv endpoint
#* @param table The table name (e.g., "public.ply_planareas_2025")
#* @param filter Filter using [CQL](https://github.com/CrunchyData/pg_tileserv?tab=readme-ov-file#table-tile-request-customization) `filter=`
#* @param use_cache boolean indicating whether to use the cache (default: TRUE)
#* @serializer unboxedJSON
#* @get /tilejson
function(table = "public.ply_planareas_2025", filter = NULL, use_cache = T) {
  # table = "public.ply_planareas_2025"; use_cache = F

  base_url <- ifelse(
    as.logical(use_cache),
    "https://tilecache.marinesensitivity.org",
    "https://tile.marinesensitivity.org")
  endpoint_url <- glue("{base_url}/{table}.json")

  # fetch data from pg_tileserv endpoint
  j <- request(endpoint_url) |>
    req_perform() |>
    resp_body_json()

  # add filter to the end of j$tileurl
  if (!is.null(filter))
    j$tileurl <- paste0(j$tileurl, "?filter=", URLencode("region_key = 'AK'") )

  # convert postgres types to tilejson types
  convert_pg_type <- function(pg_type) {
    switch(pg_type,
      "text"    = "String",
      "varchar" = "String",
      "char"    = "String",
      "float8"  = "Number",
      "float4"  = "Number",
      "numeric" = "Number",
      "int4"    = "Number",
      "int8"    = "Number",
      "bool"    = "Boolean",
      "String" ) } # default

  # build fields object from properties
  fields <- j$properties |>
    setNames(sapply(j$properties, function(x) x$name)) |>
    lapply(function(prop) convert_pg_type(prop$type))

  # construct tilejson response
  tj <- list(
    tilejson      = "3.0.0",
    name          = paste("Tiles for", table),
    description   = paste("Vector tiles for", table, "from MarineSensitivity tile server"),
    version       = "1.0.0",
    # attribution   = "(c) EcoQuants contributors, CC-BY-SA",
    attribution   = "",
    scheme        = "xyz",
    tiles         = list(j$tileurl),
    minzoom       = j$minzoom,
    maxzoom       = j$maxzoom,
    bounds        = j$bounds,
    center        = j$center,
    vector_layers = list(list(
      id          = table,
      description = paste("Layer for", table),
      fields      = fields ) ) )

  jsonlite::toJSON(tj, auto_unbox = TRUE, pretty = TRUE, null = "null", force = TRUE)

  return(tj)
}

# /echo ----
#* Echo back the input
#* @param msg The message to echo
#* @get /echo
function(msg=""){
  list(msg = paste0("The message is: '", msg, "'"))
}

# /plot ----
#* Plot a histogram
#* @serializer png
#* @get /plot
function(){
  rand <- rnorm(100)
  hist(rand)
}

# /sum ----
#* Return the sum of two numbers
#* @param a The first number to add
#* @param b The second number to add
#* @post /sum
function(a, b){
  as.numeric(a) + as.numeric(b)
}

# prune_cache_dir: LRU eviction for the reports cache ----
# Keeps total on-disk size under `max_bytes` by deleting files with
# the oldest mtime first. Any paths in `keep` (typically the file
# we just wrote or touched) are never deleted. Returns the number
# of files removed.
prune_cache_dir <- function(dir, max_bytes, keep = character(0)) {
  if (!dir.exists(dir)) return(invisible(0L))
  files <- list.files(dir, full.names = TRUE)
  if (length(files) == 0) return(invisible(0L))
  info <- file.info(files)
  info$path <- rownames(info)
  total <- sum(info$size, na.rm = TRUE)
  if (total <= max_bytes) return(invisible(0L))
  info <- info[!info$path %in% keep, , drop = FALSE]
  info <- info[order(info$mtime), , drop = FALSE]
  deleted <- 0L
  for (i in seq_len(nrow(info))) {
    if (total <= max_bytes) break
    if (unlink(info$path[i]) == 0) {
      total   <- total - info$size[i]
      deleted <- deleted + 1L
    }
  }
  if (deleted > 0L)
    message(glue(
      "[prune_cache_dir] removed {deleted} old file(s) from {dir}; ",
      "total now {round(total / 1024^2, 1)} MiB"))
  invisible(deleted)
}

# /report ----
#* Render a parameterized msens report and return a link to the output
#*
#* Expects a JSON body with fields:
#*   - `title` report title (character)
#*   - `ver` data version (e.g. "v6")
#*   - `format` one of "html", "pdf", "docx"
#*   - `areas` array of `{label, kind: "wkt"|"pra", value}`
#*
#* Responses are cached by a hash of the inputs at
#* `/share/public/reports/<hash>.<ext>` and served from
#* `https://file.marinesensitivity.org/reports/<hash>.<ext>`.
#* @serializer unboxedJSON
#* @post /report
function(req, res) {
  body <- tryCatch(
    jsonlite::fromJSON(req$postBody, simplifyVector = FALSE),
    error = function(e) NULL)
  if (is.null(body)) {
    res$status <- 400
    return(list(error = "invalid JSON body"))
  }

  title  <- body$title  %||% "BOEM Marine Sensitivity Report"
  ver    <- body$ver    %||% "v6"
  format <- body$format %||% "html"
  areas  <- body$areas  %||% list()

  if (!format %in% c("html", "pdf", "docx")) {
    res$status <- 400
    return(list(error = glue("unsupported format: {format}")))
  }
  if (length(areas) == 0) {
    res$status <- 400
    return(list(error = "areas must be a non-empty list"))
  }

  areas_json <- jsonlite::toJSON(areas, auto_unbox = TRUE)
  ext   <- c(html = "html", pdf = "pdf", docx = "docx")[[format]]
  key   <- substr(digest::digest(list(title, areas_json, ver, format)), 1, 8)
  fname <- paste0("MarineSensitivity.org_", key, ".", ext)

  out_dir  <- "/share/public/reports"
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  out_file <- file.path(out_dir, fname)
  url_out  <- paste0(
    "https://file.marinesensitivity.org/reports/", fname)

  if (file.exists(out_file)) {
    # cache hit: bump mtime so LRU pruning treats this as recently used
    Sys.setFileTime(out_file, Sys.time())
  } else {
    src <- here()
    tmp <- tempfile("rpt_"); dir.create(tmp)
    for (f in c("report.qmd", "report_area_child.qmd")) {
      src_f <- file.path(src, f)
      if (!file.exists(src_f)) {
        res$status <- 500
        return(list(error = glue(
          "missing report source: {src_f} — ",
          "pull the api repo on the server")))
      }
      file.copy(src_f, file.path(tmp, f))
    }

    api_base <- Sys.getenv(
      "MSENS_API_BASE", "https://api.marinesensitivity.org")
    mapsp_base <- Sys.getenv(
      "MSENS_MAPSP_URL", "https://shiny.marinesensitivity.org/mapsp")
    tryCatch(
      quarto::quarto_render(
        input          = file.path(tmp, "report.qmd"),
        output_format  = format,
        execute_params = list(
          title      = title,
          areas_json = areas_json,
          ver        = ver,
          format     = format,
          api_base   = api_base,
          mapsp_base = mapsp_base)),
      error = function(e) {
        res$status <<- 500
        stop(glue("quarto_render failed: {conditionMessage(e)}"))
      })

    file.copy(
      file.path(tmp, paste0("report.", ext)),
      out_file, overwrite = TRUE)
  }

  # evict oldest reports once total cache size exceeds the limit.
  # MSENS_REPORTS_MAX_MB overrides the default (500 MiB). The file we
  # just wrote / touched is passed as `keep` so it's never evicted by
  # the same request that produced it.
  max_mb <- suppressWarnings(as.numeric(
    Sys.getenv("MSENS_REPORTS_MAX_MB", "500")))
  if (!is.finite(max_mb) || max_mb <= 0) max_mb <- 500
  prune_cache_dir(out_dir, max_mb * 1024^2, keep = out_file)

  list(url = url_out)
}

# /species.csv ----
#*
#* Download the full species list intersecting a Program Area or a drawn
#* polygon, as CSV. Reused by the report's per-area download links.
#*
#* @param ver data version (default "v6")
#* @param kind area kind: "pra" (Program Area) or "wkt" (polygon WKT)
#* @param value programarea_key (for kind=pra) or WKT string (for kind=wkt)
#* @param label optional human-readable label used for the download filename
#* @serializer csv
#* @get /species.csv
function(req, res, ver = "v6", kind = "", value = "", label = "") {
  if (!nzchar(kind) || !nzchar(value)) {
    res$status <- 400
    return(data.frame(error = "missing kind or value"))
  }
  if (!kind %in% c("pra", "wkt")) {
    res$status <- 400
    return(data.frame(error = glue("unsupported kind: {kind}")))
  }

  # build polygon
  ply <- tryCatch(
    {
      if (kind == "pra") {
        dir_data <- switch(
          Sys.info()[["sysname"]],
          "Darwin" = "~/My Drive/projects/msens/data",
          "Linux"  = "/share/data")
        gpkg <- file.path(
          dir_data, "derived", ver,
          paste0("ply_programareas_2026_", ver, ".gpkg"))
        sf::st_read(gpkg, quiet = TRUE) |>
          dplyr::filter(.data$programarea_key == !!value) |>
          sf::st_geometry() |>
          sf::st_as_sf()
      } else {
        sf::st_as_sfc(value, crs = 4326) |>
          sf::st_as_sf()
      }
    },
    error = function(e) NULL)
  if (is.null(ply) || nrow(ply) == 0) {
    res$status <- 404
    return(data.frame(error = glue("area not found: kind={kind}, value={value}")))
  }

  con <- msens::sdm_db_con(version = ver, read_only = TRUE)
  on.exit(try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

  r_cell_id <- msens::cell_id_raster()[["cell_id"]]
  cells     <- msens::cells_in_polygon(ply, r_cell_id)
  spp       <- msens::species_for_cells(con, cells)

  # download filename
  slug <- label
  if (!nzchar(slug)) slug <- value
  slug <- gsub("[^A-Za-z0-9_-]+", "_", slug)
  slug <- gsub("^_+|_+$", "", slug)
  if (!nzchar(slug)) slug <- "species"
  fname <- sprintf("species_%s_%s.csv", slug, ver)
  res$setHeader(
    "Content-Disposition",
    sprintf('attachment; filename="%s"', fname))

  spp
}

# /stats.json ----
#* Current species & zone counts for a data version (default: the deployed `latest`).
#* Used by the docs to inline live numbers based on the current database version.
#* @param ver data version ("latest", "v7", ...)
#* @get /stats.json
#* @serializer unboxedJSON
function(ver = "latest") {
  con_v <- msens::sdm_db_con(version = ver, read_only = TRUE)
  on.exit(try(DBI::dbDisconnect(con_v, shutdown = TRUE), silent = TRUE), add = TRUE)
  q1 <- function(sql) as.integer(DBI::dbGetQuery(con_v, sql)[[1]][1])
  real_ver <- tryCatch(
    basename(dirname(normalizePath(msens::sdm_db_path(ver)))),
    error = function(e) ver)

  # cumulative `is_ok` filter funnel (counts surviving after each gate, in the
  # same order applied in merge_models.qmd), then the spatial Program-Area subset.
  fc <- DBI::dbGetQuery(con_v, "
    WITH t AS (
      SELECT *, (mdl_seq IN (SELECT DISTINCT mdl_seq FROM model_cell WHERE mdl_seq IS NOT NULL)) AS has_cells
      FROM taxon),
    g AS (SELECT
      taxon_id IS NOT NULL AS p1,
      mdl_seq  IS NOT NULL AS p2,
      redlist_code IS DISTINCT FROM 'EX' AS p3,
      (worms_id IS NULL OR worms_is_extinct IS NOT TRUE)  AS p3b,
      (worms_id IS NULL OR worms_is_marine  IS NOT FALSE) AS p4,
      (taxon_authority <> 'worms' OR worms_taxonomic_status IS NULL
         OR worms_taxonomic_status IN ('accepted','alternative representation')) AS p5,
      NOT (taxon_authority='worms' AND COALESCE(sp_cat,'') = 'reptile') AS p5b,
      has_cells AS p6 FROM t)
    SELECT
      count(*) s0, count(*) FILTER (WHERE p1) s1, count(*) FILTER (WHERE p1 AND p2) s2,
      count(*) FILTER (WHERE p1 AND p2 AND p3 AND p3b) s3,
      count(*) FILTER (WHERE p1 AND p2 AND p3 AND p3b AND p4) s4,
      count(*) FILTER (WHERE p1 AND p2 AND p3 AND p3b AND p4 AND p5 AND p5b) s5,
      count(*) FILTER (WHERE p1 AND p2 AND p3 AND p3b AND p4 AND p5 AND p5b AND p6) s6
    FROM g")
  rem <- as.integer(c(fc$s0, fc$s1, fc$s2, fc$s3, fc$s4, fc$s5, fc$s6,
    q1("SELECT count(DISTINCT mdl_seq) FROM zone_taxon WHERE zone_fld='programarea_key'")))
  funnel <- data.frame(
    step      = seq_along(rem),
    filter    = c("Source taxa (all datasets)", "Resolved taxon ID",
                  "Has a merged model (distribution)", "Not extinct", "Marine",
                  "Accepted taxonomy (excl. non-turtle reptiles)",
                  "Mapped to ≥1 cell — valid (is_ok)",
                  "Within BOEM Program Areas (spatial subset)"),
    remaining = rem,
    removed   = as.integer(c(0L, head(rem, -1) - tail(rem, -1))))

  list(
    version                 = real_ver,
    total_taxa              = q1("SELECT count(*) FROM taxon"),
    valid_species           = q1("SELECT count(*) FROM taxon WHERE is_ok"),
    species_full_study_area = q1("SELECT count(DISTINCT mdl_seq) FROM zone_taxon WHERE zone_fld='subregion_key' AND zone_value='FULL'"),
    species_program_areas   = q1("SELECT count(DISTINCT mdl_seq) FROM zone_taxon WHERE zone_fld='programarea_key'"),
    n_program_areas         = q1("SELECT count(*) FROM zone WHERE fld='programarea_key'"),
    n_ecoregions            = q1("SELECT count(*) FROM zone WHERE fld='ecoregion_key'"),
    n_datasets              = q1("SELECT count(*) FROM dataset"),
    n_cells                 = q1("SELECT count(*) FROM cell"),
    funnel                  = funnel,
    updated                 = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
}

# /h3 ----
#* H3-hexagon OBIS summary for a taxon / indicator, any extent, many formats
#*
#* Summarizes the OBIS snapshot to H3 hexagons at a chosen resolution, filtered
#* by taxon, and returns it in the requested spatial/tabular format. Filtering
#* by `aphiaid` includes **all descendant taxa at any rank** (resolved from the
#* WoRMS `taxon` table, e.g. `2688` = Infraorder Cetacea). Runs read-only over
#* the same DuckDB store as the `h3t` tile service; the SQL is built by
#* `obisindicators::obis_h3t_sql()` / `obis_spue_sql()`.
#*
#* @param indicator one of `n` (# records), `es` (ES50), `sp` (richness),
#*   `shannon`. Default `n`.
#* @param aphiaid optional comma-separated WoRMS AphiaID(s); includes every
#*   descendant taxon. Takes precedence over `taxon`.
#* @param effort_aphiaid optional AphiaID(s); when set (with `aphiaid`), returns
#*   the SPUE effort proxy = records(`aphiaid`) / records(`effort_aphiaid`).
#* @param taxon optional `rank:value` (e.g. `class:Aves`); ignored if `aphiaid`.
#* @param level H3 resolution 1-7 (default 3). Higher = finer hexagons.
#* @param bbox optional lon/lat extent `xmin,ymin,xmax,ymax` (default global).
#* @param years optional `min,max` year range.
#* @param format `geojson` | `gpkg` | `csv` | `parquet` | `geoparquet`
#*   (default `geojson`).
#* @serializer contentType list(type = "application/octet-stream")
#* @get /h3
function(req, res, indicator = "n", aphiaid = "", effort_aphiaid = "",
         taxon = "", level = 3, bbox = "", years = "", format = "geojson") {

  fail <- function(status, msg) {
    res$status <- status
    charToRaw(jsonlite::toJSON(list(error = msg), auto_unbox = TRUE))
  }
  if (!exists("obis_h3t_sql"))
    return(fail(500, "obisindicators SQL builders not loaded on server"))
  if (!file.exists(OBIS_DUCKDB))
    return(fail(500, paste("store not found:", OBIS_DUCKDB)))

  if (!indicator %in% c("n", "es", "sp", "shannon"))
    return(fail(400, "indicator must be one of n, es, sp, shannon"))
  fmt <- tolower(format)
  if (!fmt %in% c("geojson", "gpkg", "csv", "parquet", "geoparquet"))
    return(fail(400, "format must be geojson, gpkg, csv, parquet, or geoparquet"))
  level <- suppressWarnings(as.integer(level))
  if (is.na(level) || level < 1 || level > 7)
    return(fail(400, "level must be an integer 1-7"))

  parse_ids <- function(s) {
    if (!nzchar(s)) return(NULL)
    v <- suppressWarnings(as.integer(strsplit(trimws(s), "\\s*,\\s*")[[1]]))
    v <- v[!is.na(v)]
    if (length(v)) v else NULL
  }
  aph <- parse_ids(aphiaid)
  eff <- parse_ids(effort_aphiaid)
  yrs <- if (nzchar(years)) {
    y <- suppressWarnings(as.integer(strsplit(trimws(years), "\\s*,\\s*")[[1]]))
    if (length(y) == 2) y else NULL
  } else NULL
  tx <- NULL
  if (nzchar(taxon) && grepl(":", taxon, fixed = TRUE)) {
    p  <- strsplit(taxon, ":", fixed = TRUE)[[1]]
    tx <- setNames(list(p[2]), p[1])
  }

  # inner SQL (fixed res = level): SPUE if an effort taxon is given, else the
  # chosen indicator over the all-taxa / taxon / aphiaid-subtree store.
  inner <- tryCatch({
    # this endpoint executes the SQL directly (no h3t server) with res baked in
    # via res_placeholder = level, and applies its own bbox filter below
    # (where_bbox). obis_h3t_sql()/obis_spue_sql() emit plain SELECTs — the h3t
    # server's per-tile hex_prune injection does not apply here.
    if (!is.null(eff)) {
      if (is.null(aph))
        stop("effort_aphiaid requires aphiaid (the target/numerator taxon)")
      obis_spue_sql(num_aphiaid = aph, den_aphiaid = eff,
                    res_max = level, res_placeholder = as.character(level))
    } else {
      obis_h3t_sql(indicator = indicator, taxon = tx, aphiaid = aph,
                   years = yrs, res_max = level,
                   res_placeholder = as.character(level))
    }
  }, error = function(e) e)
  if (inherits(inner, "error")) return(fail(400, conditionMessage(inner)))

  where_bbox <- ""
  if (nzchar(bbox)) {
    bb <- suppressWarnings(as.numeric(strsplit(trimws(bbox), "\\s*,\\s*")[[1]]))
    if (length(bb) != 4 || anyNA(bb))
      return(fail(400, "bbox must be xmin,ymin,xmax,ymax (lon/lat)"))
    where_bbox <- glue(
      "WHERE h3_cell_to_lng(cell_id) BETWEEN {bb[1]} AND {bb[3]} ",
      "AND h3_cell_to_lat(cell_id) BETWEEN {bb[2]} AND {bb[4]}")
  }

  dbc <- tryCatch(
    DBI::dbConnect(duckdb::duckdb(), dbdir = OBIS_DUCKDB, read_only = TRUE),
    error = function(e) e)
  if (inherits(dbc, "error")) return(fail(500, conditionMessage(dbc)))
  on.exit(try(DBI::dbDisconnect(dbc, shutdown = TRUE), silent = TRUE), add = TRUE)

  out <- tryCatch({
    DBI::dbExecute(dbc,
      "INSTALL h3 FROM community; LOAD h3; INSTALL spatial; LOAD spatial;")
    DBI::dbExecute(dbc, glue("CREATE TEMP TABLE _q AS {inner}"))

    # cast value->DOUBLE, n->BIGINT: SUM() yields HUGEINT, which the GDAL
    # (GeoJSON/GPKG) writer rejects ("decimal precision > 19").
    geo_sel <- glue(
      "SELECT h3_h3_to_string(cell_id) AS h3_id, ",
      "value::DOUBLE AS value, n::BIGINT AS n, ",
      "ST_GeomFromText(h3_cell_to_boundary_wkt(cell_id)) AS geometry ",
      "FROM _q {where_bbox}")
    flat_sel <- glue(
      "SELECT h3_h3_to_string(cell_id) AS h3_id, ",
      "h3_cell_to_lng(cell_id) AS lng, h3_cell_to_lat(cell_id) AS lat, ",
      "value::DOUBLE AS value, n::BIGINT AS n, ",
      "h3_cell_to_boundary_wkt(cell_id) AS geom_wkt ",
      "FROM _q {where_bbox}")

    ext <- c(geojson = "geojson", gpkg = "gpkg", csv = "csv",
             parquet = "parquet", geoparquet = "parquet")[[fmt]]
    tmp <- tempfile(fileext = paste0(".", ext))
    copy_sql <- switch(fmt,
      csv        = glue("COPY ({flat_sel}) TO '{tmp}' (FORMAT CSV, HEADER)"),
      parquet    = glue("COPY ({flat_sel}) TO '{tmp}' (FORMAT PARQUET)"),
      geoparquet = glue("COPY ({geo_sel})  TO '{tmp}' (FORMAT PARQUET)"),
      geojson    = glue("COPY ({geo_sel})  TO '{tmp}' (FORMAT GDAL, DRIVER 'GeoJSON', SRS 'EPSG:4326')"),
      gpkg       = glue("COPY ({geo_sel})  TO '{tmp}' (FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:4326')"))
    DBI::dbExecute(dbc, copy_sql)
    list(tmp = tmp, ext = ext)
  }, error = function(e) e)
  if (inherits(out, "error")) return(fail(400, conditionMessage(out)))

  tag <-
    if (!is.null(eff))       sprintf("spue_%s_over_%s", aph[1], eff[1]) else
    if (!is.null(aph))       paste0("aphia", aph[1])                    else
    if (!is.null(tx))        paste0(names(tx), "-", tx[[1]])            else indicator
  fname <- sprintf("obis_h3_%s_res%d.%s",
                   gsub("[^A-Za-z0-9_-]+", "_", tag), level, out$ext)
  res$setHeader("Content-Disposition",
                sprintf('attachment; filename="%s"', fname))
  readBin(out$tmp, "raw", n = file.info(out$tmp)$size)
}

# / home ----
#* redirect to the swagger interface
#* @get /
#* @serializer html
function(req, res) {
  res$status <- 303 # redirect
  res$setHeader("Location", "./__swagger__/")
  "<html>
  <head>
    <meta http-equiv=\"Refresh\" content=\"0; url=./__swagger__/\" />
  </head>
  <body>
    <p>For documentation on this API, please visit <a href=\"http://api.ships4whales.org/__swagger__/\">http://api.ships4whales.org/__swagger__/</a>.</p>
  </body>
</html>"
}
