library(digest)
library(dplyr)
library(glue)
library(here)
library(httr2)
library(jsonlite)
library(plumber)
library(quarto)
library(stringr)

source(here("../workflows/libs/db.R")) # define: con

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
  ext <- c(html = "html", pdf = "pdf", docx = "docx")[[format]]
  key <- digest::digest(list(title, areas_json, ver, format))

  out_dir  <- "/share/public/reports"
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  out_file <- file.path(out_dir, paste0(key, ".", ext))
  url_out  <- paste0(
    "https://file.marinesensitivity.org/reports/", key, ".", ext)

  if (!file.exists(out_file)) {
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
