###
### @P4COPYRIGHT_SINCE:2022@
###

#'
#' @file httpapi.R
#' @brief Implementation layer for calls to the shim-less SciDB HTTP API
#'    available in SciDB 22.6 and later.
#'

#' We support these versions of the client API.
httpapi_supported_versions <- c(1)

#' @see GetServerVersion()
#' @noRd
GetServerVersion.httpapi <- function(db)
{
  conn <- attr(db, "connection")
  
  ## Clone the connection and unset the username/password, because:
  ## (1) the /version endpoint doesn't need a username/password;
  ## (2) the /version endpoint doesn't perform authentication or return cookies;
  ## (3) if we passed a username/password through .HttpRequest, it would 
  ##     unset conn's password because it assumes the request will
  ##     authenticate and return cookies.
  conn_clone <- rlang::env_clone(conn)
  conn_clone$username <- NULL
  conn_clone$password <- NULL
  resp <- .HttpRequest(conn_clone, "GET", .EndpointUri(db, "version"))
  stopifnot(resp$status_code == 200)  # 200 OK
  json <- jsonlite::fromJSON(rawToChar(resp$content))
  
  conn$scidb.version <- ParseVersion(json[["scidbVersion"]])

  ## The API versions that the server supports
  server_api_versions <- json[["apiMinVersion"]]:json[["apiVersion"]]
  ## The API versions that this client and the server both support
  compatible_versions <- intersect(server_api_versions, 
                                   httpapi_supported_versions)
  if (length(compatible_versions) == 0) {
    stop("No compatible API versions found with this server.",
         " The server supports versions ", server_api_versions, 
         " and I support versions ", httpapi_supported_versions)
  }
  ## The newest API version that both the client and server support
  conn$httpapi_version <- max(compatible_versions)
  
  ## If we got this far, the request succeeded - so the connected host and port
  ## supports the SciDB httpapi (no Shim).
  class(conn) <- "httpapi"
  class(db) <- c("httpapi", "afl")
  
  ## We need to return db because we have modified its class,
  ## and db is pass-by-value (it isn't an env)
  return(db)
}

#' @see NewSession()
#' @param db_or_conn a database connection object, or a connection env
#' @noRd
NewSession.httpapi <- function(db_or_conn, ...)
{
  conn <- .GetConnectionEnv(db_or_conn)
  if (is.null(conn)) stop("no connection environment")
  msg.trace("NewSession.httpapi(",
            jsonlite::toJSON(as.list.environment(conn)), ")")
  
  if (!at_least(conn$scidb.version, 22.6)) {
    stop("HTTP API is only supported for SciDB server version 22.6 and later;",
         " this server has version ", conn$scidb.version)
  }

  ## Post to /api/sessions to create a new session  
  resp <- .HttpRequest(conn, "POST", .EndpointUri(conn, "sessions"))
  stopifnot(resp$status_code == 201)  # 201 Created
  json <- jsonlite::fromJSON(rawToChar(resp$content))
  
  ## Response JSON contains the session ID
  sid <- json[["sid"]]

  ## Parse the Location and Link headers and store them
  headers_list <- curl::parse_headers_list(resp$headers)
  conn$location <- headers_list[["location"]]
  conn$links <- .ParseLinkHeaders(headers_list)
  
  ## Give the connection the session ID.
  conn$session <- sid
  ## Give the connection a unique ID - in this case just reuse the session ID.
  ## But since temporary array names contain the connection ID,
  ## replace all non-identifier characters with underscores.
  conn$id <- make.names_(sid)

  ## Register the session to be closed when the connection env
  ## gets garbage-collected.
  reg.finalizer(conn, .CloseSession.httpapi, onexit=TRUE)

  msg.trace("NewSession.httpapi() created session id ", sid)
  
  ## We don't need to return db or conn because the only object we have modified
  ## is the connection env which is pass-by-reference.
}

#' @see EnsureSession()
#' @noRd
EnsureSession.httpapi <- function(db_or_conn, ...)
{
  conn <- .GetConnectionEnv(db_or_conn)
  if (is.null(conn) || is.null(conn$session) || is.null(conn$links)
      || is.null(conn$location)) {
    NewSession.httpapi(db_or_conn)
  }
}

#' @see Reauthenticate()
#' @noRd
Reauthenticate.httpapi <- function(db, password, defer=FALSE)
{
  conn <- .GetConnectionEnv(db)
  if (is.null(conn)) stop("no connection environment")
  if (!has.chars(password)) stop("no password provided")
  
  conn$password <- password
  
  if (!defer) {
    ## Run an arbitrary query to re-activate the connection
    ## with the new password
    iquery(db, "show('help()' , 'afl')", return=TRUE)
  }
}

#' @see Close()
#' @noRd
Close.httpapi <- function(db)
{
  .CloseSession.httpapi(attr(db, "connection"))
}

#' @see iquery()
#' @noRd
Query.httpapi <- function(db, query_or_scidb, 
                          `return`=FALSE,
                          binary=TRUE,
                          arrow=FALSE,
                          ...)
{
  if (!`return`) {
    Execute.httpapi(db, query_or_scidb, ...)
    return(invisible(NULL))
  }
  if (binary || arrow) {
    return(BinaryQuery.httpapi(db,
                               query_or_scidb,
                               arrow=arrow,
                               ...))
  }
  return(TextQuery.httpapi(db, query_or_scidb, ...))
}

#' @see Execute()
#' @noRd
Execute.httpapi <- function(db, query_or_scidb, ...)
{
  EnsureSession.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  query <- .GetQueryString(query_or_scidb)
  
  ## Start a new query
  ## Even though there is nothing to return, we still use the paged workflow
  ## because it allows us to cancel the query by issuing a DELETE request
  ## before it completes. This can change once SDB-7403 is addressed.
  msg.trace("Execute.httpapi(\"", query, "\"",
            ", args=", jsonlite::toJSON(list(...)), ")")
  timings <- c(start=proc.time()[["elapsed"]])
  http_query <- New.httpquery(conn, query)
  timings <- c(timings, prepare=proc.time()[["elapsed"]])
  
  on.exit(
    suspendInterrupts(
      tryCatch(Close.httpquery(http_query),
               error=function (err) {
                 warning("[SciDBR] Error while closing query ", http_query$id, 
                         "(ignored): ", err)
               })),
    add=TRUE)
  
  if (http_query$is_selective) {
    first_operator <- strsplit(query_or_scidb, "(", fixed=TRUE)[[1]]
    stop("The AFL query '", first_operator, "(...)' returns data, ",
         "so it should be executed using Query.httpapi() with return=TRUE")
  }
  
  ## Fetch the first (and only) page from the query.
  ## We must do this even though the page won't contain any data, because
  ## any DDL action only gets committed if the query is fetched to completion.
  ## If there is an error, the on.exit() above will close the query,
  ## and the error will be propagated to the caller.
  empty_page <- Next.httpquery(http_query)
  timings <- c(timings, fetch=proc.time()[["elapsed"]])
  
  .ReportTimings(paste0("Timings for query ", http_query$id, ":"), 
                 timings)
  
  return(invisible(NULL))
}

TextQuery.httpapi <- function(db, query_or_scidb, 
                              format=NULL,
                              use_aio=NULL,
                              only_attributes=NULL,
                              ...)
{
  use_aio <- use_aio %||% getOption("scidb.aio", FALSE)
  only_attributes <- only_attributes %||% FALSE
  if (!has.chars(format)) {
    format <- if (use_aio) "tdv" else "csv+:l"
  }
  if (use_aio && !startsWith(format, "aio_")) {
    format <- paste0("aio_", format)
  }

  EnsureSession.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  query <- .GetQueryString(query_or_scidb)
  
  if (use_aio && !only_attributes) {
    ## The HTTP API doesn't let us pass the "atts_only" parameter to
    ## aio_save, so instead we'll use flatten() to turn dims into attrs.
    ## An added advantage of this is that flatten() returns dims followed
    ## by attributes, so we won't need to permute columns after reading.
    query <- sprintf("flatten(%s)", query)
  }
  
  ## Start a new query
  msg.trace("TextQuery.httpapi(\"", query, "\"",
            ", format=", format, ", use_aio=", use_aio,
            ", only_attributes=", only_attributes,
            ", args=", jsonlite::toJSON(list(...)), ")")
  timings <- c(start=proc.time()[["elapsed"]])
  http_query <- New.httpquery(conn, query, options=list(format=format))
  timings <- c(timings, prepare=proc.time()[["elapsed"]])
  
  ## Since this function currently fetches all of its results in one page,
  ## we can close the query as soon as this function exits.
  on.exit(
    suspendInterrupts(
      tryCatch(Close.httpquery(http_query),
               error=function (err) {
                 warning("[SciDBR] Error while closing query ", http_query$id, 
                         "(ignored): ", err)
               })),
    add=TRUE)

  ## If this stop() triggers, it means someone called Query(`return`=TRUE)
  ## with a non-selective query (using a DDL or updating operator).
  ## For now, we stop() so callers must use the `return` parameter correctly,
  ## but in the future we could disable this check - in which case the function
  ## would just return NULL if the query was non-selective.
  if (!http_query$is_selective) {
    first_operator <- strsplit(query_or_scidb, "(", fixed=TRUE)[[1]]
    stop("The AFL query '", first_operator, "(...)' does not return data, ",
         "so it should be executed using Query.httpapi() with return=FALSE")
  }
  
  ## Fetch the first (and only) page from the query.
  ## We must do this even if it was, because any DDL action only 
  ## gets committed if the query is fetched to completion.
  ## If there is an error, the on.exit() above will close the query,
  ## and the error will be propagated to the caller.
  raw_page <- Next.httpquery(http_query)
  timings <- c(timings, fetch=proc.time()[["elapsed"]])
  
  df <- NULL
  if (http_query$is_selective && !is.null(raw_page)) {
    ## Convert the text data to an R dataframe
    df <- .Text2df(rawToChar(raw_page), format=format)
    timings <- c(timings, parse_csv=proc.time()[["elapsed"]])
  }
  
  .ReportTimings(paste0("Timings for query ", http_query$id, ":"), 
                 timings)
  
  return(if (is.null(df)) invisible(NULL) else df)
}

## TODO: set default page_size once binary output supports paging (SDB-7836)
#' @see BinaryQuery()
#' @noRd
BinaryQuery.httpapi <- function(db,
                                query_or_scidb, 
                                arrow=FALSE,
                                use_aio=NULL,
                                buffer_size=NULL,
                                only_attributes=NULL, 
                                schema=NULL,
                                page_size=NULL, 
                                npages=NULL,
                                ...)
{
  ## In SciDBR <=3.1.0, the semantics of binary=TRUE queries are different from 
  ## those of binary=FALSE queries. To remain interchangeable with that version
  ## and keep the code as simple as possible, Query and BinaryQuery are
  ## implemented using separate functions rather than trying to combine the
  ## different behaviors into a single function.
  
  only_attributes <- only_attributes %||% FALSE
  use_aio <- arrow || (use_aio %||% getOption("scidb.aio", FALSE))

  EnsureSession.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  query <- .GetQueryString(query_or_scidb)
  
  use_int64 <- conn$int64
  result_size_limit_mb <- getOption("scidb.result_size_limit", 256)
  result_size_limit_bytes <- result_size_limit_mb * 1000000
  
  format <- if (arrow) {
              "arrow"
            } else if (only_attributes || use_aio) {
              "binary"
            } else {
              ## binary+ is not supported by aio_save; instead we use the
              ## "flatten" hack below with format="binary"
              "binary+"
            }
  if (use_aio && !startsWith(format, "aio_")) {
    format <- paste0("aio_", format)
  }
  
  if (use_aio && !only_attributes) {
    ## The HTTP API doesn't let us pass the "atts_only" parameter to
    ## aio_save, so instead we'll use flatten() to turn dims into attrs.
    ## An added advantage of this is that flatten() returns dims followed
    ## by attributes, so we won't need to permute columns after reading.
    query <- sprintf("flatten(%s)", query)
  }
  
  ## Start a new query
  msg.trace("BinaryQuery.httpapi(\"", query, "\"",
            ", format=", format, ", use_aio=", use_aio,
            ", buffer_size=", buffer_size,
            ", only_attributes=", only_attributes,
            ", schema=", schema,
            ", page_size=", page_size,
            ", npages=", npages,
            ", args=", jsonlite::toJSON(list(...)), ")")
  timings <- c(start=proc.time()[["elapsed"]])
  http_query <- New.httpquery(conn, query, 
                              options=list(format=format, 
                                           pageSize=page_size))
  timings <- c(timings, prepare=proc.time()[["elapsed"]])
  
  ## Since this function fetches all the pages it's interested in
  ## before returning, we can close the query as soon as this function exits.
  on.exit(
    suspendInterrupts(
      tryCatch(Close.httpquery(http_query),
               error=function (err) {
                 warning("[SciDBR] Error while closing query ", http_query$id, 
                         "(ignored): ", err)
               })),
    add=TRUE)
  
  ## These are the columns we expect to be encoded in the response:
  ## dimensions (if requested), followed by attributes
  dimensions <- NULL
  attributes <- NULL
  ## If the caller provided a schema, it overrides the actual schema
  schema <- schema %||% http_query$schema
  if (!is.null(schema)) {
    dimensions <- .dimsplitter(schema)
    attributes <- .attsplitter(schema)
    
    cols <- NULL
    ## length(dimensions) can be 0 if the query returned a SciDB dataframe
    if (length(dimensions) > 0 && !only_attributes) {
      ## Use stringsAsFactors=FALSE because we want the dataframe to contain
      ## actual strings (instead of dictionary-encoding them, which would turn
      ## every column of the dataframe into an integer vector)
      cols <- data.frame(name=dimensions$name, type="int64", nullable=FALSE, 
                         stringsAsFactors=FALSE)
    }
    cols <- rbind(cols, attributes)
  }
  
  ## a %<?% b: return a<b, or TRUE if either is NULL or NA
  `%<?%` <- function(a, b) {
    if (is.null(a) || is.na(a) || is.null(b) || is.na(b)) TRUE else a < b
  }
  
  ## Fetch one page at a time
  page_number <- 0
  total_nbytes <- 0
  ans <- NULL
  while (page_number %<?% npages 
         && total_nbytes %<?% result_size_limit_bytes) {
    ## Fetch the next page
    page_number <- page_number + 1
    raw_page <- Next.httpquery(http_query)
    timings[[paste0("fetch_page_", page_number)]] <- proc.time()[["elapsed"]]

    page_nbytes <- length(raw_page)
    total_nbytes <- total_nbytes + page_nbytes

    if (is.null(raw_page) || !http_query$is_selective || page_nbytes == 0
        || is.null(schema)) {
      ## We already got the last page, or the result is empty.
      msg.trace("Query page ", page_number, " returned empty result",
                "(page_bytes=", page_nbytes, ")")
      break
    }

    ## Convert the binary data to a dataframe, and add it to the answer
    if (arrow) {
      page_df <- .Arrow2df(raw_page)
    } else {
      page_df <- .Binary2df(raw_page, cols, buffer_size, use_int64)
    }

    ## Special legacy behavior for "binary" datatype: 
    ## if any column has type "binary", don't return a dataframe; 
    ## instead, return a list containing that binary value as the _first_ item,
    ## and the dimension (if present) as the _second_ item. 
    ## Don't fetch any more pages (in fact, don't fetch anything but 
    ## the first row).
    ## See issue #163 and the test for issues #163 in tests/a.R,
    ## as well as the test for issue #195 with the "binary" datatype in a.R.
    if (typeof(page_df) == "list" && "binary" %in% attributes$type) {
      page_list <- page_df  # (it isn't a dataframe)
      if (only_attributes) {
        return(page_list)
      } else {
        ## Move the dimensions to the last columns.
        ndims <- length(dimensions$name)
        ncols <- length(cols$name)
        result <- page_list[c((ndims+1):ncols, 1:ndims)]
        return(result)
      }
    }
    
    ans <- rbind(ans, page_df)
    timings[[paste0("parse_page_", page_number)]] <- proc.time()[["elapsed"]]

    page_ncols <- length(page_df)
    page_nrows <- if (page_ncols > 0) length(page_df[[1]]) else 0
    total_ncols <- length(ans)
    total_nrows <- if (total_ncols > 0) length(ans[[1]]) else 0

    msg.debug("Query page ", page_number, " returned ",
              page_nbytes, " bytes, ", 
              page_ncols, " columns, ",
              page_nrows, " rows;",
              " total so far: ", 
              total_nbytes, " bytes, ",
              total_ncols, " columns, ",
              total_nrows, " rows")

    ## If the number of results we received is less than the page size, 
    ## or if we didn't set a page size (which returns all results in one page),
    ## we know this is the last page. We can skip the final call to Next, since
    ## it will only return an empty page.
    if (is.null(page_size) || page_nrows < page_size) {
      msg.debug("Query page ", page_number, " returned ", 
                page_nrows, " rows ",
                "(page_size=", if (is.null(page_size)) "NULL" else page_size,
                "); assuming this is the last page")
      break
    }
  }
  .ReportTimings(paste0("Timings for query ", http_query$id, ":"), 
                 timings)
  
  if (is.null(ans)) {
    ## The response had no data. 
    ## If there was a schema, return an empty dataframe with the correct columns
    if (is.null(schema)) {
      msg.trace("Query returned no data and no schema; returning NULL")
      return(invisible())
    } else {
      msg.trace("Query returned no data; constructing empty dataframe")
      return(.Schema2EmptyDf(attributes, 
                             if (only_attributes) NULL else dimensions))
    }
  }

  ## TODO: For compatibility, uniqueify attribute names if !only_attributes

  return(ans)
}

## Use an S4 class to standardize field names for all upload methods.
## Because so much SciDBR code uses is.null() to check for missing values,
## it's important to allow any missing value to be NULL (instead of a
## non-null empty vector).
setClassUnion("NumericOrNull",members=c("numeric", "NULL"))
setClassUnion("CharOrNull", members=c("character", "NULL"))
setClass("UploadDesc",
         slots=c(
           ## Name of the SciDB array that will result from the upload
           array_name="CharOrNull",
           ## If TRUE, the SciDB array will be temporary (not versioned)
           temp="logical",
           ## Name of the file used for uploading
           filename="CharOrNull",
           ## Name of each attribute
           attr_names="CharOrNull",
           ## SciDB type of each attribute
           attr_types="CharOrNull",
           ## Name of each dimension
           dim_names="CharOrNull",
           ## Start coordinate of each dimension
           dim_start_coordinates="NumericOrNull",
           ## Chunk size of each dimension
           dim_chunk_sizes="NumericOrNull"
         ))

## Define as.list(UploadDesc) to assist with printing and debugging
as.list.UploadDesc <- function(desc)
{
  return(mapply(function(x) {slot(desc,x)},
                slotNames("UploadDesc"),
                SIMPLIFY=FALSE))
}

.Attrs.UploadDesc <- function(desc, default_names=NULL, default_types=NULL)
{
  stopifnot(class(desc) == "UploadDesc")
  attrs <- mapply(
    function(name, type) {
      list(name=as.character(name), type=as.character(type))
    },
    desc@attr_names %||% default_names,
    desc@attr_types %||% default_types,
    SIMPLIFY=FALSE
  )
  return(attrs)
}

.Dims.UploadDesc <- function(desc, 
                             default_names=NULL, 
                             default_chunk_sizes=NULL,
                             lengths=NULL,
                             make_unique=FALSE)
{
  dim_names <- desc@dim_names %||% default_names
  if (make_unique) {
    dim_names <- make.unique_(desc@attr_names, dim_names)
  }
  dims <- mapply(
    function(name, start, length, chunk) {
      list(name=as.character(name), 
           start=as.integer(start),
           end=(if (is.null(length)) NULL 
                else as.integer(start) + as.integer(length) - 1),
           chunk=as.integer(chunk))
    },
    dim_names,
    desc@dim_start_coordinates %||% replicate(length(dim_names), 0),
    lengths %||% replicate(length(dim_names), NULL),
    (desc@dim_chunk_sizes 
     %||% default_chunk_sizes 
     %||% replicate(length(dim_names), NULL)),
    SIMPLIFY=FALSE)
  return(dims)
}

.Desc2Schema <- function(db, desc)
{
  attrs <- .Attrs.UploadDesc(desc)
  dims <- .Dims.UploadDesc(desc)
  return(make.schema(db, attrs, dims, array_name=desc@array_name %||% ""))
}

#' @see Upload()
Upload.httpapi <- function(db, payload, 
                           name=NULL,
                           gc=TRUE,
                           temp=FALSE, 
                           attr_names=NULL,
                           attr_types=NULL,
                           dim_names=NULL,
                           dim_start_coordinates=NULL,
                           dim_chunk_sizes=NULL,
                           ## Legacy arguments (deprecated)
                           attrs=NULL,      # -> attr_names
                           attr=NULL,       # -> attr_names
                           types=NULL,      # -> attr_types
                           type=NULL,       # -> attr_types
                           start=NULL,      # -> dim_start_coordinates
                           chunk_size=NULL, # -> dim_chunk_sizes
                           ...)
{
  if (is.null(name)) {
    name <- tmpnam(db)
  }
  ## Sanitize the array name
  name <- gsub("[^[:alnum:]]", "_", name)

  EnsureSession.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  
  ## Collect "name" and implicit args into an upload descriptor
  desc <- new("UploadDesc")
  desc@array_name <- name
  desc@temp <- temp
  
  ## Generate an upload filename of the form:
  ##  upload.<connection-id>.<array-name>.<random-suffix>
  desc@filename <- basename(tempfile(sprintf("upload.%s.%s.", conn$id, name)))
  
  ## Convert legacy parameter names (deprecated) to the new names
  ## Because so much SciDBR code uses is.null() to check for missing values,
  ## it's important to set missing values to NULL (instead of a non-null
  ## empty vector).
  desc@attr_names <- as.character(attr_names %||% attrs %||% attr) %||% NULL
  desc@attr_types <- as.character(attr_types %||% types %||% type) %||% NULL
  desc@dim_names <- as.character(dim_names) %||% NULL
  desc@dim_start_coordinates <- as.numeric(dim_start_coordinates 
                                           %||% start) %||% NULL
  desc@dim_chunk_sizes <- as.numeric(dim_chunk_sizes %||% chunk_size) %||% NULL

  msg.trace("Upload.httpapi(",
            "class(payload)=", paste(class(payload), collapse=","),
            ", name=", name,
            ", UploadDesc=", jsonlite::toJSON(as.list(desc)),
            ", gc=", gc, ", temp=", temp,
            ", args=", jsonlite::toJSON(list(...)), ")")
            
  if (inherits(payload, "raw")) {
    .UploadRaw.httpapi(db, payload, desc, ...)
  } else if (inherits(payload, "data.frame")) {
    .UploadDf.httpapi(db, payload, desc, ...)
  } else if (inherits(payload, "dgCMatrix")) {
    .UploadSparseMatrix.httpapi(db, payload, desc, ...)
  } else if (is.null(dim(payload))) {
    .UploadVector.httpapi(db, payload, desc, ...)
  } else {
    .UploadDenseDimensioned.httpapi(db, payload, desc, ...)
  }
  
  ## Wrap the SciDB array with a "scidb" object.
  ## If gc==TRUE, the SciDB array will be removed from SciDB when this object
  ## gets garbage-collected.
  return(scidb(db, name, gc=gc))
}

New.httpquery <- function(conn, afl, options=list())
{
  ## Remove options whose values are NULL
  options <- Filter(Negate(is.null), options)
  
  ## If the query only consists of an identifier (alphanumeric + "_"),
  ## assume it's an array name. Wrap it with scan().
  if (! grepl("[^[:alnum:]_]", only(trimws(afl)))) {
    afl <- sprintf("scan(%s)", afl)
  }
  
  ## Prepare the JSON for posting
  query_and_options <- append(list(query=afl), options)
  query_json <- jsonlite::toJSON(query_and_options, auto_unbox=TRUE)
  
  ## Execute the POST
  resp <- .HttpRequest(conn, "POST", URI(conn, conn$links$queries),
                       data=query_json)
  stopifnot(resp$status_code == 201)  # 201 Created
  
  ## Read the headers and JSON body
  headers_list <- curl::parse_headers_list(resp$headers)
  links <- .ParseLinkHeaders(headers_list)
  json <- jsonlite::fromJSON(rawToChar(resp$content))
  
  ## Create an env for the result (use an env because it's always passed by
  ## reference, not copy-on-write, and it can be assigned a finalizer)
  query <- new.env()
  class(query) <- "httpquery"
  query$connection <- conn
  query$options <- options
  query$location <- headers_list[["location"]]
  query$next_page_number <- 1
  query$next_page_url <- links[["first"]]
  query$id <- json[["queryId"]]
  query$is_selective <- json[["isSelective"]]
  query$schema <- json[["schema"]]
  
  ## When the query gets garbage-collected, call Close.httpquery(query)
  reg.finalizer(query, Close.httpquery, onexit=TRUE)
  
  return(query)
}

Close.httpquery <- function(query)
{
  suspendInterrupts({
    if (is.null(query$location) || is.null(query$connection)) {
      msg.trace("Query ", query$id, " already closed")
      return(invisible(NULL));
    }
    
    uri <- URI(query$connection, query$location)
    msg.trace("Closing SciDB query ", query$id, " url=", uri)
    tryCatch(
      {
        resp <- .HttpRequest(query$connection, "DELETE", uri)
        ## We usually expect status 204 (No Content),
        ## but we can get status 200 with a message if the query was
        ## already closed or if it was canceled. In those cases, we can ignore
        ## the message because the query got closed, which is what we wanted.
        if (resp$status_code != 204 && resp$status_code != 200) {
          stop("DELETE ", uri, " gave unexpected status ", resp$status_code,
               ": ", rawToChar(resp$content))
        }
      },
      error=function(err) {
        warning("[SciDBR] Error closing SciDB query ", query$id, 
                " (ignored): ", err)
      })
    
    ## Set the location to NULL so we don't double-delete the session.
    ## Because query is an env, this side effect should immediately take effect
    ## outside of the current function call.
    query$location <- NULL
    query$next_page_url <- NULL
    query$connection <- NULL
  })
}

Next.httpquery <- function(query)
{
  if (is.null(query$next_page_url) || is.null(query$location)) {
    msg.trace("Query ", query$id, " already finished")
    return(NULL)
  }
  if (is.null(query$connection)) {
    stop("Query ", query$id, " has no connection")
  }
  
  ## Get the next page of the query by following the query$next URL
  uri <- URI(query$connection, query$next_page_url)
  msg.trace("Fetching page ", query$next_page_number, 
            " of query ", query$id)
  resp <- .HttpRequest(query$connection, "GET", uri)
  
  if (resp$status_code == 204) {
    ## 204 No Content means the last page has already been fetched
    query$next_page_url <- NULL
    return(NULL)
  } 
  if (resp$status_code != 200) {
    stop("[SciDBR] GET request ", uri, 
         " responded with unexpected status ", resp$status_code,
         ": ", rawToChar(resp$content))
  }
  
  headers_list <- curl::parse_headers_list(resp$headers)
  links <- .ParseLinkHeaders(headers_list)
  
  ## The response header should contain a link to the next page,
  ## or NULL if there is no next page.
  query$next_page_url <- links[["next"]]
  query$next_page_number <- 1 + query$next_page_number
  
  return(resp$content)
}

#' Close the session.
#' This is registered as a finalizer on attr(db, "connection") so it closes 
#' the session when the connection gets garbage-collected; it can also be called
#' from Close.httpapi().
#' @param conn the connection environment, usually obtained from 
#'    attr(db, "connection")
#' @noRd
.CloseSession.httpapi <- function(conn)
{
  suspendInterrupts({
    if (is.null(conn$location)) {
      msg.trace("Session ", conn$session, " already closed")
      return(invisible(NULL));
    }
    
    uri <- URI(conn, conn$location)
    msg.trace("Closing SciDB session ", conn$session, " url=", uri)
    tryCatch(
      {
        resp <- .HttpRequest(conn, "DELETE", uri)
        if (resp$status_code != 204) {  # 204 No Content
          stop("DELETE ", uri, " gave unexpected status ", resp$status_code,
               ": ", rawToChar(resp$content))
        }
      },
      error=function(err) {
        warning("[SciDBR] Error closing SciDB session ", conn$session, 
                " (ignored): ", err)
      })
    
    ## Set the location to NULL so we don't double-delete the session.
    ## Because conn is an env, this side effect should immediately take effect
    ## outside of the current function call.
    conn$location <- NULL
    ## Set the links to NULL to make sure nobody can use the session anymore
    conn$links <- NULL
    ## conn$session does not get cleared, so it can be used for reporting about
    ## the old session
  })
}

#' @noRd
.HttpRequest = function(db_or_conn, method, uri, data=NULL, 
                        attachments=NULL)
{
  conn <- .GetConnectionEnv(db_or_conn)
  if (is.null(conn)) stop("No connection environment")

  ## data must be NULL or a 1-element vector containing a character element
  stopifnot(length(data) <= 1)
  
  if (!inherits(uri, "URI")) {
    stop("String '", uri, "' is not of class URI;",
         " please use the URI() function to construct it.")
  }
  uri <- .SanitizeUri(uri)

  ## Create a new connection handle if one doesn't exist.
  ## We reuse one connection handle for all queries in a session, so that curl
  ## can automatically deal with authorization cookies sent from the server -
  ## sending them back to the server in each request, and keeping them 
  ## up to date when the server sends new cookies.
  if (is.null(conn$handle)) {
    conn$handle <- curl::new_handle()
  }  
    
  ## Reset the connection handle so we can reuse it for the new request.
  ## Cookies are preserved (they don't get erased by handle_reset()).
  ## If there is no handle or if it is dead, create a new handle.
  tryCatch(curl::handle_reset(conn$handle), 
           error=function(err) {conn$handle <- curl::new_handle()})
  h <- conn$handle
  
  ## Call curl::handle_setopt to set options for curl
  options <- switch(method, 
                    GET = list(httpget=TRUE),
                    POST = list(post=TRUE, 
                                postfieldsize=(
                                  if (is.null(data)) 0 else nchar(data)),
                                postfields=data),
                    DELETE = list(customrequest="DELETE"),
                    stop("unsupported HTTP method ", method))
  options <- c(options, list(
    http_version=2,
    ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
    ssl_verifypeer=0
  ))
  if (!is.null(conn$username) && !is.null(conn$password)) {
    options[["username"]] <- conn$username
    options[["password"]] <- conn$password
    ## Do not store the password after this connection attempt!
    ## If successful, the server will return an authorization cookie; curl will
    ## automatically save it on the connection handle and reuse it in 
    ## future requests to continue the authenticated session.
    conn$password <- NULL
  }
  curl::handle_setopt(h, .list=options)

  ## Call curl::handle_setform to add the attachments, if any
  if (length(attachments) > 0) {
    setform_args <- list(h)
    for (iatt in seq_along(attachments)) {
      name <- names(attachments)[[iatt]]
      att <- attachments[[iatt]]
      
      if (method != "POST") { 
        stop("attachment not allowed for method ", method) 
      }
      if (is.null(name) || !nzchar(only(name))) { 
        stop("no name given for attachment ", iatt) 
      }
      if (is.null(att$data)) { 
        stop("no data given for attachment ", name)
      }
      if (is.null(att$content_type)) { 
        stop("no content_type given for attachment ", name) 
      }

      setform_args[[name]] <- curl::form_data(att$data, type=att$content_type)
      msg.trace("Adding attachment ", iatt, " (name=", name, 
                ", content-type=", att$content_type, 
                ") to pending POST, with data:\n  ", att$data)
    }
    do.call(curl::handle_setform, setform_args)
  }

  if (method == "POST") {
    msg.trace("Sending ", method, " ", uri, "\n  data=", data,
              if (is.null(attachments)) "" 
              else sprintf(", with %d attachments", length(attachments)))
  } else {
    msg.trace("Sending ", method, " ", uri)
  }
  
  resp <- curl::curl_fetch_memory(uri, h)
  
  if (resp$status_code == 0 && conn$protocol == "http") {
    ## Attempted to access an https port using http
    ## If this "stop" message is changed, also change the code that handles it 
    ## in .Handshake() in utility.R
    stop("https required")
  }
  if (resp$status_code <= 199 || resp$status_code > 299) {
    ## The server responded with an error or unexpected status code
    error_msg <- rawToChar(resp$content)
    if (startsWith(error_msg, "{")) {
      ## Treat it as a JSON error message
      json <- jsonlite::fromJSON(error_msg)
      ## Add potentially useful client-side information to the error JSON
      json[["http_method"]] <- method
      json[["http_uri"]] <- uri
      json[["http_status"]] <- resp$status_code
      ## Unparse the JSON back into a string and throw it as an exception
      stop(jsonlite::toJSON(json, auto_unbox=TRUE))
    } else {
      stop(sprintf("HTTP error %s\n%s", resp$status_code, error_msg))
    }
  }
  
  ## Make sure we don't incur the overhead of parsing headers and content
  ## unless we're in trace mode
  if (is.trace()) {
    msg.trace("Received headers: ", 
              paste(curl::parse_headers(resp$headers), collapse=" | "))
    if (length(resp$content) > 0) {
      is_text <- .HttpResponseIsText(resp)
      msg.trace("Received body", 
                if (!is_text) " (binary)", 
                ":\n>>>>>\n  ",
                if (is_text) rawToChar(resp$content) else resp$content,
                "\n<<<<<")
    }
  }
  
  return(resp)
}

#' Given an HTTP response, return TRUE if the content is text,
#' or FALSE if there are non-printable characters.
#' @param resp the result of a call to curl_fetch_memory
#' @return TRUE iff the resp$content is text-formatted, i.e. if we can use 
#'  rawToChar(resp$content) without an error.
.HttpResponseIsText <- function(resp)
{
  ## The slow way: look for non-printing characters
  return(length(grepRaw("[^[:print:][:space:]]", resp$content)) == 0)
  
  ## TODO after SDB-7833: Check the Content-Type header instead.
}

#' Given a list of HTTP response headers parsed by curl::parse_headers_list,
#' return the URLs of all "Link" headers indexed by link name.
#' For example, given these Link headers:
#'   <http://www.paradigm4.com/index.html>; rel="P4 Home"
#'   <http://www.github.com/Paradigm4>; rel="P4 GitHub Space"
#' The function would return a list like:
#'   list(
#'      "P4 Home" = "http://www.paradigm4.com/index.html",
#'      "P4 GitHub Space" = "http://www.github.com/Paradigm4"
#'   )
#' @param headers_list the result of curl::parse_headers_list() on some
#'   HTTP response from a server
#' @return list(relation_name="link URL")
#' @noRd
.ParseLinkHeaders <- function(headers_list)
{
  ## Find all "Link" headers named
  link_headers <- headers_list[names(headers_list) == "link"]
  
  ## Each Link header looks like: '<URL>; rel="link_name"'
  ## Match the URL and the link name.
  m <- regexec("<([^>]*)>; rel=\"([^\"]+)\".*", link_headers, perl=TRUE) 
  matches <- regmatches(link_headers, m)
  
  ## First matched group (match number 2) of every match is the URL
  result <- lapply(matches, function(x) x[[2]])
  ## Second group (match number 3) is the link name; make it the element name
  names(result) <- lapply(matches, function(x) x[[3]])
  
  ## Voila, now we have an array with named indices where result[[name]] 
  ## is the link URL.
  return(result)
}

#' Given a relative path to an endpoint within httpapi,
#' return the full URL/URI for the endpoint, including server DNS and port.
#' @param db_or_conn scidb database connection object,
#'     _or_ its "connection" attribute
#' @param path the path to the endpoint, 
#'     relative to "http[s]://host/api/v{version}/". 
#'     It should not begin with a slash.
#' @param args optional key=value args to add to the URL.
#' @noRd
.EndpointUri <- function(db_or_conn, path, args=list())
{
  conn <- .GetConnectionEnv(db_or_conn)
  if (substr(path, 1, 1) == "/") {
    stop("relative path expected, but got ", path)
  }
  
  api_version <- conn$httpapi_version
  if (is.null(api_version)) {
    return(URI(db_or_conn, sprintf("/api/%s", path), args))
  } else {
    # We know what httpapi version to use, so add v{version} to the path
    path <- sprintf("/api/v%d/%s",
                    conn$httpapi_version,
                    path)
    return(URI(db_or_conn, path, args))
  }
}

#' @noRd
.SanitizeUri <- function(uri)
{
  uri <- oldURLencode(uri)
  uri <- gsub("\\+", "%2B", uri, perl=TRUE)
  return(uri)
}

#' Given a list of actions and when they happened, e.g. 
#' (start=time1, firstAction=time2, secondAction=time3, ...),
#' this function will output a message indicating that
#' firstAction took (time2-time1) seconds, secondAction took
#' (time3-time2) seconds, etc.
#' @param msg a message to print before the timings
#' @param timings a list of the form (start=time1, firstAction=time2, ...)
#'   where all timings were obtained from proc.time()[["elapsed"]].
.ReportTimings <- function(msg, timings)
{
  if (!is.debug()) {
    return()
  }

  for (ii in seq(2, length(timings))) {
    if (ii > 2) { 
      msg <- paste0(msg, ",") 
    }
    msg <- paste(msg, names(timings)[[ii]], 
                 timings[[ii]] - timings[[ii-1]])
  }
  msg.debug(msg)
}

#' Upload an R raw value to a special 1-element SciDB array
#' @see .raw2scidb.shim()
.UploadRaw.httpapi <- function(db, payload, desc)
{
  timings <- c(start=proc.time()[["elapsed"]])
  if (!is.raw(payload)) stop("payload must be a raw value")
  if (!is.present(desc)) stop("No upload descriptor provided")
  if (!has.chars(only(desc@array_name))) stop ("No array name provided")
  if (!has.chars(only(desc@filename))) stop("No upload filename provided")

  bytes <- .Call(C_scidb_raw, payload)
  timings <- c(timings, encode=proc.time()[["elapsed"]])

  schema <- sprintf("<%s:binary null>[%s=%d:%d:0:%d]",
                    first(desc@attr_names) %||% "val",
                    first(desc@dim_names) %||% "i",
                    first(desc@dim_start_coordinates) %||% 0,
                    first(desc@dim_start_coordinates) %||% 0,
                    first(desc@dim_chunk_sizes) %||% 1)
  
  query <- sprintf("store(input(%s, '%s', -2, '(binary null)'), %s)",
                   schema, 
                   only(desc@filename),
                   only(desc@array_name))
  
  content_type <- "application/octet-stream"
  
  .UploadWithQuery.httpapi(db, bytes, query, schema, content_type, desc)
  timings <- c(timings, upload=proc.time()[["elapsed"]])
  
  .ReportTimings(paste0("Timings for raw upload to array '", 
                        only(desc@array_name), "':"), 
                 timings)
}

.UploadDf.httpapi <- function(db, payload, desc,
                              use_aio_input=FALSE)
{
  timings <- c(start=proc.time()[["elapsed"]])
  if (!is.data.frame(payload)) stop("payload must be a dataframe")
  if (!is.present(desc)) stop("No upload descriptor provided")
  if (!has.chars(only(desc@array_name))) stop ("No array name provided")
  if (!has.chars(only(desc@filename))) stop("No upload filename provided")
  
  ## Preprocess the dataframe to prepare it for upload
  processed <- .PreprocessDfTypes(payload, desc@attr_types, use_aio_input)
  payload <- processed$df
  aio_apply_args <- processed$aio_apply_args
  desc@attr_names <- names(payload)
  desc@attr_types <- processed$attr_types
  
  timings <- c(timings, preprocess=proc.time()[["elapsed"]])
  
  ## Serialize the dataframe to TSV format
  data <- .TsvWrite(payload, file=.Primitive("return"))
  timings <- c(timings, encode=proc.time()[["elapsed"]])

  ## Construct the schema
  schema <- dfschema(desc@attr_names,
                     desc@attr_types,
                     nrow(payload),
                     chunk=desc@dim_chunk_sizes,
                     start=desc@dim_start_coordinates,
                     dim_name=desc@dim_names)

  ## Construct the AFL query used to process the data after uploading
  has_aio <- (length(grep("aio_input", names(db))) > 0)
  if (use_aio_input && has_aio) {
    ## Use aio_input() to load the data. 
    aioSettings = list(num_attributes=ncol(payload))
    if (is.present(desc@dim_chunk_sizes) && only(desc@dim_chunk_sizes) > 0) {
      aioSettings[['chunk_size']] = only(desc@dim_chunk_sizes)
    }
    
    ## Wrap it in apply() to restore the attribute names, because aio_input()
    ## returns a schema with attributes named (a1, a2, ...)
    query <- sprintf("store(project(apply(aio_input('%s', %s), %s), %s), %s)",
                     only(desc@filename),
                     get_setting_items_str(db, aioSettings),
                     only(aio_apply_args),
                     paste(desc@attr_names, collapse=", "),
                     desc@array_name)
  } else {
    query <- sprintf("store(input(%s, '%s', -2, 'tsv'), %s)",
                     schema,
                     only(desc@filename),
                     desc@array_name)
  }

  content_type <- "text/tab-separated-values"
  
  .UploadWithQuery.httpapi(db, data, query, schema, content_type, desc)
  timings <- c(timings, upload=proc.time()[["elapsed"]])
  
  .ReportTimings(paste0("Timings for dataframe upload to array '", 
                        only(desc@array_name), "':"), 
                 timings)
}

.UploadSparseMatrix.httpapi <- function(db, payload, desc)
{
  timings <- c(start=proc.time()[["elapsed"]])
  if (!inherits(payload, "dgCMatrix")) stop("payload must be a dgCMatrix")
  if (!is.present(desc)) stop("No upload descriptor provided")
  if (!has.chars(only(desc@array_name))) stop ("No array name provided")
  if (!has.chars(only(desc@filename))) stop("No upload filename provided")

  if (!is.present(desc@dim_start_coordinates)) {
    desc@dim_start_coordinates <- c(0,0)
  }
  desc@dim_start_coordinates <- as.integer(desc@dim_start_coordinates)
  
  type <- .scidbtypes[[typeof(payload@x)]]
  if (is.null(type) || type != "double") {
    stop("Sorry, the package only supports double-precision sparse matrices",
         " right now.")
  }

  desc@attr_types <- desc@attr_types %||% .scidbtypes[[typeof(payload@x)]]
  attrs <- .Attrs.UploadDesc(desc, default_names="val")
  if (length(attrs) > 1) stop("too many attributes")
  
  dims <- .Dims.UploadDesc(desc, default_names=c("i", "j"))
  if (length(dims) != 2) stop("matrix should have 2 dimensions")
  dims[[1]]$end <- nrow(payload)-1 + dims[[1]]$start
  dims[[2]]$end <- ncol(payload)-1 + dims[[2]]$start
  dims[[1]]$chunk <- min(nrow(payload), dims[[1]]$chunk)
  dims[[2]]$chunk <- min(ncol(payload), dims[[2]]$chunk)
  
  schema <- make.schema(db, attrs, dims)
  schema1d <- sprintf("<i:int64, j:int64, val:%s>[idx=0:*:0:100000]", 
                      desc@attr_types[[1]])

  col_binary_format <- sprintf("%s null", desc@attr_types[[1]])
  binary_format_str <- sprintf("(%s)", paste(rep(col_binary_format, 3), 
                                             collapse=", "))

  # Compute the indices and assemble message to SciDB in the form
  # (i:double, j:double, val:double) for indices (i, j) and data (val).
  dp <- diff(payload@p)
  j <- rep(seq_along(dp), dp) - 1
  bytes <- .Call(C_scidb_raw, as.vector(
    t(matrix(c(payload@i + dims[[1]]$start, j + dims[[2]]$start, payload@x),
             length(payload@x)))))
  timings <- c(timings, encode=proc.time()[["elapsed"]])

  query <- sprintf(
    "store(redimension(input(%s, '%s', -2, '%s'), %s), %s)",
    only(schema1d), 
    only(desc@filename), 
    only(binary_format_str),
    only(schema), 
    only(desc@array_name))
  
  content_type <- "application/octet-stream"
  
  .UploadWithQuery.httpapi(db, bytes, query, schema, content_type, desc)
  timings <- c(timings, upload=proc.time()[["elapsed"]])
  
  .ReportTimings(paste0("Timings for matrix upload to array '", 
                        only(desc@array_name), "':"), 
                 timings)
}

.UploadVector.httpapi <- function(db, payload, desc, 
                                  max_byte_size=NULL)
{
  timings <- c(start=proc.time()[["elapsed"]])

  processed <- .PreprocessArrayType(payload, type=desc@attr_types)
  payload <- processed$array
  load_type <- processed$load_type
  desc@attr_types <- processed$attr_type
  attrs <- .Attrs.UploadDesc(desc, default_names="val")
  if (length(attrs) != 1) stop("expected 1 attribute")

  ## dims_unbounded is [i=0:*:0:min(1000,n)]
  dims_unbounded <- .Dims.UploadDesc(desc,
                                     default_names="i",
                                     default_chunk_sizes=min(1000, 
                                                             length(payload)),
                                     make_unique=TRUE)
  if (length(dims_unbounded) != 1) stop("expected 1 dimension")
  
  ## dims is [i=0:n-1:0:min(1000,n)]
  dims <- dims_unbounded
  dims[[1]]$end <- dims[[1]]$start + length(payload) - 1

  ## schema is <val:type>[i=0:n-1:0:min(1000,n)]
  schema <- make.schema(db, attrs, dims)
  ## schema_unbounded: the same as `schema` but with "*" for the dimension end.
  ## Used for uploading a single page.
  schema_unbounded <- make.schema(db, attrs, dims_unbounded)
  
  page_size <- .get_multipart_post_load_block_size(
    data=payload,
    debug=is.debug(),
    max_byte_size=max_byte_size %||% getOption("scidb.max_byte_size",
                                               500*(10^6)))
  
  ## Upload page by page
  page_number <- 0
  npages <- ceiling(length(payload) / page_size)
  total_nbytes <- 0
  for (page_begin in seq(1, length(payload), page_size)) {
    page_number <- page_number + 1
    page_end <- min((page_begin + page_size - 1), length(payload))
    
    ## Convert data to raw
    page_payload <- as.matrix(payload[page_begin:page_end])
    page_bytes <- .Call(C_scidb_raw, as.vector(aperm(page_payload)))
    total_nbytes <- total_nbytes + length(page_bytes)
    timings[[paste0("encode_page_", page_number)]] <- proc.time()[["elapsed"]]

    msg.trace("Uploading page ", page_number, " of ", npages)
    if (page_number == 1) {
      query <- sprintf("store(input(%s, '%s', -2, '(%s null)'), %s)", 
                       only(schema), 
                       only(desc@filename),
                       only(load_type),
                       only(desc@array_name))
    } else {
      query <- sprintf("append(input(%s, '%s', -2, '(%s null)'), %s, i)", 
                       only(schema_unbounded), 
                       only(desc@filename),
                       only(load_type),
                       only(desc@array_name))
      ## If we created a temp array for the first page, don't create it again!
      desc@temp <- FALSE
    }
    
    content_type <- "application/octet-stream"
    .UploadWithQuery.httpapi(db, page_bytes, query, schema,
                             content_type, desc)
    timings[[paste0("upload_page_", page_number)]] <- proc.time()[["elapsed"]]
  }
  
  .ReportTimings(paste0("Timings for uploading R vector to array '",
                        only(desc@array_name), "' (", 
                        only(page_number), " pages, ",
                        only(total_nbytes), " bytes): "), 
                 timings)
}

.UploadDenseDimensioned.httpapi <- function(db, payload, desc, 
                                            reshape=TRUE, ...)
{
  if (!is.array(payload)) stop ("payload must be an array or vector")
  timings <- c(start=proc.time()[["elapsed"]])

  processed <- .PreprocessArrayType(payload, type=desc@attr_types)
  payload <- processed$array
  load_type <- processed$load_type
  desc@attr_types <- processed$attr_type
  attrs <- .Attrs.UploadDesc(desc, default_names="val")
  if (length(attrs) != 1) stop("expected 1 attribute")
  
  ## For a matrix, dimension names are (i, j)
  ## For an n-dimensional array (n>2), dim names are (i1, i2, ...)
  dim_lengths <- dim(payload)
  ndims <- length(dim_lengths)
  dim_names <- if (ndims == 2) c("i", "j") else paste0("i", 1:ndims)
  dims <- .Dims.UploadDesc(desc,
                           default_names=dim_names,
                           default_chunk_sizes=floor(10e6 ^ (1/ndims)),
                           lengths=dim_lengths,
                           make_unique=TRUE)
  
  schema <- make.schema(db, attrs, dims)
  load_schema <- sprintf("<%s:%s>[__row=1:%.0f:0:1000000]", 
                         attrs[[1]]$name, 
                         attrs[[1]]$type,
                         length(payload))

  ## Convert data to raw
  bytes <- .Call(C_scidb_raw, as.vector(aperm(payload)))
  timings[["encode"]] <- proc.time()[["elapsed"]]
  
  if (as.logical(reshape)) {
    query <- sprintf("store(reshape(input(%s, '%s', -2, '(%s null)'), %s), %s)", 
                     load_schema,
                     desc@filename, 
                     load_type, 
                     schema, 
                     desc@array_name)
    action <- "upload_and_reshape"
  } else {
    query <- sprintf("store(input(%s, '%s', -2, '(%s null)'), %s)", 
                     load_schema, 
                     desc@filename, 
                     load_type, 
                     desc@array_name)
    action <- "upload"
  }
  
  content_type <- "application/octet-stream"
  .UploadWithQuery.httpapi(db, bytes, query, schema, content_type, desc)
  timings[[action]] <- proc.time()[["elapsed"]]
  
  .ReportTimings(paste0("Timings for uploading dense ", 
                        ndims, "-dimensional array '",
                        desc@array_name, "', ", 
                        nchar(bytes, type="bytes"), " bytes: "), 
                 timings)
}

.UploadWithQuery.httpapi <- function(db, data, query, 
                                     schema, content_type, desc)
{
  if (is.null(data)) stop("No attachment data provided")
  if (!has.chars(only(query))) stop("No query provided")
  if (!is.present(desc)) stop("No upload descriptor provided")
  if (!has.chars(only(desc@array_name))) stop ("No array name provided")
  if (!has.chars(only(desc@filename))) stop("No upload filename provided")

  ## Pre-create the array if the client requested a temp array
  success <- FALSE
  
  if (desc@temp) {
    ## Caller asked for a temp array instead of a SciDB versioned array
    if (!has.chars(schema)) stop("Temp array requested, but no schema provided")
    create_temp_array(db, desc@array_name, schema)
    on.exit(if (!success) {
              Execute(db, sprintf("remove(%s)", desc@array_name))
            },
            add=TRUE)
  }

  ## Write the query JSON
  query_and_options <- list(query=query,
                            attachment=desc@filename)
  query_json <- jsonlite::toJSON(query_and_options, auto_unbox=TRUE)
  
  ## Prepare the attachments: a "query" attachment with the query JSON,
  ## and an "attachment" attachment with the uploaded data
  attachments <- list(
    query=list(data=query_json, content_type="application/json"),
    attachment=list(data=data, content_type=content_type)
  )
  
  ## Execute the POST, which uploads the data and executes the query
  conn <- attr(db, "connection")
  resp <- .HttpRequest(conn, "POST", URI(conn, conn$links$execute),
                       data=NULL, 
                       attachments=attachments)
  
  if (resp$status_code != 200 && resp$status_code != 204) {
    stop("[SciDBR] Upload request ", uri, 
         " responded with unexpected status ", resp$status_code,
         ": ", rawToChar(resp$content))
  }

  success <- TRUE  
  if (resp$status_code == 204) {
    ## 204 No Content means the query returned no data
    return(NULL)
  } 
  return(resp$content)
}

