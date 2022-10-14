###
### @P4COPYRIGHT_SINCE:2022@
###

#'
#' @file httpapi.R
#' @brief Implementation layer for calls to the shim-less SciDB HTTP API
#'    available in SciDB 22.5 and later.
#'

#' We support these versions of the client API.
httpapi_supported_versions <- c(1)

## Helper functions
`%||%` <- function(a, b) { if (length(a) > 0 && !is.na(a)) a else b }
is.present <- function(a) { length(a) > 0 }
has.chars <- function(a) { length(a) > 0 && all(nzchar(a)) }
first <- function(a) { if (length(a) > 0) a[[1]] else NULL }
only <- function(a)
{
  stopifnot(length(a) == 1) 
  a[[1]]
}

#' @see GetServerVersion()
#' @noRd
GetServerVersion.httpapi <- function(db)
{
  conn <- attr(db, "connection")
  if (is.null(conn)) stop("No connection environment")
  if (is.null(conn$handle)) {
    conn$handle <- curl::new_handle()
  }
  
  resp <- .HttpRequest(db, "GET", .EndpointUri(db, "version"))
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
  class(db) <- c("httpapi", "afl")
  
  ## We need to return db because we have modified its class,
  ## and db is pass-by-value (it isn't an env)
  return(db)
}

#' @see Connect()
#' @param db_or_conn a database connection object, or a connection env
#' @param reconnect If TRUE, make a new session regardless of whether one
#'   already exists. If FALSE and a session already exists, returns without
#'   changing anything.
#' @noRd
Connect.httpapi <- function(db_or_conn, reconnect=FALSE)
{
  conn <- .GetConnectionEnv(db_or_conn)
  if (is.null(conn)) stop("no connection environment")
  
  ## Create a new connection handle.
  ## Because we reuse one connection handle for all queries in a session,
  ## curl automatically deals with authorization cookies sent from the server -
  ## sending them back to the server in each request, and keeping them 
  ## up to date when the server sends new cookies.
  if (is.null(conn$handle)) {
    conn$handle <- curl::new_handle()
  }

  connected <- (!is.null(conn$session)
                && !is.null(conn$location) 
                && !is.null(conn$links))
  if (connected && !reconnect) {
    return()
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
  conn$id <- sid
  ## Should not use password going forward: use the authorization cookies
  ## that were returned by the server and which curl automatically saves
  ## on the connection handle.
  conn$password <- NULL

  ## Register the session to be closed when the connection env
  ## gets garbage-collected.
  reg.finalizer(conn, .CloseHttpApiSession, onexit=TRUE)
  
  ## We don't need to return db or conn because the only object we have modified
  ## is the connection env which is pass-by-reference.
}

#' @see Close()
#' @noRd
Close.httpapi <- function(db)
{
  .CloseHttpApiSession(attr(db, "connection"))
}

#' @see Execute()
#' @noRd
Execute.httpapi <- function(db, query_or_scidb, ...)
{
  Query.httpapi(db, query_or_scidb, 
                `return`=FALSE, binary=FALSE, arrow=FALSE,
                ...)
  invisible()
}

#' @see iquery()
#' @noRd
Query.httpapi <- function(db, query_or_scidb, 
                          `return`=FALSE,
                          binary=TRUE,
                          arrow=FALSE,
                          format="csv+:l",
                          ...)
{
  ## TODO: Currently ignoring `arrow`, waiting for SDB-7821
  is_debug = getOption("scidb.debug", FALSE)

  ## For backward compatibility: defer to BinaryQuery if binary==TRUE
  if (binary) {
    return(scidb_unpack_to_dataframe(db, query_or_scidb, ...))
  }
  
  Connect.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  query <- .GetQueryString(query_or_scidb)
  
  ## Start a new query
  if (is_debug) {
    message("[SciDBR] text query (", format, "): ", query)
  }
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
               })))
  
  ## TODO: In the future, once we have confirmed that all application code uses
  ## `return` in the expected way (i.e. nobody sets return=FALSE to ignore data 
  ## from a selective query), we can deprecate the `return` argument, ignore it,
  ## and just use is_selective instead.
  if (`return` == FALSE && http_query$is_selective) {
    stop("`return` argument must be TRUE for a selective query")
  }
  if (`return` == TRUE && !http_query$is_selective) {
    stop("`return` argument must be FALSE for a DDL or update query")
  }
  
  ## Fetch the first (and only) page from the query.
  ## We must do this even if return==FALSE, because any DDL action only 
  ## gets committed if the query is fetched to completion.
  ## If there is an error, the on.exit() above will close the query,
  ## and the error will be propagated to the caller.
  raw_page <- Next.httpquery(http_query)
  timings <- c(timings, fetch=proc.time()[["elapsed"]])
  
  df <- NULL
  if (`return` && http_query$is_selective && !is.null(raw_page)) {
    ## Convert the CSV data to an R dataframe
    df <- .Csv2df(rawToChar(raw_page))
    timings <- c(timings, parse_csv=proc.time()[["elapsed"]])
  }
  
  if (is_debug) {
    .ReportTimings(paste0("[SciDBR] timings for query ", http_query$id, ":"), 
                   timings)
  }
  
  return(if (is.null(df)) invisible(NULL) else df)
}

## rsamuels TODO: set default page_size once binary output supports paging
#' @see BinaryQuery()
BinaryQuery.httpapi <- function(db,
                                query_or_scidb, 
                                binary=TRUE, 
                                buffer_size=NULL,
                                only_attributes=NULL, 
                                page_size=NULL, 
                                npages=NULL, 
                                ...)
{
  ## In SciDBR <=3.1.0, the semantics of binary=TRUE queries are different from 
  ## those of binary=FALSE queries. To remain interchangeable with that version
  ## and keep the code as simple as possible, Query and BinaryQuery are
  ## implemented using separate functions rather than trying to combine the
  ## different behaviors into a single function.
  
  ## For backward compatibility: defer to Query.httpapi if binary==FALSE
  binary <- if (is.null(binary)) TRUE else binary
  if (!binary) {
    return(Query.httpapi(db, query_or_scidb, 
                        binary=FALSE, `return`=TRUE, arrow=FALSE))
  }
  
  only_attributes <- if (is.null(only_attributes)) FALSE else only_attributes
  
  Connect.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  query <- .GetQueryString(query_or_scidb)
  
  is_debug <- getOption("scidb.debug", FALSE)
  use_int64 <- conn$int64
  result_size_limit_mb <- getOption("scidb.result_size_limit", 256)
  result_size_limit_bytes <- result_size_limit_mb * 1000000
  format <- if (only_attributes) "binary" else "binary+"

  ## Start a new query
  if (is_debug) {
    message("[SciDBR] ", format, " data query: ", query)
  }
  timings <- c(start=proc.time()[["elapsed"]])
  http_query <- New.httpquery(conn, query, 
                              options=list(format=format, 
                                           pageSize=page_size))
  schema <- http_query$schema
  dimensions <- .dimsplitter(schema)
  attributes <- .attsplitter(schema)
  timings <- c(timings, prepare=proc.time()[["elapsed"]])
  
  ## Since this function fetches all the pages it's interested in
  ## before returning, we can close the query as soon as this function exits.
  on.exit(
    suspendInterrupts(
      tryCatch(Close.httpquery(http_query),
               error=function (err) {
                 warning("[SciDBR] Error while closing query ", http_query$id, 
                         "(ignored): ", err)
               })))
  
  ## These are the columns we expect to be encoded in the response:
  ## dimensions (if requested), followed by attributes
  cols <- NULL
  if (! only_attributes) {
    ## Use stringsAsFactors=FALSE because we want the dataframe to contain
    ## actual strings (instead of dictionary-encoding them, which would turn
    ## every column of the dataframe into an integer vector)
    cols <- data.frame(name=dimensions$name, type="int64", nullable=FALSE, 
                       stringsAsFactors=FALSE)
  }
  cols <- rbind(cols, attributes)
  
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

    if (is.null(raw_page) || !http_query$is_selective || page_nbytes == 0) {
      ## We already got the last page, or the result is empty.
      message("[SciDBR] Query page ", page_number, " returned empty result",
              "(page_bytes=", page_nbytes, ")")
      break
    }

    ## Convert the binary data to a dataframe, and add it to the answer
    page_df <- .Binary2df(raw_page, cols, buffer_size, use_int64)
    ans <- rbind(ans, page_df)
    timings[[paste0("parse_page_", page_number)]] <- proc.time()[["elapsed"]]

    page_ncols <- length(page_df)
    page_nrows <- if (page_ncols > 0) length(page_df[[1]]) else 0
    total_ncols <- length(ans)
    total_nrows <- if (total_ncols > 0) length(ans[[1]]) else 0

    if (is_debug) {
      message("[SciDBR] Query page ", page_number, " returned ",
              page_nbytes, " bytes, ", 
              page_ncols, " columns, ",
              page_nrows, " rows;",
              " total so far: ", 
              total_nbytes, " bytes, ",
              total_ncols, " columns, ",
              total_nrows, " rows")
    }
    
    ## If the number of results we received is less than the page size, 
    ## or if we didn't set a page size (which returns all results in one page),
    ## we know this is the last page. We can skip the final call to Next, since
    ## it will only return an empty page.
    if (is.null(page_size) || page_nrows < page_size) {
      if (is_debug) {
        message("[SciDBR] Query page ", page_number, " returned ", 
                page_nrows, " rows ",
                "(page_size=", if (is.null(page_size)) "NULL" else page_size,
                "); assuming this is the last page")
      }
      break
    }
  }
  if (is_debug) {
    .ReportTimings(paste0("[SciDBR] timings for query ", http_query$id, ":"), 
                   timings)
  }
  
  if (is.null(ans)) {
    ## The response had no data. Create an empty dataframe
    ## with the correct column types.
    if (is_debug) {
      message("[SciDBR] Query returned no data; constructing empty dataframe")
    }
    return(.Schema2EmptyDf(attributes, 
                           if (only_attributes) NULL else dimensions))
  }

  ## TODO: Uniqueify attribute names if !only_attributes
  ## TODO: do we need to permute dimension columns? Compare results to Shim
  
  return(ans)
}

## Use an S4 class to standardize fields for all upload methods
setClass("UploadDesc",
         slots=c(
           ## Name of the SciDB array that will result from the upload
           array_name="character",
           ## If TRUE, the SciDB array will be temporary (not versioned)
           temp="logical",
           ## Name of the file used for uploading
           filename="character",
           ## Name of each attribute
           attr_names="character",
           ## SciDB type of each attribute
           attr_types="character",
           ## Name of each dimension
           dim_names="character",
           ## Start coordinate of each dimension
           dim_start_coordinates="numeric",
           ## Chunk size of each dimension
           dim_chunk_sizes="numeric"
         ))

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
                           ...)
{
  args <- list(...)
  
  if (is.null(name)) {
    name <- tmpnam(db)
  }
  ## Sanitize the array name
  name <- gsub("[^[:alnum:]]", "_", name)

  Connect.httpapi(db, reconnect=FALSE)
  conn <- attr(db, "connection")
  
  ## Collect "name" and implicit args into an upload descriptor
  desc <- new("UploadDesc")
  desc@array_name <- name
  desc@temp <- temp
  
  ## Generate an upload filename of the form:
  ##  upload.<connection-id>.<array-name>.<random-suffix>
  desc@filename <- basename(tempfile(sprintf("upload.%s.%s.", conn$id, name)))
  
  ## Convert legacy parameter names (deprecated) to the new names
  desc@attr_names <- as.character(attr_names %||% args$attr)
  desc@attr_types <- as.character(attr_types %||% args$type)
  desc@dim_names <- as.character(args$dim_names)
  desc@dim_start_coordinates <- as.numeric(dim_start_coordinates 
                                           %||% args$start)
  desc@dim_chunk_sizes <- as.numeric(dim_chunk_sizes %||% args$chunk_size)
  
  if (inherits(payload, "raw")) {
    .UploadRaw.httpapi(db, payload, desc)
  } else if (inherits(payload, "data.frame")) {
    .UploadDf.httpapi(db, payload, desc)
  } else if (inherits(payload, "dgCMatrix")) {
    .UploadMatrix.httpapi(db, payload, desc)
  } else {
    .UploadMatVec.httpapi(db, payload, desc)
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
  query$schema <- json[["schema"]]
  query$is_selective <- json[["isSelective"]]
  
  ## When the query gets garbage-collected, call Close.httpquery(query)
  reg.finalizer(query, Close.httpquery, onexit=TRUE)
  
  return(query)
}

Close.httpquery <- function(query)
{
  suspendInterrupts({
    is_debug <- getOption("scidb.debug", FALSE)
    if (is.null(query$location) || is.null(query$connection)) {
      if (is_debug) {
        message("[SciDBR] Query ", query$id, " already closed")
      }
      return(invisible(NULL));
    }
    
    uri <- URI(query$connection, query$location)
    if (is_debug) {
      message("[SciDBR] Closing SciDB query ", query$id, " url=", uri)
    }
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
  is_debug <- getOption("scidb.debug", FALSE)
  if (is.null(query$next_page_url) || is.null(query$location)) {
    if (is_debug) {
      message("[SciDBR] Query ", query$id, " already finished")
    }
    return(NULL)
  }
  if (is.null(query$connection)) {
    stop("Query ", query$id, " has no connection")
  }
  
  ## Get the next page of the query by following the query$next URL
  uri <- URI(query$connection, query$next_page_url)
  if (is_debug) {
    message("[SciDBR] Fetching page ", query$next_page_number, 
            " of query ", query$id)
  }
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
.CloseHttpApiSession <- function(conn)
{
  suspendInterrupts({
    is_debug <- getOption("scidb.debug", FALSE)
    if (is.null(conn$location)) {
      if (is_debug) {
        message("[SciDBR] Session ", conn$session, " already closed")
      }
      return(invisible(NULL));
    }
    
    uri <- URI(conn, conn$location)
    if (is_debug) {
      message("[SciDBR] Closing SciDB session ", conn$session, " url=", uri)
    }
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
  is_debug = getOption("scidb.debug", FALSE)
  conn <- .GetConnectionEnv(db_or_conn)

  ## data must be NULL or a 1-element vector containing a character element
  stopifnot(length(data) <= 1)
  
  if (!inherits(uri, "URI")) {
    stop("String '", uri, "' is not of class URI;",
         " please use the URI() function to construct it.")
  }
  uri <- .SanitizeUri(uri)
  
  ## Reset the connection handle so we can reuse it for the new request.
  ## Cookies are preserved (they don't get erased by handle_reset()).
  ## If there is no handle or if it is dead, create a new handle.
  tryCatch(curl::handle_reset(conn$handle), 
           error=function(err) {conn$handle <- curl::new_handle()})
  h <- conn$handle
  
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
    ssl_verifypeer=0)
  )
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
      if (is_debug) {
        message("[SciDBR] adding attachment ", iatt, " (name=", name, 
                ", content-type=", att$content_type, 
                ") to pending POST, with data:\n  ", att$data)
      }
    }
    do.call(curl::handle_setform, setform_args)
  }

  # curl::handle_setheaders(h, .list=list(Authorization=digest_auth(conn, action, uri)))
  if (is_debug) {
    if (method == "POST") {
      message("[SciDBR] sending ", method, " ", uri, "\n  data=", data,
              if (is.null(attachments)) "" 
              else sprintf(", with %d attachments", length(attachments)))
    } else {
      message("[SciDBR] sending ", method, " ", uri)
    }
  }
  
  resp <- curl::curl_fetch_memory(uri, h)
  if (resp$status_code > 299) {
    ## The server responded with an error
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
  
  if (is_debug) {
    message("[SciDBR] received headers: ", 
            paste(curl::parse_headers(resp$headers), collapse=" | "))
    if (length(resp$content) > 0) {
      is_text <- .HttpResponseIsText(resp)
      message("[SciDBR] received body", 
              if (!is_text) " (binary)", 
              ":\n>>>>>\n  ",
              if (is_text) rawToChar(resp$content) else resp$content,
              "\n<<<<<")
    }
  }
  
  return(resp)
}

#' Given an HTTP response, return TRUE if the content is text.
#' @param resp the result of a call to curl_fetch_memory
#' @return TRUE iff the resp$content is text-formatted, i.e. if we can use 
#'  rawToChar(resp$content) without an error.
.HttpResponseIsText <- function(resp)
{
  ## The slow way: look for non-printing characters
  return(length(grepRaw("[^[:print:][:space:]]", resp$content)) == 0)
  
  ## TODO: Check the Content-Type header instead.
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
  for (ii in seq(2, length(timings))) {
    if (ii > 2) { 
      msg <- paste0(msg, ",") 
    }
    msg <- paste(msg, names(timings)[[ii]], 
                 timings[[ii]] - timings[[ii-1]])
  }
  message(msg)
}

#' Upload an R raw value to a special 1-element SciDB array
#' @see .raw2scidb.shim()
.UploadRaw.httpapi <- function(db, payload, desc)
{
  is_debug <- getOption("scidb.debug", FALSE)
  if (!is.raw(payload)) stop("payload must be a raw value")
  if (!is.present(desc)) stop("No upload descriptor provided")
  if (!has.chars(only(desc@array_name))) stop ("No array name provided")
  if (!has.chars(only(desc@filename))) stop("No upload filename provided")

  timings <- c(start=proc.time()[["elapsed"]])
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
  
  if (is_debug) {
    .ReportTimings(paste0("[SciDBR] timings for raw upload to array '", 
                          only(desc@array_name), "':"), 
                   timings)
  }
}

.UploadDf.httpapi <- function(db, payload, desc,
                              use_aio_input=FALSE)
{
  timings <- c(start=proc.time()[["elapsed"]])
  is_debug <- getOption("scidb.debug", FALSE)
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
  data <- charToRaw(.TsvWrite(payload, file=.Primitive("return")))
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
                     paste(desc@attr_names, collapse=","),
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
  
  if (is_debug) {
    .ReportTimings(paste0("[SciDBR] timings for dataframe upload to array '", 
                          only(desc@array_name), "':"), 
                   timings)
  }
}

.UploadMatrix.httpapi <- function(db, payload, name, start, ...)
{
  stop("Not implemented")
}

.UploadMatVec.httpapi <- function(db, payload, name, start, ...)
{
  stop("Not implemented")
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
    on.exit(if (!success) {Execute(db, sprintf("remove(%s)", desc@array_name))},
            add=TRUE)
  }

  ## Write the query JSON
  query_and_options <- list(query=query,
                            attachment=desc@filename)
  query_json <- jsonlite::toJSON(query_and_options, auto_unbox=TRUE)
  
  ## Collect the attachments: a "query" attachment with the query JSON,
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
