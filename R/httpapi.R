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

#' @see GetServerVersion()
#' @noRd
GetServerVersion.httpapi <- function(db)
{
  if (is.null(attr(db, "connection")$handle)) {
    attr(db, "connection")$handle <- curl::new_handle()
  }
  
  resp <- .HttpRequest(db, "GET", .EndpointUri(db, "version"))
  stopifnot(resp$status_code == 200)  # 200 OK
  json <- jsonlite::fromJSON(rawToChar(resp$content))
  
  attr(db, "connection")$scidb.version <- ParseVersion(json[["scidbVersion"]])

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
  attr(db, "connection")$httpapi_version <- max(compatible_versions)
  
  ## If we got this far, the request succeeded - so the connected host and port
  ## supports the SciDB httpapi (no Shim).
  class(db) <- c("httpapi", "afl")
  
  return(db)
}

#' @see Connect()
#' @noRd
Connect.httpapi <- function(db)
{
  ## Create a new connection handle.
  ## Because we reuse one connection handle for all queries in a session,
  ## curl automatically deals with authorization cookies sent from the server -
  ## sending them back to the server in each request, and keeping them 
  ## up to date when the server sends new cookies.
  if (is.null(attr(db, "connection")$handle)) {
    attr(db, "connection")$handle <- curl::new_handle()
  }

  ## Post to /api/sessions to create a new session  
  resp <- .HttpRequest(db, "POST", .EndpointUri(db, "sessions"))
  stopifnot(resp$status_code == 201)  # 201 Created
  json <- jsonlite::fromJSON(rawToChar(resp$content))
  
  ## Response JSON contains the session ID
  sid <- json[["sid"]]

  ## Parse the Location and Link headers and store them
  headers_list <- curl::parse_headers_list(resp$headers)
  attr(db, "connection")$location <- headers_list[["location"]]
  attr(db, "connection")$links <- .ParseLinkHeaders(headers_list)
  
  ## Give the connection the session ID.
  attr(db, "connection")$session <- sid
  ## Give the connection a unique ID - in this case just reuse the session ID.
  attr(db, "connection")$id <- sid
  ## Should not use password going forward: use the authorization cookies
  ## that were returned by the server and which curl automatically saves
  ## on the connection handle.
  attr(db, "connection")$password <- NULL

  ## Register the session to be closed when db's "connection" env
  ## gets garbage-collected.
  reg.finalizer(attr(db, "connection"), .CloseHttpApiSession, onexit=TRUE)
  
  ## Return the modified db object
  return(db)
}

#' @see Close()
#' @noRd
Close.httpapi <- function(db)
{
  .CloseHttpApiSession(attr(db, "connection"))
}

#' @see iquery()
#' @noRd
Query.httpapi <- function(db, query, 
                          `return`=FALSE, 
                          binary=TRUE, 
                          arrow=FALSE, 
                          ...)
{
  is_debug = getOption("scidb.debug", TRUE)  # rsamuels TODO change to FALSE
  
  conn <- attr(db, "connection")
  if (is.null(conn)) { stop("no connection") }
  if (is.null(query) || is.na(query)) { stop("no query") }

  if (inherits(query, "scidb")) {
    query <- query@name
  }
  
  if (binary) {
    stop("binary==TRUE not supported yet")
  }
  if (arrow) {
    stop("arrow==TRUE not supported yet")
  }
  
  if (is.null(conn$session) || is.null(conn$location) || is.null(conn$links)) {
    db <- Connect.httpapi(db)
  }

  ## Start a new query
  timings <- c(start=proc.time()[["elapsed"]])
  query <- New.httpquery(conn, query, options=list(format="csv+:l"))
  timings <- c(timings, prepare=proc.time()[["elapsed"]])
  
  ## Since this function currently fetches all of its results in one page,
  ## we can close the query as soon as this function exits.
  on.exit(
    suspendInterrupts(
      tryCatch(Close.httpquery(query),
               error=function (err) {
                 warning("[SciDBR] Error while closing query ", query$id, 
                         "(ignored): ", err)
               })))
  
  ## TODO: In the future, once we have confirmed that all application code uses
  ## `return` in the expected way (i.e. nobody sets return=FALSE to ignore data 
  ## from a selective query), we can deprecate the `return` argument, ignore it,
  ## and just use is_selective instead.
  if (`return` == FALSE && query$is_selective) {
    stop("`return` argument must be TRUE for a selective query")
  }
  if (`return` == TRUE && !query$is_selective) {
    stop("`return` argument must be FALSE for a DDL or update query")
  }
  
  ## Fetch the first (and only) page from the query.
  ## We must do this even if return==FALSE, because any DDL action only 
  ## gets committed if the query is fetched to completion.
  ## If there is an error, the on.exit() above will close the query,
  ## and the error will be propagated to the caller.
  raw_page <- Next.httpquery(query)
  timings <- c(timings, fetch=proc.time()[["elapsed"]])
  
  df <- NULL
  if (`return` && query$is_selective && !is.null(page)) {
    ## Convert the CSV data to an R dataframe
    df <- .Csv2df(rawToChar(raw_page))
    timings <- c(timings, parse_csv=proc.time()[["elapsed"]])
  }
  
  ## Report on timings
  if (is_debug) {
    msg <- paste0("[SciDBR] timings for query ", query$id, ":")
    for (ii in seq(2, length(timings))) {
      if (ii > 2) { 
        msg <- paste0(msg, ",") 
      }
      msg <- paste(msg, names(timings)[[ii]], 
                   timings[[ii]] - timings[[ii-1]])
    }
    message(msg)
  }
  
  return(if (is.null(df)) invisible(NULL) else df)
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
    is_debug <- getOption("scidb.debug", TRUE)  # rsamuels TODO change to FALSE
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
.HttpRequest = function(db_or_conn, method, uri, data=NULL)
{
  is_debug = getOption("scidb.debug", TRUE) # rsamuels TODO: change to FALSE
  conn <- .GetConnectionEnv(db_or_conn)
  h <- conn$handle
  if (is.null(h)) {
    stop("no connection handle")
  }
  ## data must be NULL or a 1-element vector containing a character element
  stopifnot(length(data) <= 1)
  
  if (!inherits(uri, "URI")) {
    stop("String '", uri, "' is not of class URI;",
         " please use the URI() function to construct it.")
  }
  uri <- .SanitizeUri(uri)
  
  ## Reset the connection handle so we can reuse it for the new request.
  ## Cookies are preserved (they don't get erased by handle_reset()).
  curl::handle_reset(h)
  
  options <- switch(method, 
                    GET = list(httpget=TRUE),
                    POST = list(post=TRUE, 
                                postfieldsize=(
                                  if (is.null(data)) 0 else nchar(data)),
                                postfields=data),
                    DELETE = list(customrequest="DELETE"),
                    stop(sprintf("unsupported HTTP method %s", method))
  )
  options <- c(options, list(
    http_version=2,
    ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
    ssl_verifypeer=0)
  )
  curl::handle_setopt(h, .list=options)
  
  # curl::handle_setheaders(h, .list=list(Authorization=digest_auth(conn, action, uri)))
  if (is_debug) {
    if (method == "POST") {
      message("[SciDBR] sending ", method, " ", uri, "\n  data=", data)
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
      ## Add potentially useful client-side information to the JSON
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
      message("[SciDBR] received body:\n>>>>>\n  ",
              rawToChar(resp$content),
              "\n<<<<<")
    }
  }
  
  return(resp)
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

New.httpquery <- function(conn, afl, page_size=NULL, options=list())
{
  ## Prepare the POST content
  query_and_options <- append(list(query=afl), options)
  if (!is.null(page_size)) {
    query_and_options <- append(query_and_options, list(pageSize=page_size))
  }
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
  query$page_size <- page_size
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
    is_debug <- getOption("scidb.debug", TRUE)  # rsamuels TODO change to FALSE
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
  is_debug <- getOption("scidb.debug", TRUE)  # rsamuels TODO change to FALSE
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