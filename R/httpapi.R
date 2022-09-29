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

#' Given a relative path to an endpoint within httpapi,
#' return the full URL/URI for the endpoint, including server DNS and port.
#' @param db_or_conn scidb database connection object,
#'     _or_ its "connection" attribute
#' @param path the path to the endpoint, 
#'     relative to "http[s]://host/api/v{version}/". 
#'     It should not begin with a slash.
#' @param args optional key=value args to add to the URL.
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

.SanitizeUri <- function(uri)
{
  uri <- oldURLencode(uri)
  uri <- gsub("\\+", "%2B", uri, perl=TRUE)
  return(uri)
}

.HttpRequest = function(db_or_conn, method, uri, data=NULL)
{
  is_debug = getOption("scidb.debug", TRUE) # rsamuels TODO: change to FALSE
  conn <- .GetConnectionEnv(db_or_conn)
  h <- conn$handle
  if (is.null(h)) {
    stop("no connection handle")
  }
  
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
    POST = list(post=TRUE, postfieldsize=length(data), postfields=data),
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
  
  response <- curl::curl_fetch_memory(uri, h)
  if (response$status_code > 299) {
    stop(sprintf("HTTP error %s\n%s", 
                 response$status_code, 
                 rawToChar(response$content)))
  }
  
  if (is_debug) {
    message("[SciDBR] received headers: ", 
            paste(curl::parse_headers(response$headers), collapse=" | "))
    if (length(response$content) > 0) {
      message("[SciDBR] received body:\n>>>>>\n  ",
              rawToChar(response$content),
              "\n<<<<<")
    }
  }
  
  return(response)
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

#' @see GetServerVersion()
GetServerVersion.httpapi <- function(db)
{
  if (is.null(attr(db, "connection")$handle)) {
    attr(db, "connection")$handle <- curl::new_handle()
  }
  
  response <- .HttpRequest(db, "GET", .EndpointUri(db, "version"))
  stopifnot(response$status_code == 200)  # 200 OK
  json <- jsonlite::fromJSON(rawToChar(response$content))
  
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
  response <- .HttpRequest(db, "POST", .EndpointUri(db, "sessions"))
  stopifnot(response$status_code == 201)  # 201 Created
  json <- jsonlite::fromJSON(rawToChar(response$content))
  
  ## Response JSON contains the session ID
  sid <- json[["sid"]]

  ## Parse the Location and Link headers and store them
  headers_list <- curl::parse_headers_list(response$headers)
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
  reg.finalizer(attr(db, "connection"), 
                .CloseHttpApiSession, 
                onexit=TRUE)
  
  ## Return the modified db object
  return(db)
}

#' Close the session.
#' This is registered as a finalizer on attr(db, "connection") so it closes 
#' the session when the connection gets garbage-collected; it can also be called
#' from Close.httpapi().
#' @param conn the connection environment, usually obtained from 
#'    attr(db, "connection")
.CloseHttpApiSession <- function(conn)
{
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
    .HttpRequest(conn, "DELETE", uri),
    error=function(err) {
      warning("[SciDBR] Error closing SciDB session ", conn$session, 
              " (ignored): ", err)
    })

  ## Set the location to NULL so we don't double-delete the session.
  ## Because conn is an env, this side effect should immediately take effect
  ## outside of the current function call.
  conn$location <- NULL
}


#' @see Close()
Close.httpapi <- function(db)
{
  .CloseHttpApiSession(attr(db, "connection"))
}