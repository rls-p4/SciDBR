###
### @P4COPYRIGHT_SINCE:2022@
###

#'
#' @file httpapi.R
#' @brief Implementation layer for calls to the Shim, particularly S3 methods
#'    on class "shim"
#'

#' @see GetServerVersion()
#' @noRd
GetServerVersion.shim <- function(db)
{
  version = SGET(db, "/version")
  attr(db, "connection")$scidb.version <- ParseVersion(version)
  
  ## If we got this far, the request succeeded - so the connected host and port
  ## is for the Shim.
  class(db) <- c("shim", "afl")
  
  return(db)
}

#' @see Connect()
#' @noRd
Connect.shim <- function(db)
{
  ## Run an arbitrary query via scidbquery().
  ## scidbquery() creates a new session, executes the query, and returns 
  ## a list(response=, session=) where session is the Shim session ID, and
  ## response is the query ID (because that's what the Shim outputs in its
  ## HTTP response to any query).
  ## We don't care about the query result. (TODO: Is it leaked?)
  x <- tryCatch(scidbquery(db, query="list('libraries')", resp=TRUE),
                error=function(e) stop("Connection error ", e),
                warning=invisible)
  
  ## Register the session to be closed when db's "connection" env
  ## gets garbage-collected.
  reg.finalizer(attr(db, "connection"), 
                .CloseShimSession, 
                onexit=TRUE)
  
  ## Get the id of the query we just ran.
  query_id <- tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
                       error=function(e) stop("Connection error ", e),
                       warning=invisible)
  
  ## Give the connection the session ID.
  attr(db, "connection")$session <- x$session
  ## Give the connection a unique ID - in this case just use the query ID.
  attr(db, "connection")$id <- query_id[[length(query_id)]]
  ## Should not use password going forward (session is stored)
  attr(db, "connection")$password <- NULL
  
  ## Return the modified db object
  return(db)
}

#' @see Close()
#' @noRd
Close.shim <- function(db)
{
  .CloseShimSession(attr(db, "connection"))
}

#' @see iquery(), Query()
#' @noRd
Query.shim = function(db, query, `return`=FALSE, binary=TRUE, arrow=FALSE, ...)
{
  DEBUG = getOption("scidb.debug", TRUE)  # rsamuels TODO change to FALSE
  if (inherits(query, "scidb"))  query = query@name
  n = -1    # Indicate to shim that we want all the output
  
  if(binary && arrow) {
    stop("Only one of `binary` and `arrow` can be true")
  }
  
  session = NULL; release = 1; 
  if (!is.null(attr(db, "connection")$session)) {
    session = attr(db, "connection")$session
    release = 0
  }
  
  if (`return`) {
    if (arrow) return(scidb_arrow_to_dataframe(db, query, ...))
    if (binary) return(scidb_unpack_to_dataframe(db, query, ...))
    
    ans = tryCatch(
      {
        # SciDB save syntax changed in 15.12
        if (at_least(attr(db, "connection")$scidb.version, 15.12)) {
          sessionid = scidbquery(db, query, save="csv+:l")
        } else {
          sessionid = scidbquery(db, query, save="csv+")
        }
        dt1 = proc.time()
        result = tryCatch(
          {
            SGET(db, "/read_lines", list(id=sessionid, n=as.integer(n + 1)))
          },
          error=function(e)
          {
            SGET(db, "/cancel", list(id=sessionid))
            if (release) SGET(db, "/release_session", list(id=sessionid), err=FALSE)
            stop(e)
          })
        if (release) {
          SGET(db, "/release_session", list(id=sessionid), err=FALSE)
        }
        if (DEBUG) message("Data transfer time ", round((proc.time() - dt1)[3], 4))
        dt1 = proc.time()
        ## Convert the CSV data to an R dataframe
        ret <- .Csv2df(result, ...)
        if (DEBUG) message("R parsing time ", round((proc.time()-dt1)[3], 4))
        ret
      }, 
      error = function(e) {
        stop(e)
      }, 
      warning=invisible)
    return(ans)
  } else {
    scidbquery(db, query, stream=0L)
  }
  invisible()
}

#' Basic low-level query. Returns query id. This is an internal function.
#' @param db scidb database connection object
#' @param query a character query string
#' @param save format string for save() query, or NULL.
#'   Example values: "dcsv", "csv+", "(double NULL, int32)"
#' @param session if you already have a SciDB http session, set this to it,
#'    otherwise NULL
#' @param resp if true, return http response
#' @param stream set to 0L or 1L to control streaming (NOT USED)
#' @param prefix optional AFL statement to prefix query in the same connection 
#'    context
#' @return list(sessionid, response) if resp==TRUE, otherwise just sessionid
#' @noRd
scidbquery = function(db, query, 
                      save=NULL, 
                      result_size_limit=NULL, 
                      session=NULL, 
                      resp=FALSE, 
                      stream,
                      prefix=attributes(db)$connection$prefix, 
                      atts_only=TRUE)
{
  DEBUG = FALSE
  STREAM = 0L
  DEBUG = getOption("scidb.debug", FALSE)
  if (missing(stream))
  {
    STREAM = 0L
  } else STREAM = as.integer(stream)
  release = 0
  if (!is.null(attr(db, "connection")$session)) {
    session = attr(db, "connection")$session
  } else {
    if (DEBUG) message("[Shim session] created new session")
  }
  sessionid = session
  if (is.null(session))
  {
    sessionid = getSession(db) # Obtain a session from shim
  }
  if (is.null(save)) save=""
  if (is.null(result_size_limit)) result_size_limit=""
  if (DEBUG)
  {
    message(query)
    t1 = proc.time()
  }
  ans = tryCatch(
    {
      args = list(id=sessionid, afl=0L, query=query, stream=0L)
      args$release = release
      args$prefix = c(getOption("scidb.prefix"), prefix)
      if (!is.null(args$prefix)) args$prefix = paste(args$prefix, collapse=";")
      args$save = save
      args$result_size_limit = result_size_limit
      if (!is.null(args$save)) args$atts_only=ifelse(atts_only, 1L, 0L)
      do.call("SGET", args=list(db=db, resource="/execute_query", args=args))
    }, error=function(e)
    {
      SGET(db, "/cancel", list(id=sessionid), err=FALSE)
      if (release) SGET(db, "/release_session", list(id=sessionid), err=FALSE)
      e$call = NULL
      stop(e)
    }, interrupt=function(e)
    {
      SGET(db, "/cancel", list(id=sessionid), err=FALSE)
      if (release) SGET(db, "/release_session", list(id=sessionid), err=FALSE)
      stop("cancelled")
    }, warning=invisible)
  if (DEBUG) message("Query time ", round( (proc.time() - t1)[3], 4))
  if (resp) return(list(session=sessionid, response=ans))
  sessionid
}

# Normally called with raw data and args=list(id=whatever)
POST = function(db, data, args=list(), err=TRUE)
{
  # check for new shim simple post option (/upload), otherwise use
  # multipart/file upload (/upload_file)
  shimspl = strsplit(attr(db, "connection")$scidb.version, "\\.")[[1]]
  shim_yr = tryCatch(as.integer(gsub("[A-z]", "", shimspl[1])), error=function(e) 16, warning=function(e) 8)
  shim_mo = tryCatch(as.integer(gsub("[A-z]", "", shimspl[2])), error=function(e) 16, warning=function(e) 8)
  if (is.na(shim_yr)) shim_yr = 16
  if (is.na(shim_mo)) shim_mo = 8
  simple = (shim_yr >= 15 && shim_mo >= 7) || shim_yr >= 16
  if (simple)
  {
    uri = URI(db, "/upload", args)
    uri = oldURLencode(uri)
    uri = gsub("\\+", "%2B", uri, perl=TRUE)
    h = new_handle()
    handle_setheaders(h, .list=list(Authorization=digest_auth(db, "POST", uri)))
    handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                                ssl_verifypeer=0, post=TRUE, http_version=2, postfieldsize=length(data),
                                postfields=data))
    ans = curl_fetch_memory(uri, h)
    if (ans$status_code > 299 && err) stop("HTTP error ", ans$status_code)
    return(rawToChar(ans$content))
  }
  uri = URI(db, "/upload_file", args)
  uri = oldURLencode(uri)
  uri = gsub("\\+", "%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth(db, "POST", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0, http_version=2))
  tmpf = tempfile()
  if (is.character(data)) data = charToRaw(data)
  writeBin(data, tmpf)
  handle_setform(h, file=form_file(tmpf))
  ans = curl_fetch_memory(uri, h)
  unlink(tmpf)
  if (ans$status_code > 299 && err) stop("HTTP error", ans$status_code)
  return(rawToChar(ans$content))
}

#' Close the Shim session.
#' This is registered as a finalizer on attr(db, "connection") so it closes 
#' the session when the connection gets garbage-collected; it can also be called
#' from Close.shim().
#' @param conn the connection environment, usually obtained from 
#'    attr(db, "connection")
#' @noRd
.CloseShimSession <- function(conn) 
{
  is_debug = getOption("scidb.debug", TRUE)  # rsamuels TODO: change to FALSE
  if (is.null(conn$session)) {
    if (is_debug) {
      message("[Shim session] Session already closed. Nothing to do here.")
    }
    return();
  }
  
  if (is_debug) {
    message("[Shim session] automatically cleaning up db session ", 
            conn$session)
  }
  SGET(conn, "/release_session", list(id=conn$session), err=FALSE)
  
  ## Set the session to NULL so we don't double-release the session.
  ## Because conn is an env, this side effect should persist
  ## outside of the current function call.
  conn$session <- NULL
}

