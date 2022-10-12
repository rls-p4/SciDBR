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

  ## We don't need to return db because the only object we have modified
  ## is attr(db, "connection") which is an env (pass-by-reference).
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

#' See BinaryQuery()
BinaryQuery.shim = function(db, query_or_scidb, 
                            binary=NULL, buffer_size=NULL,
                            only_attributes=NULL, schema=NULL, ...)
{
  DEBUG = FALSE
  INT64 = attr(db, "connection")$int64
  DEBUG = getOption("scidb.debug", FALSE)
  AIO = getOption("scidb.aio", FALSE)
  RESULT_SIZE_LIMIT = getOption("scidb.result_size_limit", 256)
  if (DEBUG) {
    if (is.null(attr(db, "connection")$session)) {
      stop("[Shim session] unexpected in long running shim session")
    }
  }
  query = query_or_scidb
  lazyeval_ret = lazyeval(db, query)
  only_attributes = if (is.null(only_attributes)) {
    dist = lazyeval_ret$distribution
    if(is.na(dist)) FALSE else if(dist == 'dataframe') TRUE else FALSE
  } else {
    only_attributes
  }
  if (is.null(binary)) binary = TRUE
  if (!inherits(query, "scidb"))
  {
    # make a scidb object out of the query, optionally using a supplied schema to skip metadata query
    if (is.null(schema)) {
      query = if(is.na(lazyeval_ret$schema)) {
        scidb(db, query)
      } else {
        scidb(db, query, schema=lazyeval_ret$schema)
      }
    } else {
      query = scidb(db, query, schema=schema)
    }
  }
  attributes = schema(query, "attributes")
  dimensions = schema(query, "dimensions")
  query = query@name
  if(! binary) return(iquery(db, query, binary=FALSE, `return`=TRUE, arrow=FALSE))
  if (only_attributes)
  {
    internal_attributes = attributes
    internal_query = query
  } else {
    dim_names = dimensions$name
    attr_names = attributes$name
    all_names = c(dim_names, attr_names)
    internal_query = query
    if (length(all_names) != length(unique(all_names)))
    {
      # Cast to completely unique names to be safe:
      cast_dim_names = make.names_(dim_names)
      cast_attr_names = make.unique_(cast_dim_names, make.names_(attributes$name))
      cast_schema = sprintf("<%s>[%s]", paste(paste(cast_attr_names, attributes$type, sep=":"), collapse=","), paste(cast_dim_names, collapse=","))
      internal_query = sprintf("cast(%s, %s)", internal_query, cast_schema)
      all_names = c(cast_dim_names, cast_attr_names)
      dim_names = cast_dim_names
    }
    # Apply dimensions as attributes, using unique names. Manually construct the list of resulting attributes:
    dimensional_attributes = data.frame(name=dimensions$name, type="int64", nullable=FALSE) # original dimension names (used below)
    internal_attributes = rbind(attributes, dimensional_attributes)
    if (AIO == FALSE)
    {
      dim_apply = paste(dim_names, dim_names, sep=",", collapse=",")
      internal_query = sprintf("apply(%s, %s)", internal_query, dim_apply)
    }
  }
  ns = rep("", length(internal_attributes$nullable))
  ns[internal_attributes$nullable] = "null"
  format_string = paste(paste(internal_attributes$type, ns), collapse=",")
  format_string = sprintf("(%s)", format_string)
  
  ## Start running the query
  if (DEBUG) message("Data query ", internal_query)
  if (DEBUG) message("Format ", format_string)
  sessionid = scidbquery(
    db,
    internal_query,
    save=format_string,
    result_size_limit=RESULT_SIZE_LIMIT,
    atts_only=ifelse(only_attributes, TRUE, ifelse(AIO, FALSE, TRUE)))
  if (!is.null(attr(db, "connection")$session)) { # if session already exists
    release = 0
  } else { # need to get new session every time
    release = 1;
  }
  if (release) on.exit( SGET(db, "/release_session", list(id=sessionid), err=FALSE), add=TRUE)

  ## Fetch the query output data in binary format  
  dt2 = proc.time()
  uri = URI(db, "/read_bytes", list(id=sessionid, n=0))
  h = new_handle()
  handle_setheaders(h, .list=list(`Authorization`=digest_auth(db, "GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0, http_version=2))
  resp = curl_fetch_memory(uri, h)
  if (resp$status_code > 299) stop("HTTP error", resp$status_code)
  if (DEBUG) message("Data transfer time ", round((proc.time() - dt2)[3], 4))

  ## Unpack the binary data into a data.table
  dt1 = proc.time()
  ans = .Binary2df(resp$content, internal_attributes, buffer_size, INT64)
  if (DEBUG) message("Total R parsing time ", round( (proc.time() - dt1)[3], 4))
  
  ## If there was no data, return an empty dataframe with the query's schema
  if (is.null(ans)) {
    return(.Schema2EmptyDf(attributes,
                           if (only_attributes) NULL else dimensions))
  }
  
  ## Permute dimension cols, see issue #125
  if (only_attributes) {
    colnames(ans) = make.names_(attributes$name)
  } else {
    nd = length(dimensions$name)
    i = ncol(ans) - nd
    ans = ans[, c( (i+1):ncol(ans), 1:i)]
    colnames(ans) = make.names_(c(dimensions$name, attributes$name))
  }
  
  return(ans)
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

