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
  version = SGET.shim(db, "/version")
  attr(db, "connection")$scidb.version <- ParseVersion(version)
  
  ## If we got this far, the request succeeded - so the connected host and port
  ## is for the Shim.
  class(attr(db, "connection")) <- "shim"
  class(db) <- c("shim", "afl")
  
  return(db)
}

#' @see NewSession()
#' @noRd
NewSession.shim <- function(db, auth_type=NULL)
{
  ## Check for login using either scidb or HTTP digest authentication
  conn = attr(db, "connection")
  if (has.chars(conn$username)) {
    stopifnot(has.chars(auth_type))
    conn$authtype = auth_type
    if (auth_type=="scidb") {
      conn$protocol = "https"
    } else {
      ## HTTP basic digest auth
      conn$digest = paste(username, password, sep=":")
    }
  }
  
  ## Run an arbitrary query via scidbquery.shim().
  ## scidbquery.shim() creates a new session, executes the query, and returns 
  ## a list(response=, session=) where session is the Shim session ID, and
  ## response is the query ID (because that's what the Shim outputs in its
  ## HTTP response to any query).
  ## We don't care about the query result. (TODO: Is it leaked?)
  x <- tryCatch(scidbquery.shim(db, query="list('libraries')", resp=TRUE),
                error=function(e) stop("Connection error ", e),
                warning=invisible)
  
  ## Register the session to be closed when db's "connection" env
  ## gets garbage-collected.
  reg.finalizer(conn, 
                .CloseShimSession, 
                onexit=TRUE)
  
  ## Get the id of the query we just ran.
  query_id <- tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
                       error=function(e) stop("Connection error ", e),
                       warning=invisible)
  
  ## Give the connection the session ID.
  conn$session <- x$session
  ## Give the connection a unique ID - in this case just use the query ID.
  conn$id <- query_id[[length(query_id)]]
  ## Should not use password going forward (session is stored)
  conn$password <- NULL

  ## We don't need to return db because the only object we have modified
  ## is attr(db, "connection") which is an env (pass-by-reference).
}

EnsureSession.shim <- function(db_or_conn, ...)
{
  conn = attr(db, "connection")
  if (is.null(conn) || is.null(conn$session) || is.null(conn$id)) {
    NewSession.shim(db_or_conn)
  }
}

Reauthenticate.shim <- function(db, password, defer=FALSE)
{
  # Nothing to do here for shim
}

#' @see Close()
#' @noRd
Close.shim <- function(db)
{
  .CloseShimSession(attr(db, "connection"))
}

#' @see Execute()
#' @see scidbquery.shim()
#' @noRd
Execute.shim <- function(db, query_or_scidb, ...)
{
  scidbquery.shim(db, .GetQueryString(query_or_scidb), ...)
  invisible()
}

#' @see iquery(), Query()
#' @noRd
Query.shim = function(db, query_or_scidb,
                      `return`=FALSE, binary=TRUE, arrow=FALSE, ...)
{
  DEBUG = getOption("scidb.debug", FALSE)
  query = .GetQueryString(query_or_scidb)
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
    if (arrow) return(scidb_arrow_to_dataframe.shim(db, query, ...))
    if (binary) return(scidb_unpack_to_dataframe(db, query, ...))
    
    ans = tryCatch(
      {
        # SciDB save syntax changed in 15.12
        if (at_least(attr(db, "connection")$scidb.version, 15.12)) {
          sessionid = scidbquery.shim(db, query, save="csv+:l")
        } else {
          sessionid = scidbquery.shim(db, query, save="csv+")
        }
        dt1 = proc.time()
        result = tryCatch(
          {
            SGET.shim(db, "/read_lines", 
                      list(id=sessionid, n=as.integer(n + 1)))
          },
          error=function(e)
          {
            SGET.shim(db, "/cancel", list(id=sessionid))
            if (release) {
              SGET.shim(db, "/release_session", list(id=sessionid), err=FALSE)
            }
            stop(e)
          })
        if (release) {
          SGET.shim(db, "/release_session", list(id=sessionid), err=FALSE)
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
    scidbquery.shim(db, query, stream=0L)
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
  sessionid = scidbquery.shim(
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
  if (release) {
    on.exit(SGET.shim(db, "/release_session", list(id=sessionid), err=FALSE), 
            add=TRUE)
  }

  ## Fetch the query output data in binary format  
  dt2 = proc.time()
  uri = URI(db, "/read_bytes", list(id=sessionid, n=0))
  h = new_handle()
  handle_setheaders(h, .list=list(`Authorization`=.digest_auth(db, "GET", uri)))
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

  ## Special behavior for "binary" datatype: 
  ## if any column has type "binary", don't return a dataframe; instead, return
  ## a list containing just that binary value. Don't permute the columns
  ## to put the dimension(s) first; the binary value should be the first element
  ## in the returned list. See issue #163 and the test for issue #163 in 
  ## tests/a.R .
  if (typeof(ans) == "list" && "binary" %in% internal_attributes$type) {
    return(ans)
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

#' @see Upload()
Upload.shim = function(db, payload, name=NULL, ...)
{
  if (is.null(name)) {
    name <- tmpnam(db)
  }

  if (inherits(payload, "raw")) {
    X = (.raw2scidb.shim(db, payload, name=name, ...))
  } else if (inherits(payload, "data.frame")) {
    X = (.df2scidb.shim(db, payload, name=name, ...))
  } else if (inherits(payload, "dgCMatrix")) {
    X = (.matrix2scidb.shim(db, payload, name=name, ...))
  } else {
    X = (.matvec2scidb.shim(db, payload, name=name, ...))
  }
  return(X)
}

# Return a shim session ID or error
getSession.shim = function(db)
{
  session = SGET.shim(db, "/new_session")
  if (length(session)<1) stop("SciDB http session error; are you connecting to a valid SciDB host?")
  session = gsub("\r", "", session)
  session = gsub("\n", "", session)
  session
}

# Issue an HTTP GET request.
# db_or_conn: scidb database connection object _or_ its "connection" attribute
# resource (string): A URI identifying the requested service
# args (list): A list of named query parameters
# err (boolean): If true, stop if the server returned an error code
# binary (boolean): If true, return binary data, else convert to character data
SGET.shim = function(db_or_conn, resource, args=list(), err=TRUE, binary=FALSE)
{
  if (!(substr(resource, 1, 1)=="/")) resource = paste("/", resource, sep="")
  uri = URI(db_or_conn, resource, args)
  uri = oldURLencode(uri)
  uri = gsub("\\+", "%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=.digest_auth(db_or_conn, "GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0, http_version=2))
  ans = curl_fetch_memory(uri, h)
  if (ans$status_code > 299 && err)
  {
    msg = sprintf("HTTP error %s", ans$status_code)
    if (ans$status_code >= 400) msg = sprintf("%s\n%s", msg, rawToChar(ans$content))
    stop(msg)
  }
  if (binary) return(ans$content)
  rawToChar(ans$content)
}

URI.shim = function(db_or_conn, resource="", args=list())
{
  conn <- .GetConnectionEnv(db_or_conn)

  ## For the shim, we need to pass authentication and admin settings
  ## in the URL parameters.
  if (!is.null(conn$auth)) args = c(args, list(auth=conn$auth))
  if (!is.null(conn$password)) args = c(args, list(password=conn$password))
  if (!is.null(conn$username)) args = c(args, list(user=conn$username))
  if (!is.null(conn$admin) && conn$admin) args = c(args, list(admin=1))
  return(URI.default(db_or_conn, resource, args))
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
scidbquery.shim = function(db, query, 
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
    sessionid = getSession.shim(db) # Obtain a session from shim
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
      do.call("SGET.shim", 
              args=list(db=db, resource="/execute_query", args=args))
    }, error=function(e)
    {
      SGET.shim(db, "/cancel", list(id=sessionid), err=FALSE)
      if (release) {
        SGET.shim(db, "/release_session", list(id=sessionid), err=FALSE)
      }
      e$call = NULL
      stop(e)
    }, interrupt=function(e)
    {
      SGET.shim(db, "/cancel", list(id=sessionid), err=FALSE)
      if (release) {
        SGET.shim(db, "/release_session", list(id=sessionid), err=FALSE)
      }
      stop("cancelled")
    }, warning=invisible)
  if (DEBUG) message("Query time ", round( (proc.time() - t1)[3], 4))
  if (resp) return(list(session=sessionid, response=ans))
  sessionid
}

#' Return a SciDB query expression as a data frame
#' @param db scidb database connection object
#' @param query A SciDB query expression or scidb object
#' @param ... optional extra arguments (see below)
#' @note option extra arguments
#' \itemize{
#'   \item{only_attributes}{ optional logical value, TRUE if only attributes should be returned}
#'   \item{schema}{ optional result schema string }
#' }
#' @keywords internal
scidb_arrow_to_dataframe.shim = function(db, query, ...) {
  INT64 = attr(db, "connection")$int64
  DEBUG = getOption("scidb.debug", FALSE)
  RESULT_SIZE_LIMIT = getOption("scidb.result_size_limit", 256)
  AIO = getOption("scidb.aio", TRUE)
  
  if (inherits(db, "httpapi")) {
    stop("scidb_arrow_to_dataframe not yet implemented for httpapi")
  }
  
  if (!AIO) {
    stop("AIO Must be TRUE for Arrow")
  }
  
  args = list(...)
  
  # TODO: Look into this, but guarantees we have only_atts
  lazyeval_ret = lazyeval(db, query)
  args$only_attributes = if (is.null(args$only_attributes)) {
    dist = lazyeval_ret$distribution
    if(is.na(dist)) FALSE else if(dist == 'dataframe') TRUE else FALSE
  } else {
    args$only_attributes
  }
  
  # Get the atts and dims so we can filter the results
  if (!inherits(query, "scidb"))
  {
    # make a scidb object out of the query, optionally using a supplied schema to skip metadata query
    if (is.null(args$schema)) {
      query = if(is.na(lazyeval_ret$schema)) {
        scidb(db, query)
      } else {
        scidb(db, query, schema=lazyeval_ret$schema)
      }
    } else {
      query = scidb(db, query, schema=args$schema)
    }
  }
  
  attributes = schema(query, "attributes")
  dimensions = schema(query, "dimensions")
  query = query@name
  
  # Make the scidbquery
  if (DEBUG) message("Data query ", query)
  # if (DEBUG) message("Format ", format_string)
  sessionid = scidbquery.shim(
    db,
    query,
    save="arrow",
    result_size_limit=RESULT_SIZE_LIMIT,
    atts_only=ifelse(args$only_attributes, TRUE, ifelse(AIO, FALSE, TRUE)))
  if (!is.null(attr(db, "connection")$session)) { # if session already exists
    release = 0
  } else { # need to get new session every time
    release = 1;
  }
  if (release) {
    on.exit(SGET.shim(db, "/release_session", list(id=sessionid), err=FALSE), 
            add=TRUE)
  }
  
  dt2 = proc.time()
  uri = URI(db, "/read_bytes", list(id=sessionid, n=0))
  h = new_handle()
  handle_setheaders(h, .list=list(`Authorization`=.digest_auth(db, "GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0, http_version=2))
  resp = curl_fetch_memory(uri, h)
  
  if (resp$status_code > 299) stop("HTTP error", resp$status_code)
  if (DEBUG) message("Data transfer time ", round((proc.time() - dt2)[3], 4))
  if (DEBUG) message("Data size ", length(resp$content))
  dt1 = proc.time()
  
  res <- arrow::read_ipc_stream(resp$content, as_data_frame = T)
  if (DEBUG) message("Total R parsing time ", round( (proc.time() - dt1)[3], 4))
  
  # Reorganize
  if (!args$only_attributes) {
    res <- res[, c(dimensions$name, attributes$name)]
  }
  
  return(res)
}

#' Internal function to upload an R raw value to special 1-element SciDB array
#' @param db scidb database connection
#' @param X a raw value
#' @param name (character) SciDB array name
#' @param gc (boolean) set to \code{TRUE} to connect SciDB array to R's garbage collector
#' @param ... optional extra arguments
#' \itemize{
#' \item {temp:} {(boolean) create a temporary SciDB array}
#' }
#' @return a \code{\link{scidb}} object
#' @keywords internal
.raw2scidb.shim = function(db, X, name, gc=TRUE, ...)
{
  if (!is.raw(X)) stop("X must be a raw value")
  args = list(...)
  # Obtain a session from shim for the upload process
  if (!is.null(attr(db, "connection")$session)) { # if session already exists
    session = attr(db, "connection")$session
    release = 0
  } else { # need to get new session every time
    session = getSession.shim(db)
    if (length(session)<1) stop("SciDB http session error")
    release = 1;
  }
  if (release) {
    on.exit(SGET.shim(db, "/release_session", list(id=session), err=FALSE), 
            add=TRUE)
  }
  
  bytes = .Call(C_scidb_raw, X)
  ans = .Post.shim(db, bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))
  
  schema = "<val:binary null>[i=0:0:0:1]"
  if (!is.null(args$temp))
  {
    if (args$temp) create_temp_array(db, name, schema)
  }
  
  query = sprintf("store(input(%s,'%s',-2,'(binary null)'),%s)", schema, ans, name)
  iquery(db, query)
  return(scidb(db, name, gc=gc))
}

#' Internal function to upload an R data frame to SciDB
#' @param db scidb database connection
#' @param X a data frame
#' @param name SciDB array name
#' @param chunk_size optional value passed to the aio_input operator see https://github.com/Paradigm4/accelerated_io_tools
#' @param types SciDB attribute types
#' @param gc set to \code{TRUE} to connect SciDB array to R's garbage collector
#' @return a \code{\link{scidb}} object, or a character schema string if \code{schema_only=TRUE}.
#' @keywords internal
.df2scidb.shim = function(db, X,
                          name=tmpnam(db),
                          types=NULL,
                          use_aio_input=FALSE,
                          chunk_size=NULL,
                          gc=TRUE,
                          temp=FALSE,
                          start=NULL,
                          ...)
{
  if (!is.data.frame(X)) stop("X must be a data frame")

  ## Preprocess the dataframe to prepare it for upload
  processed = .PreprocessDfTypes(X, types, use_aio_input)
  X = processed$df
  ncolX = ncol(X)
  nrowX = nrow(X)
  anames = names(X)
  typ = processed$attr_types
  aio_apply_args = processed$aio_apply_args

  ## Serialize the dataframe to TSV format
  data = charToRaw(.TsvWrite(X, file=.Primitive("return")))

  ## Obtain a session from the SciDB http service for the upload process
  if (!is.null(attr(db, "connection")$session)) { 
    ## session already exists
    session = attr(db, "connection")$session
  } else { 
    ## need to get new session
    session = getSession.shim(db)
    if (length(session)<1) stop("SciDB http session error")
    on.exit(SGET.shim(db, "/release_session", list(id=session), err=FALSE),
            add=TRUE)
  }

  tmp = .Post.shim(db, data, list(id=session))
  tmp = gsub("\n", "", gsub("\r", "", tmp))
  
  ## Generate a load_tools query
  aio = length(grep("aio_input", names(db))) > 0
  if (use_aio_input && aio) {
    aioSettings = list(num_attributes = ncolX)
    if (!is.null(chunk_size)) {
      aioSettings[['chunk_size']] = chunk_size
    }
    LOAD = sprintf("project(apply(aio_input('%s', %s),%s),%s)", 
                   tmp,
                   get_setting_items_str(db, aioSettings), 
                   aio_apply_args,
                   paste(anames, collapse=","))
  } else {
    LOAD = sprintf("input(%s, '%s', -2, 'tsv')",
                   dfschema(anames, typ, nrowX, chunk=chunk_size, start=start), 
                   tmp)
  }
  
  if (temp) { 
    ## Use scidb temporary array instead of regular versioned array
    targetArraySchema = lazyeval(db, LOAD)$schema
    create_temp_array(db, name, schema = targetArraySchema)
  }
  
  query = sprintf("store(%s,%s)", LOAD, name)
  Execute(db, query, session=session)
  return(scidb(db, name, gc=gc))
}

#' Internal function to upload an R sparse matrix into SciDB
#' @param db scidb database connection
#' @param X a sparse matrix
#' @param name (character) SciDB array name
#' @param rowChuckSize,colChunkSize (int) optional value passed to the aio_input operator see https://github.com/Paradigm4/accelerated_io_tools
#' @param start (int) dimension start values
#' @param temp (boolean) create a temporary SciDB array
#' @param gc (boolean) set to \code{TRUE} to connect SciDB array to R's garbage collector
#' @return a \code{\link{scidb}} object
#' @keywords internal
.matrix2scidb.shim = function(db, X, name, 
                              rowChunkSize=1000, 
                              colChunkSize=1000,
                              start=NULL, 
                              temp=FALSE, 
                              gc=TRUE, 
                              ...)
{
  D = dim(X)
  if (is.null(start)) start=c(0, 0)
  if (length(start) < 1) stop ("Invalid starting coordinates")
  if (length(start) > 2) start = start[1:2]
  if (length(start) < 2) start = c(start, 0)
  start = as.integer(start)
  type = .scidbtypes[[typeof(X@x)]]
  if (is.null(type)) {
    stop(paste("Unsupported data type. The package presently supports: ",
               paste(.scidbtypes, collapse=" "), ".", sep=""))
  }
  if (type != "double") stop("Sorry, the package only supports double-precision sparse matrices right now.")
  schema = sprintf(
    "<val:%s null>[i=%.0f:%.0f:0:%.0f; j=%.0f:%.0f:0:%.0f]", 
    type, 
    start[[1]],
    nrow(X)-1+start[[1]],
    min(nrow(X), rowChunkSize), 
    start[[2]], 
    ncol(X)-1+start[[2]],
    min(ncol(X), colChunkSize))
  schema1d = sprintf("<i:int64 null, j:int64 null, val:%s null>[idx=0:*:0:100000]", type)
  
  # Compute the indices and assemble message to SciDB in the form
  # double, double, double for indices i, j and data val.
  dp = diff(X@p)
  j  = rep(seq_along(dp), dp) - 1
  
  bytes = .Call(C_scidb_raw, as.vector(t(matrix(c(X@i + start[[1]], j + start[[2]], X@x), length(X@x)))))

  # Obtain a session from shim for the upload process
  if (!is.null(attr(db, "connection")$session)) { # if session already exists
    session = attr(db, "connection")$session
    release = 0
  } else { # need to get new session every time
    session = getSession.shim(db)
    if (length(session)<1) stop("SciDB http session error")
    release = 1;
  }
  if (release) {
    on.exit(SGET.shim(db, "/release_session", list(id=session), err=FALSE), 
            add=TRUE)
  }
  
  # Upload the data  
  ans = .Post.shim(db, bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))
  
  # Create a temporary array 'name'
  if(temp){ # Use scidb temporary array instead of regular versioned array
    targetArraySchema = schema
    create_temp_array(db, name, schema = targetArraySchema)
  }
  
  # redimension into a matrix
  query = sprintf("store(redimension(input(%s,'%s',-2,'(double null,double null,double null)'),%s),%s)", schema1d, ans, schema, name)
  iquery(db, query)
  scidb(db, name, gc=gc)
}


#' Internal function to upload an R vector, dense n-d array or matrix to SciDB
#' @param db scidb database connection
#' @param X a vector, dense n-d array or matrix
#' @param name (character) SciDB array name
#' @param start (int) dimension start value
#' @param gc (boolean) set to TRUE to connect SciDB array to R's garbage collector
#' @param temp (boolean) create a temporary SciDB array
#' @param ... optional extra arguments.
#' \itemize{
#' \item {attr: } {attribute name}
#' \item {reshape: } {(boolean) to control reshape}
#' \item {type: } {(character) desired data type - however, limited type conversion available}
#' \item {max_byte_size: } {(int) maximum size of each block (in bytes) while uploading vectors in
#' a multi-part fashion. Minimum block size is 8 bytes. }
#' }
#' @return a \code{\link{scidb}} object
#' @keywords internal
.matvec2scidb.shim = function(db, X,
                              name=tmpnam(db),
                              start,
                              gc=TRUE,
                              temp=FALSE, ...)
{
  # Check for a bunch of optional hidden arguments
  args = list(...)
  attr_name = "val"
  if (!is.null(args$attr)) attr_name = as.character(args$attr)      # attribute name
  
  processed = .PreprocessArrayType(X, type=args$type)
  X = processed$array
  ## attr_type: the type of the attribute
  attr_type = processed$attr_type
  ## load_type: the type used internally for loading the data
  load_type = processed$load_type
  
  do_reshape = TRUE
  if (!is.null(args$reshape)) do_reshape = as.logical(args$reshape) # control reshape
  
  chunkSize = c(min(1000L, nrow(X)), min(1000L, ncol(X)))
  chunkSize = as.numeric(chunkSize)
  if (length(chunkSize) == 1) chunkSize = c(chunkSize, chunkSize)
  
  overlap = c(0, 0)
  if (is.null(start)) start = c(0, 0)
  start     = as.numeric(start)
  if (length(start) ==1) start = c(start, start)
  D = dim(X)
  start = as.integer(start)
  overlap = as.integer(overlap)
  dimname = make.unique_(attr_name, "i")
  DEBUG = getOption("scidb.debug", FALSE)

  if (is.null(D))
  {
    # X is a vector
    do_reshape = FALSE
    chunkSize = min(chunkSize[[1]], length(X))
    X = as.matrix(X)
    block_size = .get_multipart_post_load_block_size(
      data = X,
      debug = DEBUG,
      max_byte_size = if(is.null(args$max_byte_size)) getOption('scidb.max_byte_size', 500*(10^6)) else args$max_byte_size)
    # Define schema for an initial SciDB upload and provide a template to
    # load subsequent blocks of the vector
    schema = sprintf(
      "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, attr_type, dimname, start[[1]],
      nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = schema
    # Define a temporary schema for multi-part loading of blocks of the vector
    temp_schema = sprintf(
      "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, attr_type, dimname, start[[1]],
      min((nrow(X) - 1 + start[[1]]), (block_size - 1 + start[[1]])),
      min(nrow(X), chunkSize), overlap[[1]])
  } else if (length(D) > 2)
  {
    # X is a dense n-d array
    ndim = length(D)
    chunkSize = rep(floor(10e6 ^ (1 / ndim)), ndim)
    start = rep(0, ndim)
    end = D - 1
    dimNames = make.unique_(attr_name, paste("i", 1:length(D), sep=""))
    schema = sprintf("< %s : %s null >[%s]", attr_name, attr_type, paste(sprintf( "%s=%.0f:%.0f,%.0f,0", dimNames, start, end, chunkSize), collapse=","))
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, attr_type,  length(X))
  } else {
    # X is a matrix
    schema = sprintf(
      "< %s : %s  null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", attr_name, attr_type, start[[1]],
      nrow(X) - 1 + start[[1]], chunkSize[[1]], overlap[[1]], start[[2]], ncol(X) - 1 + start[[2]],
      chunkSize[[2]], overlap[[2]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, attr_type,  length(X))
  }
  if (!is.array(X)) stop ("X must be an array or vector")
  
  td1 = proc.time()
  # Obtain a session from shim for the upload process
  if (!is.null(attr(db, "connection")$session)) { # if session already exists
    session = attr(db, "connection")$session
    release = 0
  } else { # need to get new session every time
    session = getSession.shim(db)
    if (length(session)<1) stop("SciDB http session error")
    release = 1;
  }
  if (release) {
    on.exit(SGET.shim(db, "/release_session", list(id=session), err=FALSE), 
            add=TRUE)
  }
  shimcon_time = round((proc.time() - td1)[3], 4)
  
  if(is.null(D)) {
    .multipart_post_load.shim(db, session,
                              X, name,
                              load_schema, schema, load_type,
                              temp_schema, block_size,
                              temp, DEBUG, shimcon_time)
  } else {
    td2 = proc.time()
    bytes = .Call(C_scidb_raw, as.vector(aperm(X)))
    ans = .Post.shim(db, bytes, list(id=session))
    ans = gsub("\n", "", gsub("\r", "", ans))
    post_time = round((proc.time() - td2)[3], 4)
    
    if (DEBUG)
    {
      message("Data upload time ", (shimcon_time + post_time))
    }
    
    # Create a temporary array 'name'
    if(temp){ # Use scidb temporary array instead of regular versioned array
      targetArraySchema = schema
      create_temp_array(db, name, schema = targetArraySchema)
    }
    
    # Load query
    if (do_reshape)
    {
      query = sprintf("store(reshape(input(%s,'%s', -2, '(%s null)'),%s),%s)", load_schema, ans, load_type, schema, name)
    } else
    {
      query = sprintf("store(input(%s,'%s', -2, '(%s null)'),%s)", load_schema, ans, load_type, name)
    }
    iquery(db, query)
  }
  scidb(db, name, gc=gc)
}

.multipart_post_load.shim <- function(db, session,
                                      data, name,
                                      load_schema, schema, type,
                                      temp_schema, block_size,
                                      temp, debug, shimcon_time)
{
  
  total_length = as.numeric(length(data))
  
  # Create a temporary array 'name'
  if(temp){ # Use scidb temporary array instead of regular versioned array
    targetArraySchema = schema
    create_temp_array(db, name, schema = targetArraySchema)
  }
  
  # Declare a numeric variable for storing post time
  post_time = 0
  
  for(begin in seq(1, total_length, block_size)) {
    
    end = min((begin + block_size -1), total_length)
    
    # convert data to raw
    td = proc.time()
    data_part = as.matrix(data[begin:end])
    bytes = .Call(C_scidb_raw, as.vector(aperm(data_part)))
    post_time = post_time + round((proc.time() - td)[3], 4)
    
    # upload data
    ans = .Post.shim(db, bytes, list(id=session))
    ans = gsub("\n", "", gsub("\r", "", ans))
    
    if(debug) message("Uploading block ", ceiling(begin/block_size), " of ", ceiling(total_length/block_size))
    # load query
    if(begin == 1) {
      query = sprintf("store(input(%s,'%s', -2, '(%s null)'), %s)", load_schema, ans, type, name)
      iquery(db, query)
    } else {
      query = sprintf("input(%s,'%s', -2, '(%s null)')", temp_schema, ans, type)
      iquery(db, sprintf("append(%s, %s, i)", query, name))
    }
  }
  
  if(debug) message("Data upload time ", shimcon_time + post_time)
  
  return(NULL)
}

# Normally called with raw data and args=list(id=whatever)
.Post.shim = function(db, data, args=list(), err=TRUE)
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
    handle_setheaders(h, .list=list(Authorization=.digest_auth(db, "POST", uri)))
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
  handle_setheaders(h, .list=list(Authorization=.digest_auth(db, "POST", uri)))
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
  is_debug = getOption("scidb.debug", FALSE)
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
  SGET.shim(conn, "/release_session", list(id=conn$session), err=FALSE)
  
  ## Set the session to NULL so we don't double-release the session.
  ## Because conn is an env, this side effect should persist
  ## outside of the current function call.
  conn$session <- NULL
}

#' Convenience function for digest authentication.
#' @param db_or_conn a scidb database connection object,
#'    _or_ its "connection" attribute
#' @param method digest method
#' @param uri uri
#' @param realm realm
#' @param nonce nonce
#' @keywords internal
#' @importFrom digest digest
.digest_auth = function(db_or_conn, method, uri, realm="", nonce="123456")
{
  .scidbenv = .GetConnectionEnv(db_or_conn)
  
  if (!is.null(.scidbenv$authtype))
  {
    if (.scidbenv$authtype != "digest") return("")
  }
  uri = gsub(".*/", "/", uri)
  userpwd = .scidbenv$digest
  if (is.null(userpwd)) userpwd=":"
  up = strsplit(userpwd, ":")[[1]]
  user = up[1]
  pwd  = up[2]
  if (is.na(pwd)) pwd=""
  ha1=digest(sprintf("%s:%s:%s", user, realm, pwd), algo="md5", serialize=FALSE)
  ha2=digest(sprintf("%s:%s", method,  uri), algo="md5", serialize=FALSE)
  cnonce="MDc1YmFhOWFkY2M0YWY2MDAwMDBlY2JhMDAwMmYxNTI="
  nc="00000001"
  qop="auth"
  response=digest(sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2), algo="md5", serialize=FALSE)
  sprintf('Digest username="%s", realm=%s, nonce="%s", uri="%s", cnonce="%s", nc=%s, qop=%s, response="%s"', user, realm, nonce, uri, cnonce, nc, qop, response)
}
