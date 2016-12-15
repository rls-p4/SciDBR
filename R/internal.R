# Non-exported utility functions

#' Unpack and return a SciDB query expression as a data frame
#' @param db scidb database connection object
#' @param query A SciDB query expression or scidb object
#' @param ... optional extra arguments
#' @keywords internal
#' @importFrom curl new_handle handle_setheaders handle_setopt curl_fetch_memory handle_setform form_file
#' @importFrom data.table  data.table
scidb_unpack_to_dataframe = function(db, query, ...)
{
  DEBUG = FALSE
  projected = FALSE
  if(!inherits(query, "scidb")) query = scidb(db, query)
  DEBUG = getOption("scidb.debug", FALSE)
  buffer = 100000L
  args = list(...)
  if(!is.null(args$buffer))
  {
    argsbuf = tryCatch(as.integer(args$buffer), warning=function(e) NA)
    if(!is.na(argsbuf) && argsbuf <= 1e9) buffer = as.integer(argsbuf)
  }
  if(!is.null(args$attributes))
  {
      x = scidb(db, sprintf("project(%s, %s)", query@name, paste(schema(query, "attributes")$name, collapse=",")))
  } else
  {
    ndim = length(schema(query, "dimensions")$name)
    if(getOption("scidb.unpack"))
    {
      dim = make.unique_(c(schema(query, "attributes")$name, schema(query, "dimensions")$name), "i")
      x = scidb(db, sprintf("unpack(%s, %s)", query@name, dim))
    } else
    {
      dims_query = schema(query, "dimensions")$name
      dims = paste(dims_query, dims_query, sep=",", collapse=",")
      x = scidb(db, sprintf("project(apply(%s, %s), %s, %s)", query@name,
                      dims, paste(dims_query, collapse=","), paste(schema(query, "attributes")$name, collapse=",")))
    }
  }
  A = schema(x, "attributes")
  ns = rep("", length(A$nullable))
  ns[A$nullable] = "null"
  format_string = paste(paste(A$type, ns), collapse=",")
  format_string = sprintf("(%s)", format_string)
  sessionid = scidbquery(db, x@name, save=format_string, release=0)
  on.exit( SGET(db, "/release_session", list(id=sessionid), err=FALSE), add=TRUE)

  dt2 = proc.time()
  uri = URI(db, "/read_bytes", list(id=sessionid, n=0))
  h = new_handle()
  handle_setheaders(h, .list=list(`Authorization`=digest_auth(db, "GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0))
  resp = curl_fetch_memory(uri, h)
  if(resp$status_code > 299) stop("HTTP error", resp$status_code)
# Explicitly reap the handle to avoid short-term build up of socket descriptors
  rm(h)
  gc()
  if(DEBUG) cat("Data transfer time", (proc.time() - dt2)[3], "\n")
  dt1 = proc.time()
  len = length(resp$content)
  p = 0
  ans = c()
  atts = schema(x, "attributes")$name
  cnames = c(atts, "lines", "p")  # we are unpacking to a SciDB array, ignore dims
  n = length(atts)
  rnames = c()
  if(projected) n = length(args$project)
  while(p < len)
  {
    dt2 = proc.time()
    tmp   = .Call("scidb_parse", as.integer(buffer), A$type, A$nullable, resp$content, as.double(p), PACKAGE="scidb")
    names(tmp) = cnames
    lines = tmp[[n+1]]
    p_old = p
    p     = tmp[[n+2]]
    if(DEBUG) cat("  R buffer ", p, "/", len, " bytes parsing time", (proc.time() - dt2)[3], "\n")
    dt2 = proc.time()
    if(lines > 0)
    {
      if("binary" %in% A$type)
      {
        if(DEBUG) cat("  R rbind/df assembly time", (proc.time() - dt2)[3], "\n")
        return(lapply(1:n, function(j) tmp[[j]][1:lines]))
      }
      len_out = length(tmp[[1]])
      if(lines < len_out) tmp = lapply(tmp[1:n], function(x) x[1:lines])
# adaptively re-estimate a buffer size
      avg_bytes_per_line = ceiling((p - p_old) / lines)
      buffer = min(getOption("scidb.buffer_size"), ceiling(1.3 * (len - p) / avg_bytes_per_line)) # Engineering factors
# Assemble the data frame
      if(is.null(ans)) ans = data.table::data.table(data.frame(tmp[1:n], stringsAsFactors=FALSE))
      else ans = rbind(ans, data.table::data.table(data.frame(tmp[1:n], stringsAsFactors=FALSE)))
    }
    if(DEBUG) cat("  R rbind/df assembly time", (proc.time() - dt2)[3], "\n")
  }
  if(is.null(ans))
  {
    xa = schema(x, "attributes")$name
    xd = schema(x, "dimensions")$name
    n = length(xd) + length(xa)
    ans = vector(mode="list", length=n)
    names(ans) = c(xd, xa)
    class(ans) = "data.frame"
    return(ans)
  }
  if(DEBUG) cat("Total R parsing time", (proc.time() - dt1)[3], "\n")
  ans = as.data.frame(ans)
  gc()
  ans
}

#' Convenience function for digest authentication.
#' @param db a scidb database connection object
#' @param method digest method
#' @param uri uri
#' @param realm realm
#' @param nonce nonce
#' @keywords internal
#' @importFrom digest digest
digest_auth = function(db, method, uri, realm="", nonce="123456")
{
  .scidbenv = attr(db, "connection")
  if(exists("authtype", envir=.scidbenv))
  {
   if(.scidbenv$authtype != "digest") return("")
  }
  uri = gsub(".*/", "/", uri)
  userpwd = .scidbenv$digest
  if(is.null(userpwd)) userpwd=":"
  up = strsplit(userpwd, ":")[[1]]
  user = up[1]
  pwd  = up[2]
  if(is.na(pwd)) pwd=""
  ha1=digest(sprintf("%s:%s:%s", user, realm, pwd, algo="md5"), serialize=FALSE)
  ha2=digest(sprintf("%s:%s", method,  uri, algo="md5"), serialize=FALSE)
  cnonce="MDc1YmFhOWFkY2M0YWY2MDAwMDBlY2JhMDAwMmYxNTI="
  nc="00000001"
  qop="auth"
  response=digest(sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2), algo="md5", serialize=FALSE)
  sprintf('Digest username="%s", realm=%s, nonce="%s", uri="%s", cnonce="%s", nc=%s, qop=%s, response="%s"', user, realm, nonce, uri, cnonce, nc, qop, response)
}

# Internal warning function
warnonce = (function() {
  state = list(
    count="Use the AFL op_count macro operator for an exact count of data rows.",
    nonum="Note: The R sparse Matrix package does not support certain value types like\ncharacter strings"
  )
  function(warn) {
    if(!is.null(state[warn][[1]])) {
      message(state[warn])
      s <<- state
      s[warn] = c()
      state <<- s
    }
  }
}) ()


# Some versions of RCurl seem to contain a broken URLencode function.
oldURLencode = function (URL, reserved = FALSE) 
{
    OK = paste0("[^-ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz0123456789$_.+!*'(),", 
        if (!reserved) 
            ";/?:@=&", "]")
    x = strsplit(URL, "")[[1L]]
    z = grep(OK, x)
    if (length(z)) {
        y = sapply(x[z], function(x) paste0("%", as.character(charToRaw(x)), 
            collapse = ""))
        x[z] = y
    }
    paste(x, collapse = "")
}


# Internal function
create_temp_array = function(db, name, schema)
{
# SciDB temporary array syntax varies with SciDB version
  TEMP = "'TEMP'"
  if(newer_than(attr(db, "connection")$scidb.version, "14.12")) TEMP="true"
  query   = sprintf("create_array(%s, %s, %s)", name, schema, TEMP)
  iquery(db, query, `return`=FALSE)
}


#' An important internal convenience function that returns a scidb object.  If
#' eval=TRUE, a new SciDB array is created the returned scidb object refers to
#' that.  Otherwise, the returned scidb object represents a SciDB array promise.
#'
#' @param db scidb connection object
#' @param expr (character) A SciDB expression or array name
#' @param eval (logical) If TRUE evaluate expression and assign to new SciDB array.
#'                 If FALSE, infer output schema but don't evaluate.
#' @param name (optional character) If supplied, name for stored array when eval=TRUE
#' @param gc (optional logical) If TRUE, tie SciDB object to  garbage collector.
#' @param depend (optional list) An optional list of other scidb objects
#'         that this expression depends on (preventing their garbage collection
#'         if other references to them go away).
#' @param schema (optional) used to create SciDB temp arrays
#'               (requires scidb >= 14.8)
#' @param temp (optional) used to create SciDB temp arrays
#'               (requires scidb >= 14.8)
#' @return A \code{scidb} array object
#' @note Only AFL supported.
`.scidbeval` = function(db, expr, eval=FALSE, name, gc=TRUE, depend, schema, temp)
{
  ans = c()
  if(missing(depend)) depend = c()
  if(missing(schema)) schema = ""
  if(missing(temp)) temp = FALSE
  if(!is.list(depend)) depend = list(depend)
# Address bug #45. Try to cheaply determine if expr refers to a named array
# or an AFL expression. If it's a named array, then eval must be set TRUE.
  if(!grepl("\\(", expr, perl=TRUE)) eval = TRUE
  if(`eval`)
  {
    if(missing(name) || is.null(name))
    {
      newarray = tmpnam(db)
      if(temp) create_temp_array(db, newarray, schema)
    }
    else newarray = name
    query = sprintf("store(%s,%s)", expr, newarray)
    scidbquery(db, query, stream=0L)
    ans = scidb(db, newarray, gc=gc)
    if(temp) ans@meta$temp = TRUE
  } else
  {
    ans = scidb(db, expr, gc=gc)
# Assign dependencies
    if(length(depend) > 0)
    {
      assign("depend", depend, envir=ans@meta)
    }
  }
  ans
}

make.names_ = function(x)
{
  gsub("\\.","_", make.names(x, unique=TRUE), perl=TRUE)
}

# x is vector of existing values
# y is vector of new values
# returns a set the same size as y with non-conflicting value names
make.unique_ = function(x, y)
{
  z = make.names(gsub("_", ".", c(x, y)), unique=TRUE)
  gsub("\\.", "_", utils::tail(z, length(y)))
}


# Make a name from a prefix and a unique SciDB identifier.
getuid = function(db)
{
  .scidbenv = attr(db, "connection")
  if(!exists("uid", envir=.scidbenv)) stop("Not connected...try scidbconnect")
  get("uid", envir=.scidbenv)
}
tmpnam = function(db, prefix="R_array")
{
  stopifnot(inherits(db, "afl"))
  salt = basename(tempfile(pattern=prefix))
  paste(salt, getuid(db), sep="")
}

# Return a shim session ID or error
getSession = function(db)
{
  session = SGET(db, "/new_session")
  if(length(session)<1) stop("SciDB http session error; are you connecting to a valid SciDB host?")
  session = gsub("\r", "", session)
  session = gsub("\n", "", session)
  session
}

# Supply the base SciDB URI from the global host, port and auth
# parameters stored in the "connection" environment in the db object
# Every function that needs to talk to the shim interface should use
# this function to supply the URI.
# Arguments:
# db scidb database connection object
# resource (string): A URI identifying the requested service
# args (list): A list of named query parameters
URI = function(db, resource="", args=list())
{
  .scidbenv = attr(db, "connection")
  if(!exists("host", envir=.scidbenv)) stop("Not connected...try scidbconnect")
  if(exists("auth", envir=.scidbenv))
    args = c(args, list(auth=get("auth", envir=.scidbenv)))
  if(!is.null(.scidbenv$password)) args = c(args, list(password=.scidbenv$password))
  if(!is.null(.scidbenv$username)) args = c(args, list(user=.scidbenv$username))
  prot = paste(get("protocol", envir=.scidbenv), "//", sep=":")
  if("password" %in% names(args) || "auth" %in% names(args)) prot = "https://"
  ans  = paste(prot, get("host", envir=.scidbenv), ":", get("port", envir=.scidbenv), sep="")
  ans = paste(ans, resource, sep="/")
  if(length(args)>0)
    ans = paste(ans, paste(paste(names(args), args, sep="="), collapse="&"), sep="?")
  ans
}

SGET = function(db, resource, args=list(), err=TRUE, binary=FALSE)
{
  if(!(substr(resource, 1, 1)=="/")) resource = paste("/", resource, sep="")
  uri = URI(db, resource, args)
  uri = oldURLencode(uri)
  uri = gsub("\\+", "%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth(db, "GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0))
  ans = curl_fetch_memory(uri, h)
  if(ans$status_code > 299 && err)
  {
    msg = sprintf("HTTP error %s", ans$status_code)
    if(ans$status_code >= 500) msg = sprintf("%s\n%s", msg, rawToChar(ans$content))
    stop(msg)
  }
  if(binary) return(ans$content)
  rawToChar(ans$content)
}

# Normally called with raw data and args=list(id=whatever)
POST = function(db, data, args=list(), err=TRUE)
{
# check for new shim simple post option (/upload), otherwise use
# multipart/file upload (/upload_file)
  shimspl = strsplit(attr(db, "connection")$scidb.version, "\\.")[[1]]
  shim_yr = tryCatch(as.integer(gsub("[A-z]", "", shimspl[1])), error=function(e) 16, warning=function(e) 8)
  shim_mo = tryCatch(as.integer(gsub("[A-z]", "", shimspl[2])), error=function(e) 16, warning=function(e) 8)
  if(is.na(shim_yr)) shim_yr = 16
  if(is.na(shim_mo)) shim_mo = 8
  simple = (shim_yr >= 15 && shim_mo >= 7) || shim_yr >= 16
  if(simple)
  {
    uri = URI(db, "/upload", args)
    uri = oldURLencode(uri)
    uri = gsub("\\+", "%2B", uri, perl=TRUE)
    h = new_handle()
    handle_setheaders(h, .list=list(Authorization=digest_auth(db, "POST", uri)))
    handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                                ssl_verifypeer=0, post=TRUE, postfieldsize=length(data), postfields=data))
    ans = curl_fetch_memory(uri, h)
    if(ans$status_code > 299 && err) stop("HTTP error ", ans$status_code)
    return(rawToChar(ans$content))
  }
  uri = URI(db, "/upload_file", args)
  uri = oldURLencode(uri)
  uri = gsub("\\+", "%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth(db, "POST", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(getOption("scidb.verifyhost", FALSE)),
                              ssl_verifypeer=0))
  tmpf = tempfile()
  if(is.character(data)) data = charToRaw(data)
  writeBin(data, tmpf)
  handle_setform(h, file=form_file(tmpf))
  ans = curl_fetch_memory(uri, h)
  unlink(tmpf)
  if(ans$status_code > 299 && err) stop("HTTP error", ans$status_code)
  return(rawToChar(ans$content))
}

# Basic low-level query. Returns query id. This is an internal function.
# db: scidb database connection object
# query: a character query string
# save: Save format query string or NULL.
# release: Set to zero preserve web session until manually calling release_session
# session: if you already have a SciDB http session, set this to it, otherwise NULL
# resp(logical): return http response
# stream: Set to 0L or 1L to control streaming (NOT USED)
# prefix: optional AFL statement to prefix query in the same connection context.
# Example values of save:
# save="dcsv"
# save="csv+"
# save="(double NULL, int32)"
#
# Returns the HTTP session in each case
scidbquery = function(db, query, save=NULL, release=1, session=NULL, resp=FALSE, stream, prefix=attr(db, "prefix"))
{
  DEBUG = FALSE
  STREAM = 0L
  DEBUG = getOption("scidb.debug", FALSE)
  if(missing(stream))
  {
    STREAM = 0L
  } else STREAM = as.integer(stream)
  sessionid = session
  if(is.null(session))
  {
# Obtain a session from shim
    sessionid = getSession(db)
  }
  if(is.null(save)) save=""
  if(DEBUG)
  {
    cat(query, "\n")
    t1=proc.time()
  }
  ans = tryCatch(
    {
      args = list(id=sessionid, afl=0L, query=query, stream=0L)
      args$release = release
      args$prefix = c(getOption("scidb.prefix"), prefix)
      if(!is.null(args$prefix)) args$prefix = paste(args$prefix, collapse=";")
      args$save = save
      args = list(db=db, resource="/execute_query", args=args)
      do.call("SGET", args=args)
    }, error=function(e)
    {
      # User cancel?
      SGET(db, "/cancel", list(id=sessionid), err=FALSE)
      SGET(db, "/release_session", list(id=sessionid), err=FALSE)
      stop(as.character(e))
    }, interrupt=function(e)
    {
      SGET(db, "/cancel", list(id=sessionid), err=FALSE)
      SGET(db, "/release_session", list(id=sessionid), err=FALSE)
      stop("cancelled")
    })
  if(DEBUG) cat("Query time", (proc.time()-t1)[3], "\n")
  if(resp) return(list(session=sessionid, response=ans))
  sessionid
}

.Matrix2scidb = function(db, X, name, rowChunkSize=1000, colChunkSize=1000, start=c(0, 0), gc=TRUE, ...)
{
  D = dim(X)
  rowOverlap = 0L
  colOverlap = 0L
  if(missing(start)) start=c(0, 0)
  if(length(start) < 1) stop ("Invalid starting coordinates")
  if(length(start) > 2) start = start[1:2]
  if(length(start) < 2) start = c(start, 0)
  start = as.integer(start)
  type = .scidbtypes[[typeof(X@x)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(.scidbtypes, collapse=" "), ".", sep=""))
  }
  if(type != "double") stop("Sorry, the package only supports double-precision sparse matrices right now.")
  schema = sprintf(
      "< val : %s null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X), rowChunkSize), rowOverlap, start[[2]], ncol(X)-1+start[[2]],
      min(ncol(X), colChunkSize), colOverlap)
  schema1d = sprintf("<i:int64 null, j:int64 null, val : %s null>[idx=0:*,100000,0]", type)

# Obtain a session from shim for the upload process
  session = getSession(db)
  if(length(session)<1) stop("SciDB http session error")
  on.exit(SGET(db, "/release_session", list(id=session), err=FALSE) , add=TRUE)

# Compute the indices and assemble message to SciDB in the form
# double, double, double for indices i, j and data val.
  dp = diff(X@p)
  j  = rep(seq_along(dp), dp) - 1

# Upload the data
  bytes = .Call("scidb_raw", as.vector(t(matrix(c(X@i + start[[1]], j + start[[2]], X@x), length(X@x)))), PACKAGE="scidb")
  ans = POST(db, bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))

# redimension into a matrix
  query = sprintf("store(redimension(input(%s,'%s',-2,'(double null,double null,double null)'),%s),%s)", schema1d, ans, schema, name)
  iquery(db, query)
  scidb(db, name, gc=gc)
}


# raw value to special 1-element SciDB array
raw2scidb = function(db, X, name, gc=TRUE, ...)
{
  if(!is.raw(X)) stop("X must be a raw value")
  args = list(...)
# Obtain a session from shim for the upload process
  session = getSession(db)
  if(length(session)<1) stop("SciDB http session error")
  on.exit(SGET(db, "/release_session", list(id=session), err=FALSE) , add=TRUE)

  bytes = .Call("scidb_raw", X, PACKAGE="scidb")
  ans = POST(db, bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))

  schema = "<val:binary null>[i=0:0,1,0]"
  if(!is.null(args$temp))
  {
    if(args$temp) create_temp_array(db, name, schema)
  }

  query = sprintf("store(input(%s,'%s',-2,'(binary null)'),%s)", schema, ans, name)
  iquery(db, query)
  scidb(db, name, gc=gc)
}

# Internal utility function used to format numbers
noE = function(w) sapply(w,
  function(x)
  {
    if(is.infinite(x)) return("*")
    if(is.character(x)) return(x)
    sprintf("%.0f", x)
  })

#' Returns TRUE if version string x is greater than or equal to than version y
#' @param x version string like "12.1", "15.12", etc. (non-numeric ignored)
#' @param y version string like "12.1", "15.12", etc. (non-numeric ignored)
#' @return logical TRUE if x is greater than or equal to y
newer_than = function(x, y)
{
  b = as.numeric(gsub("-.*", "", gsub("[A-z].*", "", strsplit(sprintf("%s.0", x), "\\.")[[1]])))
  b = b[1] + b[2] / 100
  a = as.numeric(gsub("-.*", "", gsub("[A-z].*", "", strsplit(sprintf("%s.0", y), "\\.")[[1]])))
  a = a[1] + a[2] / 100
  b >= a
}

# Used in delayed assignment of scidb object schema
lazyeval = function(db, name)
{
  escape = gsub("'", "\\\\'", name, perl=TRUE)
  query = iquery(db, sprintf("show('filter(%s, true)', 'afl')", escape), `return`=TRUE, binary=FALSE)
  # NOTE that we need binary=FALSE here to avoid a terrible recursion
  list(schema = gsub("^.*<", "<", query$schema, perl=TRUE))
}

#' Internal function to upload an R data frame to SciDB
#' @param db scidb database connection
#' @param X a data frame
#' @param name SciDB array name
#' @param chunk_size passed to the aio_input operator see https://github.com/Paradigm4/accelerated_io_tools
#' @param types SciDB attribute types
#' @param gc set to \code{TRUE} to connect SciDB array to R's garbage collector
#' @return a \code{\link{scidb}} object, or a character schema string if \code{schema_only=TRUE}.
#' @keywords internal
df2scidb = function(db, X,
                    name=tmpnam(db),
                    types=NULL,
                    chunk_size=100000,
                    gc)
{
  .scidbenv = attr(db, "connection")
  if(!is.data.frame(X)) stop("X must be a data frame")
  if(missing(gc)) gc = FALSE
  nullable = TRUE
  anames = make.names(names(X), unique=TRUE)
  anames = gsub("\\.","_", anames, perl=TRUE)
  if(length(anames)!=ncol(X)) anames = make.names(1:ncol(X))
  if(!all(anames==names(X))) warning("Attribute names have been changed")

# Default type is string
  typ = rep("string", ncol(X))
  dcast = anames
  if(!is.null(types)) {
    for(j in 1:ncol(X)) typ[j] = types[j]
  } else {
    for(j in 1:ncol(X)) {
      if("numeric" %in% class(X[,j])) 
        typ[j] = "double"
      else if("integer" %in% class(X[,j])) 
        typ[j] = "int32"
      else if("logical" %in% class(X[,j])) 
        typ[j] = "bool"
      else if("character" %in% class(X[,j])) 
        typ[j] = "string"
      else if("factor" %in% class(X[,j])) 
      {
        typ[j] = "string"
      }
      else if("POSIXct" %in% class(X[,j])) 
      {
        warning("Converting R POSIXct to SciDB datetime as UTC time. Subsecond times rounded to seconds.")
        X[,j] = format(X[,j],tz="UTC")
        typ[j] = "datetime"
      }
    }  
  }
  for(j in 1:ncol(X))
  {
    if(typ[j] == "datetime") dcast[j] = sprintf("%s, datetime(a%d)",anames[j],j-1)
    else if(typ[j] == "string") dcast[j] = sprintf("%s, a%d", anames[j], j-1)
    else dcast[j] = sprintf("%s, dcast(a%d, %s(null))", anames[j], j-1, typ[j])
  }
  args = sprintf("<%s>", paste(anames, ":", typ, " null", collapse=","))

# Obtain a session from the SciDB http service for the upload process
  session = getSession(db)
  on.exit(SGET(db, "/release_session", list(id=session), err=FALSE) ,add=TRUE)

  ncolX = ncol(X)
  nrowX = nrow(X)
  X = charToRaw(fwrite(X, file=return))
  tmp = POST(db, X, list(id=session))
  tmp = gsub("\n", "", gsub("\r", "", tmp))

# Generate a load_tools query
  aio = length(grep("aio_input", .scidbenv$ops)) > 0
  atts = paste(dcast, collapse=",")
  if(aio)
  {
    LOAD = sprintf("project(apply(aio_input('%s','num_attributes=%d','chunk_size=%.0f'),%s),%s)", tmp,
                 ncolX, chunk_size, atts, paste(anames, collapse=","))
  } else
  {
    LOAD = sprintf("input(%s, '%s', -2, 'tsv')", dfschema(anames, typ, nrowX, chunk_size), tmp)
  }
  query = sprintf("store(%s,%s)", LOAD, name)
  scidbquery(db, query, release=1, session=session, stream=0L)
  scidb(db, name, gc=gc)
}

#' Fast write.table/textConnection substitute
#'
#' Conversions are vectorized and the entire output is buffered in memory and written in
#' one shot. Great option for replacing writing to a textConnection (much much faster).
#' Not such a great option for writing to files, only 10% faster than write.table and
#' obviously much greater memory use.
#' @param x a data frame
#' @param file a connection or \code{return} to return character output directly (fast)
#' @param sep column separator
#' @param format optional fprint-style column format specifyer
#' @return Use for the side effect of writing to the connection returning \code{NULL}, or
#' return a character value when \code{file=return}.
#' @keywords internal
fwrite = function(x, file=stdout(), sep="\t", format=paste(rep("%s", ncol(x)), collapse=sep))
{
  if(!is.data.frame(x)) stop("x must be a data.frame")
  if(is.function(file)) return(paste(do.call("sprintf", args=c(format, as.list(x))), collapse="\n"))
  write(paste(do.call("sprintf", args=c(format, as.list(x))), collapse="\n"), file=file)
  invisible()
}


matvec2scidb = function(db, X,
                        name=tmpnam(db),
                        start,
                        gc=TRUE, ...)
{
# Check for a bunch of optional hidden arguments
  args = list(...)
  attr_name = "val"
  if(!is.null(args$attr)) attr_name = as.character(args$attr)      # attribute name
  do_reshape = TRUE
  type = force_type = .Rtypes[[typeof(X)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(unique(names(.Rtypes)), collapse=" "), ".", sep=""))
  }
  if(!is.null(args$reshape)) do_reshape = as.logical(args$reshape) # control reshape
  if(!is.null(args$type)) force_type = as.character(args$type) # limited type conversion
  chunkSize = c(min(1000L, nrow(X)), min(1000L, ncol(X)))
  chunkSize = as.numeric(chunkSize)
  if(length(chunkSize) == 1) chunkSize = c(chunkSize, chunkSize)
  overlap = c(0, 0)
  if(missing(start)) start = c(0, 0)
  start     = as.numeric(start)
  if(length(start) ==1) start = c(start, start)
  D = dim(X)
  start = as.integer(start)
  overlap = as.integer(overlap)
  dimname = make.unique_(attr_name, "i")
  if(is.null(D))
  {
# X is a vector
    if(!is.vector(X)) stop ("Unsupported object")
    do_reshape = FALSE
    chunkSize = min(chunkSize[[1]], length(X))
    X = as.matrix(X)
    schema = sprintf(
        "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, dimname, start[[1]],
        nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = schema
  } else if(length(D) > 2)
  {
# X is an n-d array
    stop("not supported yet") # XXX WRITE ME
    do_reshape = TRUE
    X = as.matrix(as.vector(aperm(X)))
    schema = sprintf(
        "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, dimname, start[[1]],
        nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, force_type, length(X))
  } else {
# X is a matrix
    schema = sprintf(
      "< %s : %s  null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, start[[1]],
      nrow(X) - 1 + start[[1]], chunkSize[[1]], overlap[[1]], start[[2]], ncol(X) - 1 + start[[2]],
      chunkSize[[2]], overlap[[2]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, force_type,  length(X))
  }
  if(!is.matrix(X)) stop ("X must be a matrix or a vector")

  DEBUG = getOption("scidb.debug", FALSE)
  td1 = proc.time()
# Obtain a session from shim for the upload process
  session = getSession(db)
  on.exit( SGET(db, "/release_session", list(id=session), err=FALSE) ,add=TRUE)

# Upload the data
  bytes = .Call("scidb_raw", as.vector(t(X)), PACKAGE="scidb")
  ans = POST(db, bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))
  if(DEBUG)
  {
    cat("Data upload time", (proc.time() - td1)[3], "\n")
  }

# Load query
  if(do_reshape)
  {
    query = sprintf("store(reshape(input(%s,'%s', -2, '(%s null)'),%s),%s)",load_schema, ans, type, schema, name)
  } else
  {
    query = sprintf("store(input(%s,'%s', -2, '(%s null)'),%s)",load_schema, ans, type, name)
  }
  iquery(db, query)
  scidb(db, name, gc=gc)
}
