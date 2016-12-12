#' Evaluate an expression to \code{scidb} or \code{scidb} objects
#'
#' Force evaluation of an expression that yields a \code{scidb} or \code{scidb} object,
#' storing the result to a SciDB array when \code{eval=TRUE}.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @param expr a quoted SciDB expression \code{scidb} object
#' @param eval \code{FALSE} do not evaluate the expression in SciDB (leave as a view)
#' @param name (character) optional SciDB array name to store result to
#' @param gc (logical) optional, when TRUE tie result to R garbage collector
#' @param temp (logical, optional), when TRUE store as a SciDB temp array
#' @export
store = function(db, expr, eval=TRUE, name, gc=TRUE, temp=FALSE)
{
  ans = eval(expr)
  if(!(inherits(ans,"scidb"))) return(ans)
# If expr is a stored temp array, then re-use its name
  if(!is.null(ans@meta$temp) && ans@meta$temp && missing(name)) name=ans@name
  .scidbeval(db, ans@name, `eval`=eval, name=name, gc=gc, schema=schema(ans), temp=temp)
}

#' Create an R reference to a SciDB array or expression.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @param name a character string name of a stored SciDB array or a valid SciDB AFL expression
#' @param gc a logical value, \code{TRUE} means connect the SciDB array to R's garbage collector
#' @return a \code{scidb} object
#' @importFrom methods new show
#' @export
scidb = function(db, name, gc=FALSE)
{
  if(missing(name)) stop("array or expression must be specified")
  if(is.scidb(name))
  {
    query = name@name
    return(.scidbeval(db, name@name, eval=FALSE, gc=gc, depend=list(name)))
  }

  obj = new("scidb", name=name)
  obj@meta = new.env()
  obj@meta$db = db
  delayedAssign("state", lazyeval(db, name), assign.env=obj@meta)
  delayedAssign("schema", get("state")$schema, eval.env=obj@meta, assign.env=obj@meta)

  if(gc)
  {
    if(length(grep("\\(", name)) == 0)
    {
      obj@meta$name = name
    }
    obj@meta$remove = TRUE
    reg.finalizer(obj@meta, function(e)
        {
          if (e$remove && exists("name", envir=e))
            {
# XXX
browser()
# XXX restrict to temp     scidbquery(db, sprintf("remove(%s)", e$name), release=1)
            }
        }, onexit = TRUE)
  }
  obj
}


#' Connect to a SciDB database
#' @param host optional host name or I.P. address of a SciDB shim service to connect to
#' @param port optional port number of a SciDB shim service to connect to
#' @param username optional authentication username
#' @param password optional authentication password
#' @param auth_type optional SciDB authentication type
#' @param protocol optional shim protocol type
#' @note
#' Use the optional \code{username} and \code{password} arguments with
#' \code{auth_type} set to "digest" to use HTTP digest authentication (see the
#' shim documentation to configure this).  Digest authentication may use either
#' "http" or "https" selected by the \code{protocol} setting.
#' Set \code{auth_type = "scidb"} to use SciDB authentication, which only
#' works over "https".
#'
#' All arguments support partial matching.
#' @return A scidb connection object.
#' @examples
#' \dontrun{
#' s <- scidbconnect()
#'
#' # SciDB 15.12 authentication example (using shim's default HTTPS port 8083)
#' s <- scidbconnect(user="root", password="Paradigm4", auth_type="scidb", port=8083)
#' }
#' @importFrom digest digest
#' @importFrom openssl base64_encode
#' @export
scidbconnect = function(host=getOption("scidb.default_shim_host", "127.0.0.1"),
                        port=getOption("scidb.default_shim_port", 8080L),
                        username, password,
                        auth_type=c("scidb", "digest"), protocol=c("http", "https"))
{
  .scidbenv = new.env() # connection state
  auth_type = match.arg(auth_type)
  protocol = match.arg(protocol)
  assign("host", host, envir=.scidbenv)
  assign("port", port, envir=.scidbenv)
  assign("protocol", protocol, envir=.scidbenv)
  if(missing(username)) username = c()
  if(missing(password)) password = c()
# Check for login using either scidb or HTTP digest authentication
  if(!is.null(username))
  {
    assign("authtype", auth_type, envir=.scidbenv)
    assign("authenv", new.env(), envir=.scidbenv)
    if(auth_type=="scidb")
    {
      assign("username", username, envir=.scidbenv)
      assign("password", base64_encode(digest(charToRaw(password),
        serialize=FALSE, raw=TRUE, algo="sha512")), envir=.scidbenv)
    } else # HTTP basic digest auth
    {
      assign("digest", paste(username, password, sep=":"), envir=.scidbenv)
    }
  }
# Set up a db object
  db = list()
  class(db) = "afl"
  attr(db, "connection") = .scidbenv
# Update the shim.version option
  options(shim.version=SGET(db, "/version"))

# Update the scidb.version option
  v = strsplit(gsub("[A-z\\-]", "", gsub("-.*", "", getOption("shim.version"))), "\\.")[[1]]
  if(length(v) < 2) v = c(v, "1")
  options(scidb.version=sprintf("%s.%s", v[1], v[2]))


# Use the query ID from a query as a unique ID for automated
# array name generation.
  x = tryCatch(
        scidbquery(db, query="list('libraries')", release=1, resp=TRUE),
        error=function(e) stop("Connection error"))
  if(is.null(.scidbenv$uid))
  {
    id = tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
           error=function(e) stop("Connection error"))
    id = id[[length(id)]]
    assign("uid", id, envir=.scidbenv)
  }

# Update available operators and macros and return afl object
  ops = iquery(db, "merge(redimension(project(list('operators'), name), <name:string>[i=0:*,1000000,0]), redimension(apply(project(list('macros'), name), i, No + 1000000), <name:string>[i=0:*,1000000,0]))", `return`=TRUE, binary=FALSE)[,2]
  update.afl(db, ops)
}


# binary=FALSE is needed by some queries, don't get rid of it.
#' Run a SciDB query, optionally returning the result.
#' @param db a scidb database connection from \code{\link{scidbconnect}}
#' @param query a single SciDB query string
#' @param return if \code{TRUE}, return the result
#' @param binary set to \code{FALSE} to read result from SciDB in text form
#' @param ... additional options passed to \code{read.table} when \code{binary=FALSE}
#' @importFrom utils read.table
#' @export
iquery = function(db, query, `return`=FALSE, binary=TRUE, ...)
{
  DEBUG = getOption("scidb.debug", FALSE)
  if(is.scidb(query))  query = query@name
  n = -1    # Indicate to shim that we want all the output
  if(`return`)
  {
    if(binary) return(scidb_unpack_to_dataframe(db, query, ...))

    ans = tryCatch(
       {
        # SciDB save syntax changed in 15.12
        if(newer_than(getOption("scidb.version", "15.12"),15.12))
        { 
          sessionid = scidbquery(db, query, save="csv+:l", release=0)
        } else sessionid = scidbquery(db, query, save="csv+", release=0)
        dt1 = proc.time()
        result = tryCatch(
          {
            SGET(db, "/read_lines", list(id=sessionid, n=as.integer(n + 1)))
          },
          error=function(e)
          {
             SGET(db, "/cancel", list(id=sessionid))
             SGET(db, "/release_session", list(id=sessionid), err=FALSE)
             stop(e)
          })
        SGET(db, "/release_session", list(id=sessionid), err=FALSE)
        if(DEBUG) cat("Data transfer time",(proc.time() - dt1)[3],"\n")
        dt1 = proc.time()
# Handle escaped quotes
        result = gsub("\\\\'","''", result, perl=TRUE)
        result = gsub("\\\\\"","''", result, perl=TRUE)
# Map SciDB missing (aka null) to NA, but preserve DEFAULT null.
# This sucky parsing is not a problem for binary transfers.
        result = gsub("DEFAULT null","@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo", result, perl=TRUE)
        result = gsub("null","NA", result, perl=TRUE)
        result = gsub("@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo","DEFAULT null", result, perl=TRUE)
        val = textConnection(result)
        ret = c()
        if(length(val) > 0)
          ret = tryCatch(read.table(val, sep=",", stringsAsFactors=FALSE, header=TRUE, ...),
                error = function(e) stop("SciDB query error"))
        close(val)
        if(DEBUG) cat("R parsing time",(proc.time()-dt1)[3],"\n")
        ret
       }, error = function(e)
           {
             stop(e)
           })
      return(ans)
  } else
  {
    scidbquery(db, query, release=1, stream=0L)
  }
  invisible()
}


#' Upload R data to SciDB
#' @param db a scidb database connection returned from \code{\link{scidbconnect}}
#' @param x an R data frame, raw value, Matrix, matrix, or vector object
#' @param name a SciDB array name to use
#' @param start starting SciDB integer coordinate index (does not apply to data frames)
#' @param gc set to FALSE to disconnect the SciDB array from R's garbage collector
#' @param ... other options, see \code{\link{df2scidb}}
#' @return A \code{scidb} object
#' @export
as.scidb = function(db, x,
                    name,
                    start,
                    gc=TRUE, ...)
{
  if(missing(name))
  {
    name = tmpnam(db)
  }
  if(inherits(x, "raw"))
  {
    return(raw2scidb(db, x, name=name, gc=gc,...))
  }
  if(inherits(x, "data.frame"))
  {
    return(df2scidb(db, x, name=name, gc=gc, ...))
  }
  if(inherits(db, x, "dgCMatrix"))
  {
    return(.Matrix2scidb(db, x, name=name, start=start, gc=gc, ...))
  }
  return(matvec2scidb(db, x, name=name, start=start, gc=gc, ...))
}

#' Download SciDB data to R
#' @param x a \code{\link{scidb}} object (a SciDB array or expression)
#' @return An R \code{\link{data.frame}}
#' @examples
#' \dontrun{
#' s = scidbconnect()
#' x = scidb(s, "build(<v:double>[i=1:5], sin(i))")
#' R(x)
#'#  i          v
#'#1 1  0.8414710
#'#2 2  0.9092974
#'#3 3  0.1411200
#'#4 4 -0.7568025
#'#5 5 -0.9589243
#' }
#' @export
R = function(x)
{
  stopifnot(is.scidb(x))
  iquery(x@meta$db, x, `return`=TRUE)
}
