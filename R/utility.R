#' Evaluate an expression to \code{scidb} or \code{scidb} objects
#'
#' Force evaluation of an expression that yields a \code{scidb} or \code{scidb} object,
#' storing the result to a SciDB array when \code{eval=TRUE}.
#' @param expr a quoted SciDB expression \code{scidb} object
#' @param eval \code{FALSE} do not evaluate the expression in SciDB (leave as a view)
#' @param name (character) optional SciDB array name to store result to
#' @param gc (logical) optional, when TRUE tie result to R garbage collector
#' @param temp (logical, optional), when TRUE store as a SciDB temp array
#' @export
scidbeval = function(expr, eval=TRUE, name, gc=TRUE, temp=FALSE)
{
  ans = eval(expr)
  if(!(inherits(ans,"scidb"))) return(ans)
# If expr is a stored temp array, then re-use its name
  if(!is.null(ans@gc$temp) && ans@gc$temp && missing(name)) name=ans@name
  .scidbeval(ans@name, `eval`=eval, name=name, gc=gc, schema=schema(ans), temp=temp)
}

#' Create an R reference to a SciDB array or expression.
#' @param name a character string name of a stored SciDB array or a valid SciDB AFL expression
#' @param gc a logical value, \code{TRUE} means connect the SciDB array to R's garbage collector
#' @return a \code{scidb} object
#' @importFrom methods new show
#' @export
scidb = function(name, gc=FALSE)
{
  if(missing(name)) stop("array or expression must be specified")
  if(is.scidb(name))
  {
    query = name@name
    return(.scidbeval(name@name, eval=FALSE, gc=gc, depend=list(name)))
  }

  obj = new("scidb", name=name)
  obj@gc = new.env()
  obj@meta = new.env()
  delayedAssign("state", lazyeval(name), assign.env=obj@meta)
  delayedAssign("schema", get("state")$schema, eval.env=obj@meta, assign.env=obj@meta)
  delayedAssign("logical_plan", get("state")$logical_plan, eval.env=obj@meta, assign.env=obj@meta)

  if(gc)
  {
    if(length(grep("\\(", name)) == 0)
    {
      obj@gc$name = name
    }
    obj@gc$remove = TRUE
    reg.finalizer(obj@gc, function(e)
        {
          if (e$remove && exists("name", envir=e))
            {
              tryCatch(scidbremove(e$name, warn=FALSE), error = function(e) invisible())
            }
        }, onexit = TRUE)
  }
  obj
}

#' Is the object a temporary SciDB array
#' @param name a character name of a SciDB array or a \code{scidb} object
#' @return logical value indicating if \code{name} is temporary
#' @export
is.temp = function(name)
{
  if(is.scidb(name))
  {
    if(!is.null(name@gc$temp)) return(name@gc$temp)
    name = name@name
  }
  query = sprintf("filter(list('arrays'),name='%s')", name)
  ans = iquery(query,return=TRUE)
  if(nrow(ans)<1) return(FALSE)
  if(is.null(ans$temporary)) return(FALSE)
  ans$temporary == "true"
}

#' Connect to a SciDB database
#' @param host optional host name or I.P. address of a SciDB shim service to connect to
#' @param port optional port number of a SciDB shim service to connect to
#' @param username optional authentication username
#' @param password optional authentication password
#' @param auth_type optional SciDB authentication type
#' @param protocol optional shim protocol type
#' @note
#' The SciDB connection state is maintained internally to the \code{scidb}
#' package. We internalize state to facilitate operations involving \code{scidb}
#' objects.
#' Thus, only one open SciDB connection is supported at
#' a time.
#' One may connect to and use multiple SciDB databases by sequentially calling
#' \code{scidbconnect} between operations. Note that \code{scidb} objects are not
#' valid across different SciDB databases.
#' 
#' Use the optional \code{username} and \code{password} arguments with
#' \code{auth_type} set to "digest" to use HTTP digest authentication (see the
#' shim documentation to configure this).  Digest authentication may use either
#' "http" or "https" selected by the \code{protocol} setting.
#' Set \code{auth_type = "scidb"} to use SciDB authentication, which only
#' works over "https".
#'
#' Disconnection is automatically handled by the package.
#'
#' All arguments support partial matching.
#' @return \code{NULL} is returned invisibly; this function is used for its
#' side effect.
#' @examples
#' \dontrun{
#' scidbconnect()
#'
#' # SciDB 15.12 authentication example (using shim's default HTTPS port 8083)
#' scidbconnect(user="root", password="Paradigm4", auth_type="scidb", port=8083)
#' }
#' @importFrom digest digest
#' @importFrom openssl base64_encode
#' @export
scidbconnect = function(host=options("scidb.default_shim_host")[[1]],
                        port=options("scidb.default_shim_port")[[1]],
                        username, password,
                        auth_type=c("scidb", "digest"), protocol=c("http", "https"))
{
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
# Update the shim.version option
  options(shim.version=SGET("/version"))

# Update the scidb.version option
  v = strsplit(gsub("[A-z\\-]", "", gsub("-.*", "", getOption("shim.version"))), "\\.")[[1]]
  if(length(v) < 2) v = c(v, "1")
  options(scidb.version=sprintf("%s.%s", v[1], v[2]))

# Update the scidb.version option for older systems (shim version unreliable)
  if(!compare_versions(options("scidb.version")[[1]], 15.12))
  {
    v = iquery("list('libraries')", return=TRUE, binary=FALSE)
    options(scidb.version=sprintf("%s.%s", v$major[1], v$minor[1]))
  }

# Use the query ID from a query as a unique ID for automated
# array name generation.
  x = tryCatch(
        scidbquery(query="list('libraries')", release=1, resp=TRUE),
        error=function(e) stop("Connection error"))
  if(is.null(.scidbenv$uid))
  {
    id = tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
           error=function(e) stop("Connection error"))
    id = id[[length(id)]]
    assign("uid", id, envir=.scidbenv)
  }

# Save available operators
  assign("ops", iquery("list('operators')", `return`=TRUE, binary=FALSE), envir=.scidbenv)
  invisible()
}



#' Remove one or more scidb arrays
#' @param x (character), a vector or single character string listing array names
#' @param error (function), optional error handler function; use stop or warn, for example
#' @param force (optional boolean), if TRUE really remove this array, even if scidb.safe_remove=TRUE
#' @param warn (optional boolean), if FALSE supress warnings
#' @param recursive (optional boolean), if TRUE, recursively remove this array and its dependency graph
#' @return \code{NULL}
scidbremove = function(x, error=warning, force, warn=TRUE, recursive=FALSE)
{
  if(is.null(x)) return(invisible())
  if(missing(force)) force=FALSE
  errfun=error

  safe = options("scidb.safe_remove")[[1]]
  if(is.null(safe)) safe = TRUE
  if(!safe) force=TRUE
  uid = get("uid",envir=.scidbenv)
  if(is.scidb(x)) x = list(x)
  for(y in x)
  {
# Depth-first, a bad way to traverse this XXX improve
    if(recursive && (is.scidb(y)) && !is.null(unlist(y@gc$depend)))
    {
      for(z in y@gc$depend) scidbremove(z,error,force,warn,recursive)
    }
    if(is.scidb(y)) y = y@name
    if(grepl("\\(",y)) next  # Not a stored array
    query = sprintf("remove(%s)",y)
    if(grepl(sprintf("^R_array.*%s$",uid),y,perl=TRUE))
    {
      tryCatch( scidbquery(query, release=1, stream=0L),
                error=function(e) if(!recursive && warn)errfun(e))
    } else if(force)
    {
      tryCatch( scidbquery(query, release=1, stream=0L),
                error=function(e) if(!recursive && warn)errfun(e))
    } else if(warn)
    {
      warning("The array ",y," is protected from easy removal. Specify force=TRUE if you really want to remove it.")
    }
  }
  invisible()
}


# binary=FALSE is needed by some queries, don't get rid of it.
#' Run a SciDB query, optionally returning the result.
#' @param query a single SciDB query string
#' @param return if \code{TRUE}, return the result
#' @param binary set to \code{FALSE} to read result from SciDB in text form
#' @param ... additional options passed to \code{read.table} when \code{binary=FALSE}
#' @importFrom utils read.table
#' @export
iquery = function(query, `return`=FALSE, binary=TRUE, ...)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE == options("scidb.debug")[[1]]) DEBUG=TRUE
  if(is.scidb(query))  query = query@name
  n = -1    # Indicate to shim that we want all the output
  if(`return`)
  {
    if(binary) return(scidb_unpack_to_dataframe(query, ...))

    ans = tryCatch(
       {
        # SciDB save syntax changed in 15.12
        if(compare_versions(options("scidb.version")[[1]],15.12))
        { 
          sessionid = scidbquery(query, save="csv+:l", release=0)
        } else sessionid = scidbquery(query, save="csv+", release=0)
        dt1 = proc.time()
        result = tryCatch(
          {
            SGET("/read_lines", list(id=sessionid, n=as.integer(n + 1)))
          },
          error=function(e)
          {
             SGET("/cancel", list(id=sessionid))
             SGET("/release_session", list(id=sessionid), err=FALSE)
             stop(e)
          })
        SGET("/release_session", list(id=sessionid), err=FALSE)
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
    scidbquery(query, release=1, stream=0L)
  }
  invisible()
}
