#' Evaluate an Expression to \code{scidb} or \code{scidb} objects
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
  .scidbeval(ans@name, `eval`=eval, name=name, gc=gc, schema=ans@schema, temp=temp)
}

#' Create an R reference to a SciDB array or expression.
#' @param name a character string name of a stored SciDB array or a valid SciDB AFL expression
#' @param gc a logical value, \code{TRUE} means connect the SciDB array to R's garbage collector
#' @return a \code{scidb} object
#' @export
scidb = function(name, gc=FALSE)
{
  if(missing(name)) stop("array or expression must be specified")
  if(is.scidb(name))
  {
    query = name@name
    return(.scidbeval(name@name, eval=FALSE, gc=gc, depend=list(name)))
  }
  escape = gsub("'","\\\\'", name, perl=TRUE)
# SciDB explain_logical operator changed in version 15.7
  if(compare_versions(options("scidb.version")[[1]], 15.7))
  {
    query = sprintf("join(show('filter(%s,true)','afl'), _explain_logical('filter(%s,true)','afl'))", escape, escape)
  }
  else
  {
    query = sprintf("join(show('filter(%s,true)','afl'), explain_logical('filter(%s,true)','afl'))", escape, escape)
  }
  query = iquery(query, `return`=TRUE, binary=FALSE)
# NOTE that we need binary=FALSE here to avoid a terrible recursion
  logical_plan = query$logical_plan
  schema = gsub("^.*<", "<", query$schema, perl=TRUE)
  obj = scidb_from_schemastring(schema, name)
  obj@logical_plan = logical_plan
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
  } else obj@gc = new.env()
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
#' 
#' Thus, only one open SciDB connection is supported at
#' a time.
#' 
#' One may connect to and use multiple SciDB databases by sequentially calling
#' \code{scidbconnect} between operations. Note that \code{scidb} objects are not
#' valid across different SciDB databases.
#' 
#' Use the optional \code{username} and \code{password} arguments to authenticate
#' the connection with the shim service. PAM-authenticated connections require
#' an encrypted connection with shim, available by default on port 8083.
#' 
#' Use the optional \code{username} and \code{password} arguments with
#' \code{auth_type} set to "digest" to use HTTP digest authentication (see the
#' shim documentation to configur this).  Digest authentication may use either
#' "http" or "https" transports selected by the \code{protocol} setting.
#'
#' Disconnection is automatically handled by the package.
#' @return \code{NULL} is returned invisibly; this function is used for its
#' side effect.
#' importFrom digest digest
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
      auth = paste(username, password, sep=":")
      assign("auth", auth, envir=.scidbenv)
    } else # HTTP basic digest auth
    {
      assign("digest", paste(username, password, sep=":"), envir=.scidbenv)
    }
  }

# Use the query ID from a query as a unique ID for automated
# array name generation.
  x = tryCatch(
        scidbquery(query="setopt('precision','16')", release=1, resp=TRUE, stream=0L),
        error=function(e) stop("Connection error"))
  if(is.null(.scidbenv$uid))
  {
    id = tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
           error=function(e) stop("Connection error"))
    id = id[[length(id)]]
    assign("uid", id, envir=.scidbenv)
  }
# Try to load the accelerated_io_tools, then load_tools, then
# prototype_load_tools libraries:
  got_load = tryCatch(
    {
      scidbquery(query="load_library('accelerated_io_tools')",
               release=1, resp=FALSE, stream=0L)
      TRUE
    }, error=function(e) FALSE)
  if(!got_load) got_load = tryCatch(
    {
      scidbquery(query="load_library('load_tools')",
               release=1, resp=FALSE, stream=0L)
      TRUE
    }, error=function(e) FALSE)
  if(!got_load) got_load = tryCatch(
    {
      scidbquery(query="load_library('prototype_load_tools')",
               release=1, resp=FALSE, stream=0L)
      TRUE
    }, error=function(e) FALSE)
  if(!got_load) warning("The load_tools SciDB plugin can't be found. load_tools is required to upload data.frames from R to SciDB. You can install the plugin from https://github.com/Paradigm4/load_tools")
# Try to load the dense_linear_algebra library
  tryCatch(
    scidbquery(query="load_library('dense_linear_algebra')",
               release=1, resp=FALSE, stream=0L),
    error=invisible)
# Try to load the example_udos library (>= SciDB 13.6)
  tryCatch(
    scidbquery(query="load_library('example_udos')", release=1, resp=FALSE, stream=0L),
    error=invisible)
# Try to load the superfunpack
  tryCatch(
    scidbquery(query="load_library('superfunpack')", release=1, resp=FALSE, stream=0L),
    error=invisible)
# Try to load the P4 library
  tryCatch(
    scidbquery(query="load_library('linear_algebra')", release=1, resp=FALSE, stream=0L),
    error=invisible)
# Try to load the chunk_unique library
  tryCatch(
    scidbquery(query="load_library('cu')", release=1, resp=FALSE, stream=0L),
    error=invisible)
# Save available operators
  assign("ops", iquery("list('operators')", `return`=TRUE, binary=FALSE), envir=.scidbenv)
# Update the scidb.version option
  v = scidbls(type="libraries")[1,]
  if("major" %in% names(v))
  {
    options(scidb.version=paste(v$major, v$minor, sep="."))
  }
# Update the shim.version option
  options(shim.version=SGET("/version"))

  invisible()
}


#' List objects in a SciDB database
#' @param type (character), one of the indicated list types
#' @param verbose (boolean), include attribute and dimension data when type="arrays"
#' @param n maximum lines of output to return
#' @return a list
#' @export
scidblist = function(pattern,
                     type= c("arrays", "operators", "functions", "types",
                             "aggregates", "instances", "queries", "libraries"),
                     verbose=FALSE, n=Inf)
{
  type = match.arg(type)
  if(n==Inf) n = -1   # non-intuitive read.table syntax
  Q = iquery(paste("list('",type,"')",sep=""), return=TRUE, nrows=n, binary=FALSE)
  if(dim(Q)[1] == 0) return(NULL)
  z = Q[, -1, drop=FALSE]
  if(type=="arrays" && !verbose) {
    z = z[,1]
    if(!missing(pattern))
      z = grep(z, pattern=pattern, value=TRUE)
  }
  z
}
#' List objects in a SciDB database
#' @param ... options passed to \code{\link{scidblist}}
#' @return a list
#' @export
scidbls = function(...) scidblist(...)


#' Remove one or more scidb arrays
#' @param x (character), a vector or single character string listing array names
#' @param error (function), optional error handler function; use stop or warn, for example
#' @param force (optional boolean), if TRUE really remove this array, even if scidb.safe_remove=TRUE
#' @param warn (optional boolean), if FALSE supress warnings
#' @param recursive (optional boolean), if TRUE, recursively remove this array and its dependency graph
#' @return \code{NULL}
#' @export
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
#' Remove one or more scidb arrays
#' @param x a vector or single character string listing array names
#' @param error  error handler. Use stop or warn, for example.
#' @param ... additional options for \code{\link{scidbremove}}
#' @return \code{NULL}
#' @export
scidbrm = function(x, error=warning, ...) scidbremove(x, error, ...)


# binary=FALSE is needed by some queries, don't get rid of it.
#' Run a SciDB query, optionally returning the result.
#' @param query a single SciDB query string
#' @param return if \code{TRUE}, return the result
#' @param binary set to \code{FALSE} to read result from SciDB in text form
#' @param ... additional options passed to \code{read.table} when \code{binary=FALSE}
#' @export
iquery = function(query, `return`=FALSE, binary=TRUE, ...)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE == options("scidb.debug")[[1]]) DEBUG=TRUE
  if(is.scidb(query))  query=query@name
  qsplit = strsplit(query,";")[[1]]
  m = 1
  n = -1    # Indicate to shim that we want all the output
  for(query in qsplit)
  {
# Only return the last query, mimicking command-line iquery.
    if(`return` && m == length(qsplit))
    {
      if(binary)
      {
        return(scidb_unpack_to_dataframe(query,...))
      }
      ans = tryCatch(
       {
        sessionid = scidbquery(query,save="lcsv+",release=0)
        dt1 = proc.time()
        result = tryCatch(
          {
            SGET("/read_lines", list(id=sessionid, n=as.integer(n+1)))
          },
          error=function(e)
          {
             SGET("/cancel", list(id=sessionid))
             SGET("/release_session", list(id=sessionid), err=FALSE)
             stop(e)
          })
        SGET("/release_session", list(id=sessionid), err=FALSE)
        if(DEBUG) cat("Data transfer time",(proc.time()-dt1)[3],"\n")
        dt1 = proc.time()
# Handle escaped quotes
        result = gsub("\\\\'","''", result, perl=TRUE)
        result = gsub("\\\\\"","''", result, perl=TRUE)
# Map SciDB missing (aka null) to NA, but preserve DEFAULT null.
# This sucks, need to avoid this parsing and move on to binary xfer.
        result = gsub("DEFAULT null","@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo", result, perl=TRUE)
        result = gsub("null","NA", result, perl=TRUE)
        result = gsub("@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo","DEFAULT null", result, perl=TRUE)
        val = textConnection(result)
        ret = tryCatch({
                read.table(val, sep=",", stringsAsFactors=FALSE, header=TRUE, ...)},
                error=function(e){ warning(e); c()})
        close(val)
        if(DEBUG) cat("R parsing time",(proc.time()-dt1)[3],"\n")
        ret
       }, error = function(e)
           {
             stop(e)
           })
    } else
    {
      ans = scidbquery(query, release=1, stream=0L)
    }
    m = m + 1
  }
  if(!(`return`)) return(invisible())
  ans
}

#' Recursively set all arrays in a \code{scidb} dependency graph to persist.
#' @param x a \code{\link{scidb}} object
#' @param gc \code{TRUE} connects arrays to R's garbage collector
#' @export
persist = function(x, gc=FALSE) UseMethod("persist")
persist.default = function(x, gc=FALSE)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  if(!is.scidb(x)) return(invisible())
  if(DEBUG) cat("Persisting ",x@name,"\n")
  x@gc$remove = gc
  if(is.null(unlist(x@gc$depend))) return()
  for(y in x@gc$depend)
  {
    if(DEBUG) cat("Persisting ",y@name,"\n")
    y@gc$remove = gc
    if(!is.null(y@gc$depend)) persist(y, gc=gc)
  }
}

persist.glm_scidb = function(x, gc=FALSE)
{
  .traverse.glm_scidb(x, persist, gc)
}

# A special remove function for complicated model objects
scidbremove.glm_scidb = function(x, error=warning,  force=FALSE, warn=TRUE, recursive=TRUE)
{
  .traverse.glm_scidb(x, scidbremove, error, force, warn, recursive)
}
