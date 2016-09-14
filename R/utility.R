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
                        auth_type=c("digest", "scidb"), protocol=c("http", "https"))
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
        scidbquery(query="list('libraries')", release=1, resp=TRUE, stream=0L),
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


#' List objects in a SciDB database
#' @param pattern a regular-expression style search pattern
#' @param type (character), one of the indicated list types
#' @param verbose (boolean), include attribute and dimension data when type="arrays"
#' @param n maximum lines of output to return
#' @return a list of matching SciDB database objects
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
#' @importFrom utils read.table
#' @export
iquery = function(query, `return`=FALSE, binary=TRUE, ...)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE == options("scidb.debug")[[1]]) DEBUG=TRUE
  if(is.scidb(query))  query = query@name
  qsplit = strsplit(query, ";")[[1]]
  m = 1
  n = -1    # Indicate to shim that we want all the output
  for(query in qsplit)
  {
# Only return the last query, mimicking command-line iquery.
    if(`return` && m == length(qsplit))
    {
      if(binary)
      {
        return(scidb_unpack_to_dataframe(query, ...))
      }
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
    } else
    {
      ans = scidbquery(query, release=1, stream=0L)
    }
    m = m + 1
  }
  if(!(`return`)) return(invisible())
  ans
}

#' Method to connect to a host, run SciDB query and return R dataframe
#' 
#' Alternate way of running a SciDB query. This does not require scidbconnect();
#' this function directly connects to the specified \code{host} at the specified
#' \code{port} to issue the specified \code{query}. The caller must also have (and
#' provide) the following information about the return data type -- the name,
#' data type and nullability characteristic of each returned attribute.  Note
#' that this method is for use when you want the result returned to an R
#' dataframe; if you want to return some delimited output (e.g. CSV, TSV) or if
#' you want to use SciDBR to issue queries without returning a result, use the
#' \code{iquery()} function instead
#' @param aflstr the AFL query as a character string
#' @param dims vector of returned result dimensions
#' @param attribs a vector of character strings listing the names of the attributes returned by the query
#' @param TYPES a vector of character strings listing the data-types of the attributes returned by the query
#' @param NULLABILITY a vector of logical values listing the nullability characteristic of the attributes returned by the query
#' @param host optional host name or I.P. address of a SciDB shim service to connect to (character)
#' @param port optional port number of a SciDB shim service to connect to (integer)
#' @examples
#' \dontrun{
#' # Connect to one R session and execute the following commands
#' # The store(apply(..)) query creates a temporary array `temp_demo` for demo purposes
#' library(scidb)
#' scidbconnect()
#' iquery("store(apply(build(<val1:double>[i=0:4,5,0, j=0:3,2,0], random()), val2, i*10+j), temp_demo)")
#'
#' # Now disconnect from this R session and restart another R session
#' # In the subsequent code, we will not use scidbconnect() explicitly
#' library(scidb)
#' aflstr="between(temp_demo,0,0,2,2)"
#' TYPES=c("double", "int64")
#' NULLABILITY=c(FALSE, FALSE)
#' attribs=c("val1", "val2")
#' dims=c("i","j")
#' query_to_df(aflstr=aflstr, dims=dims, attribs=attribs, TYPES=TYPES, NULLABILITY=NULLABILITY)
#' # Note that you can supply a modified query while keeping the otherparameters
#' #the same (so long as the return type remains the same)
#' query_to_df(aflstr="between(temp_demo,1,1,3,3)", dims=dims, attribs=attribs, TYPES=TYPES, NULLABILITY=NULLABILITY)
#' 
#' # The real advantage of using this way of issuing SciDB queries and
#' # returning the result to R dataframes is evident when comparing timings
#' # with the alternate method of using scidbconnect() and iquery()
#' # Method 1:
#' t1 = proc.time()
#' x1 = query_to_df(aflstr="between(temp_demo,1,1,3,3)",
#'                  dims=dims, attribs=attribs, TYPES=TYPES,
#'                  NULLABILITY=NULLABILITY)
#' (proc.time()-t1)[[3]]
#' # [1] 0.069
#'
#' # Method 2:
#' t1 = proc.time()
#' scidbconnect()
#' x2 = iquery("between(temp_demo,1,1,3,3)", return=TRUE)
#' (proc.time()-t1)[[3]]
#' # [1] 0.419 
#'
#' # Finally verify that outputs are identical
#' identical(x1, x2)
#' # [1] TRUE
#' 
#' # Clean up
#' scidbrm("temp_demo", force=TRUE)
#' }
#' @export
query_to_df = function(aflstr, dims, attribs, TYPES, NULLABILITY, 
                      host = options("scidb.default_shim_host")[[1]], 
                      port = options("scidb.default_shim_port")[[1]]
)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE == options("scidb.debug")[[1]]) DEBUG=TRUE

  aflstr = sprintf("project(apply(%s,%s),%s,%s)", 
                   aflstr, 
                   paste(dims, dims, sep=",", collapse=","), 
                   paste(dims, collapse=",") , 
                   paste(attribs, collapse=",")
                  )

  # Some basic string cleaning on the AFL string so that it can be passed to httr::GET e.g. SPACE is replaced with %20
  aflstr = oldURLencode(aflstr)

  # Formulate the shim url
  shimurl=paste("http://", host, ":", port, sep = "")
  
  # Connect to SciDB
  # /new_session
  id = httr::GET(paste(shimurl, "/new_session", sep=""))
  session = gsub("([\r\n])", "", rawToChar(id$content)) # the gsub is added to remove some trailing characters if present 

  # AFL query and save format
  TYPES=c(rep("int64", length(dims)), TYPES)
  NULLABILITY=c(rep(FALSE, length(dims)), NULLABILITY)
  savestr=sprintf("(%s)", 
                    paste(TYPES, 
                          ifelse(NULLABILITY,
                          "+null", ""), 
                          sep="", collapse = ",")
                 )
  
  # Now formulate and execute the query
  # /execute_query
  query = sprintf("%s/execute_query?id=%s&query=%s&save=%s",
                  shimurl, session, aflstr, savestr)
  resp = httr::GET(query)
  
  # View the status of the result
  if(DEBUG) { cat("save status: "); cat(sprintf("%d\n", resp$status_code)) }
  
  # Get the data returned by the previous save
  # /read_bytes
  resp = httr::GET(sprintf("%s/read_bytes?id=%s&n=0", shimurl, session))
  if (DEBUG) {
		cat("read status: "); cat(sprintf("%d\n", resp$status_code))
		print(resp)
	}
  
  # Release the session
  httr::GET(sprintf("%s/release_session?id=%s", shimurl, session))
  
  #BEGIN: Copied from scidbr/R/internal.R >> scidb_unpack_to_dataframe()
  len = length(resp$content)
  p = 0 
  ans = c()
  
  # BEGIN: Hard coded for tests
  buffer = 100000L
  # END: HARD CODED
  
  cnames = c(dims, attribs, "lines", "p")  # we are unpacking to a SciDB array, ignore dims
  n = length(dims) + length(attribs)
  
  while(p < len)
  {
    dt2 = proc.time()
    tmp   = .Call("scidb_parse", as.integer(buffer), TYPES, NULLABILITY, resp$content, as.double(p), PACKAGE="scidb")
    names(tmp) = cnames
    lines = tmp[[n+1]]
    p_old = p
    p     = tmp[[n+2]]
    if(DEBUG) cat("  R buffer ", p, "/", len, " bytes parsing time", (proc.time() - dt2)[3], "\n")
    dt2 = proc.time()
    if(lines > 0)
    {
          if("binary" %in% TYPES)
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
  ans = as.data.frame(ans)
  ans
  #END: Copied from scidbr/R/internal.R >> scidb_unpack_to_dataframe()
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
