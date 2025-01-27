# Non-exported utility functions

## Helper functions
is.present <- function(a) { length(a) > 0 }
has.chars <- function(a) { is.present(a) && any(nzchar(a[!is.na(a)])) }
`%||%` <- function(a, b) { if (is.present(a)) a else b }
first <- function(a) { if (length(a) > 0) a[[1]] else NULL }
only <- function(a) {
  if (length(a) == 1) a[[1]] else stop("Expected 1 item, got ", length(a))
}

#' Make a "handshake" with the server to figure out which API to use.
#' When this function returns value "rv":
#'   - attr(rv, "connection")$scidb.version will be the server version
#'   - class(rv) will be the correct class to handle API calls
#'   - Other fields on rv and attr(rv, "connection") will contain necessary
#'     information for the subclass to talk to the server
#'   - All other fields are inherited from the input argument "db".
#' @param db a scidb database connection from \code{\link{scidbconnect}}
#' @return db with modifications reflecting information from the handshake
#' @noRd
Handshake = function(db)
{
  conn <- attr(db, "connection")
  
  ## First assume the hostname/port is for the HTTP API.
  ## If protocol is "http" and no username is present, try it unencrypted.
  httpapi_result <- NULL
  if (conn$protocol == "http" && !is.present(conn$username)) {
    httpapi_result <- tryCatch(
      list(db=GetServerVersion.httpapi(db)),
      error=function(err) { 
        if (length(err) == 1 && err[[1]] == "https required") {
          ## We tried HTTP, but HTTPS is required. Suppress the error,
          ## and set httpapi_result to NULL so we try again with HTTPS below.
          ## (Note failing from http to https is safe, 
          ##  but failing from https to http would not be safe.)
          NULL
        }
        list(error=err) 
      }
    )
  }
  
  ## If we didn't try the HTTP API yet - or if we got an "https required" 
  ## error when we did - try the HTTP API via https.
  if (is.null(httpapi_result)) {
    https_db <- db
    attr(https_db, "connection") <- rlang::env_clone(conn)
    attr(https_db, "connection")$protocol <- "https"
    httpapi_result <- tryCatch(
      list(db=GetServerVersion.httpapi(https_db)),
      error=function(err) { list(error=err) }
    )
  }
  if (!is.null(httpapi_result[["db"]])) {
    ## Return the copy of db with modifications made in GetServerVersion.httpapi
    ## (and possibly using https instead of http)
    return(httpapi_result[["db"]])
  }
  
  ## The HTTP API endpoint didn't work, so assume this server/port is for 
  ## the Shim.
  shim_result <- tryCatch(
    list(db=GetServerVersion.shim(db)),
    error=function(err) { list(error=err) }
  )
  if (!is.null(shim_result[["db"]])) {
    ## Return the copy of db with modifications made in GetServerVersion.shim
    return(shim_result[["db"]])
  }
  
  ## Neither worked, so throw an error with both error messages.
  conn <- attr(db, "connection")
  stop("Could not connect to either httpapi or shim on ", 
       conn$host, ":", conn$port, ".\n",
       "  httpapi connection error: ", httpapi_result[["error"]], "\n",
       "  shim connection error: ", shim_result[["error"]])
}

#' Unpack and return a SciDB query expression as a data frame
#' @param db scidb database connection object
#' @param query A SciDB query expression or scidb object
#' @param binary optional logical value. If \code{FALSE} use text transfer, 
#'    otherwise binary transfer. Defaults to \code{TRUE}.
#' @param buffer_size optional integer. Initial parse buffer size in bytes, 
#'    adaptively resized as needed. Larger buffers can be faster but consume
#'    more memory. Default size is 100000L. (Previously called "buffer";
#'    that parameter name is accepted too but it is deprecated.)
#' @param only_attributes optional logical value. \code{TRUE} means
#'    don't retrieve dimension coordinates, only return attribute values.
#'    Defaults to \code{NULL} which means "return coordinates unless the array
#'    is a dataframe".
#' @param schema optional result schema string, only applies when \code{query} 
#'    is not a SciDB object. Supplying this avoids one extra metadata query to
#'    determine result schema. Defaults to \code{schema(query)}.
#' @keywords internal
#' @importFrom curl new_handle handle_setheaders handle_setopt curl_fetch_memory
#'    handle_setform form_file
#' @importFrom data.table  data.table
#' @import bit64
#' @noRd
scidb_unpack_to_dataframe = function(db, query, binary=TRUE, buffer=NULL, 
                                     only_attributes=NULL, schema=NULL, 
                                     buffer_size=NULL, ...)
{
  trace <- .TraceEnterInternalFn("scidb_unpack_to_dataframe", query=query,
                                 binary=binary, buffer=buffer,
                                 only_attributes=only_attributes,
                                 schema=schema, buffer_size=buffer_size, ...)
  on.exit(.TraceExit(trace, returnValue()), add=TRUE)

  return(
    Query(db, 
          query,
          binary=binary, 
          buffer_size=if (is.null(buffer_size)) buffer else buffer_size, 
          only_attributes=only_attributes, 
          schema=schema,
          `return`=TRUE,
          ...))
}

#' Given _either_ a scidb database connection object _or_ 
#' its attr(, "connection") env, return the connection env.
#' @param db_or_conn a scidb database connection object,
#'    _or_ a connection env 
#' @return a connection env
#' @noRd
.GetConnectionEnv = function(db_or_conn)
{
  if (inherits(db_or_conn, "afl")) {
    return(attr(db_or_conn, "connection"))
  }
  if (is.environment(db_or_conn)) {
    return(db_or_conn)
  }
  stop("Not a connection or connection env: {", db_or_conn, "}")
}

#' Given _either_ a query string _or_ an object of class "scidb",
#' return the query string.
#' @param query_or_scidb a query string, _or_ an object of class "scidb"
#' @return a query string
#' @noRd
.GetQueryString = function(query_or_scidb)
{
  stopifnot(is.present(query_or_scidb))
  if (inherits(query_or_scidb, "scidb")) {
    return(query_or_scidb@name)
  }
  if (is.character(query_or_scidb) && ! is.na(query_or_scidb)) {
    return(query_or_scidb)
  }
  stop("Not a scidb object or query string: {", query_or_scidb, "}")
}

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
  if (at_least(attr(db, "connection")$scidb.version, "14.12")) TEMP="true"
  query   = sprintf("create_array(%s, %s, %s)", name, schema, TEMP)
  iquery(db, query, `return`=FALSE)
}

# Internal function
get_setting_items_str = function(db, settings, sep=',') {

  convert_single_item_v19 = function(key, value) {
    if(is.character(value))
      value = sprintf("'%s'", value)  # Quote string value(s)
    valueStr = if(length(value) > 1) sprintf("(%s)", paste(value, collapse = ',')) else value

    sprintf("%s:%s", key, valueStr)
  }
  convert_single_item_pre_v19 = function(key, value) {
    valueStr = if(length(value) > 1) paste(value, collapse = ',') else value
    sprintf("'%s=%s'", key, valueStr)
  }

  convert_single_item = if (at_least(attr(db, "connection")$scidb.version, "19.0"))
    convert_single_item_v19 else convert_single_item_pre_v19

  items = mapply(convert_single_item, names(settings), settings)
  paste(items, collapse = sep)
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
#' @noRd
`.scidbeval` = function(db, expr, eval=FALSE, name, gc, depend, schema, temp)
{
  ans = c()
  if (missing(depend)) depend = c()
  if (missing(schema)) schema = ""
  if (missing(temp)) temp = FALSE
  if (!is.list(depend)) depend = list(depend)
# Address bug #45. Try to cheaply determine if expr refers to a named array
# or an AFL expression. If it's a named array, then eval must be set TRUE.
  if (!grepl("\\(", expr, perl=TRUE)) eval = TRUE
  if (`eval`)
  {
    if (missing(name) || is.null(name))
    {
      newarray = tmpnam(db)
      if (temp) create_temp_array(db, newarray, schema)
      # No name means we're dealing with a temporary situation so set gc to TRUE
      gc = TRUE
    } else {
      newarray = name
      # If a name is provided, default to no gc
      if (missing(gc)) {
        gc = FALSE
      }
    }

    query = sprintf("store(%s,%s)", expr, newarray)
    Execute(db, query)
    ans = scidb(db, newarray, gc=gc)
    if (temp) ans@meta$temp = TRUE
  } else {
    ans = scidb(db, expr, gc=gc)
# Assign dependencies
    if (length(depend) > 0)
    {
      assign("depend", depend, envir=ans@meta)
    }
  }
  ans
}

make.names_ = function(x)
{
  gsub("\\.", "_", make.names(x, unique=TRUE), perl=TRUE)
}

# x is vector of existing values
# y is vector of new values
# returns a set the same size as y with non-conflicting value names
make.unique_ = function(x, y)
{
  z = make.names(gsub("_", ".", c(x, y)), unique=TRUE)
  gsub("\\.", "_", utils::tail(z, length(y)))
}


#' @return the unique ID of the connection
getuid = function(db)
{
  .scidbenv = attributes(db)$connection
  if (is.null(.scidbenv$id)) stop("Not connected...try scidbconnect")
  .scidbenv$id
}

tmpnam = function(db, prefix="R_array")
{
  stopifnot(inherits(db, "afl"))
  salt = basename(tempfile(pattern=prefix))
  paste(salt, getuid(db), sep="_")
}

# Supply the base SciDB URI from the global host, port and auth
# parameters stored in the "connection" environment in the db object
# Every function that needs to talk to the shim interface should use
# this function to supply the URI.
# Arguments:
# db_or_conn: scidb database connection object _or_ its "connection" attribute
# resource (string): A URI identifying the requested service
# args (list): A list of named query parameters
URI.default = function(db_or_conn, resource="", args=list())
{
  conn = .GetConnectionEnv(db_or_conn)
  
  if (is.null(conn$host)) stop("Not connected...try scidbconnect")

  proto = paste(conn$protocol, "//", sep=":")
  if ("password" %in% names(args) || "auth" %in% names(args)) {
    proto = "https://"
  }
  if (!is.null(conn$port)) {
    ans = paste(proto, conn$host, ":", conn$port, sep="")
  } else { 
    ## if port value is NULL, Shim port must have been forwarded to a URL
    ## and only having the URL is sufficient
    ans = paste(proto, conn$host, sep = "")
  }
  ans = paste(ans, resource, sep=if (substr(resource, 1, 1)!="/") "/" else "")
  if (length(args)>0) {
    ans = paste(ans, 
                paste(paste(names(args), args, sep="="), collapse="&"), 
                sep="?")
  }
  
  ## Mark this string as a URI
  class(ans) <- c("URI", class(ans))
  ans
}

#' Parse a version string, returning only the major and minor versions
#' @param version a version string
#' @return the version string with alphabetic characters removed, and
#'   the string trimmed to just the major and minor version
#' @noRd
ParseVersion <- function(version)
{
  v <- strsplit(gsub("[A-z\\-]", "", gsub("-.*", "", version)), "\\.")[[1]]
  if (length(v) < 2) {
    v <- c(v, "1")
  }
  return(sprintf("%s.%s", v[1], v[2]))
}

#' Unpack SciDB binary-encoded data into a data frame
#' @param data the binary-encoded data
#' @param cols a data.frame containing `names`, `type`, and `nullable` for each
#'    attribute or dimension in the binary-encoded data
#' @param buffer_size the size of the buffer to use for parsing
#' @param use_int64 if true, use the `integer64` package for large numbers; 
#'    if false, estimate large numbers with a float (loses precision) 
#' @return a data frame
#' @noRd
.Binary2df = function(data, cols, buffer_size=NULL, use_int64=TRUE)
{
  DEBUG = getOption("scidb.debug", FALSE)
  
  buffer_size = tryCatch(as.integer(buffer_size), warning=function(e) NULL)
  if (length(buffer_size) == 0 || buffer_size > 1e9) {
    buffer_size = 100000L
  }
  
  len = length(data)
  p = 0
  ans = c()
  cnames = c(cols$name, "lines", "p")  # we are unpacking to a SciDB array, ignore dims
  n = nrow(cols)
  rnames = c()
  typediff = setdiff(cols$type, names(.scidbtypes))
  if(length(typediff) > 0)
  {
    stop(typediff, " SciDB type not supported. Try converting to string in SciDB or use a binary=FALSE data transfer")
  }
  while (p < len)
  {
    dt2 = proc.time()
    tmp   = .Call(C_scidb_parse, as.integer(buffer_size), cols$type,
                  cols$nullable, data, as.double(p), as.integer(use_int64))
    names(tmp) = cnames
    lines = tmp[[n+1]]
    p_old = p
    p     = tmp[[n+2]]
    if (DEBUG) message("  R buffer_size ", p, "/", len, " bytes parsing time ", round( (proc.time() - dt2)[3], 4))
    dt2 = proc.time()
    if (lines > 0)
    {
      if ("binary" %in% cols$type)
      {
        ## Replicate weird behavior for backward compatibility: 
        ## if any column has type "binary", don't return a dataframe;
        ## instead, return a list consisting of just that binary value. 
        ## See issue #163 and the test for issue #163 in tests/a.R .
        if (DEBUG) {
          message("  R rbind/df assembly time ",
                  round( (proc.time() - dt2)[3], 4))
        }
        ans = lapply(1:n, function(j) tmp[[j]][1:lines])
        names(ans) = cols$name
        return(ans)
      }
      len_out = length(tmp[[1]])
      if (lines < len_out) tmp = lapply(tmp[1:n], function(x) x[1:lines])
      # adaptively re-estimate a buffer size
      avg_bytes_per_line = ceiling( (p - p_old) / lines)
      buffer_size = min(getOption("scidb.buffer_size"), ceiling(1.3 * (len - p) / avg_bytes_per_line)) # Engineering factors
      # Assemble the data frame
      ans = data.table::rbindlist(list(ans, tmp[1:n]))
      #      if (is.null(ans)) ans = data.table::data.table(data.frame(tmp[1:n], stringsAsFactors=FALSE, check.names=FALSE))
      #      else ans = rbind(ans, data.table::data.table(data.frame(tmp[1:n], stringsAsFactors=FALSE, check.names=FALSE)))
    }
    if (DEBUG) message("  R rbind/df assembly time ", round( (proc.time() - dt2)[3], 4))
  }
  
  if (is.null(ans)) {
    return(ans)
  }
  
  ## Clean up the data frame:
  ## 1. If there were int64 attributes, set their class to `integer64`
  ans = as.data.frame(ans, check.names=FALSE)
  if (use_int64)
  {
    for (i64 in which(cols$type %in% "int64")) {
      ## From https://advanced-r-solutions.rbind.io/s3.html :
      ## "oldClass() is basically the same as class(), except that it 
      ##  doesn’t return implicit classes, i.e. it’s basically 
      ##  attr(x, "class")". Unclear why it's used here instead of class().
      oldClass(ans[, i64]) = "integer64"
    }
  }
  ## 2. Handle datetime attributes (integer POSIX time)
  for (idx in which(cols$type %in% "datetime")) {
    ans[, idx] = as.POSIXct(ans[, idx], origin="1970-1-1", tz = "GMT")
  } 
  
  return(ans)
}

#' Decode Apache Arrow-encoded data into a data frame
#' @param arrow_data the Arrow-encoded data
#' @noRd
.Arrow2df <- function(arrow_data)
{
  return(arrow::read_ipc_stream(arrow_data, as_data_frame=TRUE))
}

.mapNullToNA <- function(textdata)
{
  # Map SciDB missing (aka null) to NA, but preserve DEFAULT null.
  # This sucky parsing is not a problem for binary transfers.
  textdata = gsub("DEFAULT null", "@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo", 
                  textdata, perl=TRUE)
  textdata = gsub("null", "NA", 
                  textdata, perl=TRUE)
  textdata = gsub("@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo", "DEFAULT null", 
                  textdata, perl=TRUE)
  return(textdata)
}

#' Parse CSV-formatted data into a data frame
#' @param csvdata the CSV-formatted string
#' @param header TRUE iff the first line of the CSV data is a header line
#' @param ... remaining arguments are passed to read.table()
#' @return a data frame
#' @seealso read.table()
#' @noRd
.Csv2df <- function(csvdata, header=TRUE, ...)
{
  # Handle escaped quotes
  csvdata = gsub("\\\\'", "''", csvdata, perl=TRUE)
  csvdata = gsub("\\\\\"", "''", csvdata, perl=TRUE)
  csvdata = .mapNullToNA(csvdata)
  
  val = textConnection(csvdata)
  on.exit(close(val), add=TRUE)
  ret = c()
  if (length(val) > 0)
  {
    # Remaining "..." arguments get passed to read.table,
    # overriding the defaults of sep=",", stringsAsFactors=FALSE, etc.
    args = list(file=val, 
                ...,  # Any settings in "..." override the ones below
                sep=",", 
                stringsAsFactors=FALSE, 
                header=header)

    # Remove any NULL values passed to args via "..."
    args = args[lengths(args) > 0]
    
    # If "..." contained "sep", "stringsAsFactors", or "header", ensure that
    # the value from "..." overrides the one hard-coded in this function.
    args = args[! duplicated(names(args))]

    # Silently ignore any arguments that would be rejected by read.table().
    # These can sometimes be passed unintentionally via default arguments
    # declared in another function.
    args = args[names(args) %in% names(as.list(args(read.table)))]
    
    ret = tryCatch(do.call("read.table", args=args),
                   error = function(e) {
                     stop("Query result parsing error ", as.character(e))
                   })
  }
  return(ret)
}

#' Parse TSV-formatted data into a data frame
#' @param tsvdata the TSV-formatted string
#' @param header TRUE iff the first line of the TSV data is a header line
#' @param ... remaining arguments are passed to read.delim()
#' @return a data frame
#' @seealso read.table()
#' @noRd
.Tsv2df <- function(tsvdata, header=TRUE, ...)
{
  tsvdata = .mapNullToNA(tsvdata)
  
  val = textConnection(tsvdata)
  on.exit(close(val), add=TRUE)
  ret = c()
  if (length(val) > 0)
  {
    args = list(file=val, 
                ..., 
                quote="",
                sep="\t",
                stringsAsFactors=FALSE, 
                header=header)
    args$only_attributes = NULL
    args = args[! duplicated(names(args))]
    ret = tryCatch(do.call("read.delim", args=args),
                   error = function(e) {
                     stop("Query result parsing error ", as.character(e))
                   })
  }
  return(ret)
}

.Text2df <- function(textdata, format, ...)
{
  if (startsWith(format, "aio_")) {
    format <- substring(format, 5)
  }
  header <- grepl(":.*l", format, perl=TRUE)
  if (startsWith(format, "csv")) {
    return(.Csv2df(textdata, header=header, ...))
  }
  if (startsWith(format, "tsv") || startsWith(format, "tdv")) {
    return(.Tsv2df(textdata, header=header, ...))
  }
  stop("Unknown text format '", format, "'")
}

#' Given a schema, return an empty dataframe for that schema.
#' @param attributes the schema's attributes (a dataframe with `name`, `type`)
#' @param dimensions the schema's dimensions (a dataframe with `name`)
#' @return an empty data frame with columns for the dimensions and attributes
#' @noRd
.Schema2EmptyDf <- function(attributes, dimensions=NULL)
{
  xa = attributes$name
  xd = NULL
  classes = list()
  classes_dimensions = NULL
  if (!is.null(dimensions)) {
    xd = dimensions$name
    classes_dimensions = rep("numeric", length(xd))
  }
  has_binary = FALSE
  for (i in 1:nrow(attributes)) {
    t = attributes$type[i]
    if(t == 'bool') {
      classes = c(classes, 'logical')
    } else if(t == 'binary') {
      classes = c(classes, 'list')
      has_binary = TRUE
    } else if(t == 'datetime') {
      classes[[length(classes) + 1]] = as.character(c('POSIXct', 'POSIXt'))
    } else if(t == 'string' || t == 'char') {
      classes = c(classes, 'character')
    } else if(t %in% c('int8', 'uint8', 'int16', 'uint16', 'int32')) {
      classes = c(classes, 'integer')
    } else {
      classes = c(classes, 'numeric')
    }
  }
  n = length(xd) + length(xa)
  ans = vector(mode="list", length=n)
  if (has_binary) {
    # C_scidb_parse leaves dimensions at the end,
    # for "binary" leave dimensions as they are
    classes = c(classes, classes_dimensions)
    names(ans) = make.names_(c(xa, xd))
    for(i in 1:length(ans)) {
      class(ans[[i]]) = classes[[i]]
    }
  } else {
    classes = c(classes_dimensions, classes)
    names(ans) = make.names_(c(xd, xa))
    class(ans) = "data.frame"
    for(i in 1:ncol(ans)) {
      # Workaround for POSIXct class
      if (length(classes[[i]]) == 2
          && all.equal(classes[[i]], as.character(c("POSIXct", "POSIXt")))) {
        class(ans[, i]) = 'numeric'
      }
      class(ans[, i]) = classes[[i]]
    }
  }
  return(ans)  
}

# Internal utility function used to format numbers
noE = function(w) sapply(w,
  function(x)
  {
    if (is.infinite(x)) return("*")
    if (is.character(x)) return(x)
    sprintf("%.0f", x)
  })

#' Returns TRUE if version string x is greater than or equal to than version y
#' @param x version string like "12.1", "15.12", etc. (non-numeric ignored)
#' @param y version string like "12.1", "15.12", etc. (non-numeric ignored)
#' @return logical TRUE if x is greater than or equal to y
#' @noRd
at_least = function(x, y)
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
  if(inherits(name, 'scidb')) name = name@name
  escape = gsub("'", "\\\\'", name, perl=TRUE)
  query = iquery(db, sprintf("show('filter(%s, true)', 'afl')", escape), `return`=TRUE, binary=FALSE, arrow=FALSE)
  # NOTE that we need binary=FALSE here to avoid a terrible recursion
  list(schema = gsub("^.*<", "<", query$schema, perl=TRUE),
       distribution = query$distribution)
}

#' Fast write.table/textConnection substitute
#'
#' Conversions are vectorized and the entire output is buffered in memory and written in
#' one shot. Great option for replacing writing to a textConnection (much much faster).
#' Not such a great option for writing to files, marginal difference from write.table and
#' obviously much greater memory use. This works for TSV or any character-
#' delimited format which doesn't require quoting, as long as the delimiter
#' doesn't appear in the data.
#' @param x a data frame
#' @param file a connection or \code{.Primitive("return")} to return character output directly (fast)
#' @param sep column separator
#' @param sprintf_template optional sprintf-style template, e.g. "%s,%s,%s"
#' @return Use for the side effect of writing to the connection returning \code{NULL}, or
#' return a character value when \code{file=return}.
#' @importFrom utils write.table
#' @keywords internal
#' @noRd
.TsvWrite = function(x, file=stdout(), sep="\t", sprintf_template=NULL)
{
  if (is.null(sprintf_template)) {
    sprintf_template = paste(rep("%s", ncol(x)), collapse=sep)
  }
  if (length(sprintf_template) > 1) {
    sprintf_template = paste(sprintf_template, collapse=sep)
  }
  foo = NULL
  rm(list="foo") # avoid package R CMD check warnings of undeclared variable
  if (!is.data.frame(x)) stop("x must be a data.frame")
  
  if (is.null(file) || ncol(x) > 97) # use slow write.table method
  {
    tc = textConnection("foo", open="w")
    write.table(x, sep=sep, col.names=FALSE, row.names=FALSE, 
                file=tc, quote=FALSE)
    close(tc)
    return(paste(foo, collapse="\n"))
  }
  
  if (is.function(file)) {
    return(paste(do.call("sprintf", args=c(sprintf_template, as.list(x))), 
                 collapse="\n"))
  }
  write(paste(do.call("sprintf", args=c(sprintf_template, as.list(x))), 
              collapse="\n"), 
        file=file)
  invisible()
}

#' Preprocess a dataframe to prepare it for being uploaded to SciDB. 
#' Returns a structured list containing information about the attributes
#' and data types to use for the upload, as well as a modified dataframe
#' that is based on the original, but with some values converted into 
#' SciDB-compatible datatypes and values.
#' @param X the dataframe being prepared for upload
#' @param types (optional) a list of datatypes, one for each column;
#'   if omitted, this will be derived from the class of every R column in X.
#' @param use_aio (optional) if TRUE, also prepares the dataframe for loading
#'   via the aio_input() operator, and populates $aio_apply_args in the 
#'   returned list.
#' @return a list containing:
#'   - $df: a copy of the input dataframe X, 
#'          with values converted into SciDB-compatible values
#'          and column names converted into SciDB-compatible identifiers
#'   - $attr_types: a list of strings representing the SciDB type for each
#'                  attribute
#'   - $aio_apply_args: (only if use_aio==TRUE)
#'                      a single string that can be pasted into the right-hand
#'                      side of an AFL apply(, ...) operator, which renames
#'                      the (a1, a2, ...) attributes created by the aio_input()
#'                      operator back to their expected attribute names,
#'                      and also does any post-load processing to convert the
#'                      values into the proper SciDB types.
#' @noRd
.PreprocessDfTypes = function(X, types=NULL, use_aio=FALSE)
{
  stopifnot(ncol(X) > 0)
  stopifnot(nrow(X) > 0)
  
  ## Turn the attribute names into unique SciDB identifiers
  anames = make.names_(names(X))
  if (length(anames) != ncol(X)) anames = make.names(1:ncol(X))
  if (!all(anames == names(X))) warning("Attribute names have been changed")
  names(X) = anames

  ## Use caller-provided list of types, defaulting to string if absent
  typ = rep("string", ncol(X))
  if (is.present(types)) {
    for (j in 1:ncol(X)) typ[j] = types[j]
  }
  
  ## Collect SciDB types of X's columns into `typ`.
  ## While doing so, adapt the values in X to conform to those types.
  for (j in 1:ncol(X)) {
    if ((! grepl("^int", typ[j])) && "numeric" %in% class(X[, j]))
    {
      if(is.null(types)) typ[j] = "double"
      X[, j] = gsub("NA", "null",
                    sprintf("%.17g",
                            ifelse(X[, j] > 0 & X[, j] < .Machine$double.xmin, 0,
                                   ifelse(X[, j] < 0 & X[, j] > -.Machine$double.xmin, 0, X[, j]))))
    }
    else if (grepl("^int64", typ[j]) || "integer64" %in% class(X[, j]))
    {
      if(is.null(types)) typ[j] = "int64"
      X[, j] = gsub("NA", "null",  sprintf("%s", bit64::as.integer64(X[, j])))
    }
    else if (grepl("^int", typ[j]) || "integer" %in% class(X[, j]))
    {
      if(is.null(types)) typ[j] = "int32"
      X[, j] = gsub("NA", "null", sprintf("%d", X[, j]))
    }
    else if ("logical" %in% class(X[, j]))
    {
      if(is.null(types)) typ[j] = "bool"
      X[, j] = gsub("na", "null", tolower(sprintf("%s", X[, j])))
    }
    else if ("character" %in% class(X[, j]))
    {
      if(is.null(types)) typ[j] = "string"
      X[is.na(X[, j]), j] = "null"
    }
    else if ("factor" %in% class(X[, j]))
    {
      if(is.null(types)) typ[j] = "string"
      isna = is.na(X[, j])
      X[, j] = sprintf("%s", X[, j])
      if (any(isna)) X[isna, j] = "null"
    }
    else if ("Date" %in% class(X[, j]) || "POSIXct" %in% class(X[, j]))
    {
      warning("Converting R Date/POSIXct to SciDB datetime as UTC time. Subsecond times rounded to seconds.")
      X[, j] = round(as.double(as.POSIXct(X[, j], tz="UTC")))
      X[, j] = gsub("NA", "null", sprintf("%d", X[, j]))
      if(is.null(types)) typ[j] = "datetime"
    }
  }
  
  result = list(df=X, attr_types=typ)
  
  if (use_aio) {
    ## The caller is planning to use `aio_input()` to load the data.
    ## aio_input() outputs all attribute names as (a1, a2, ...), so we need to
    ## construct a string that the caller can use in an "apply()" operator
    ## that maps the attributes back to their original names (while using
    ## dcast() to typecast the attributes to their proper datatypes).
    aio_apply_args = anames
    for (j in 1:ncol(X))
    {
      if (typ[j] == "datetime") {
        aio_apply_args[j] = sprintf("%s, datetime(a%d)", anames[j], j - 1)
      } else if (typ[j] == "string") {
        aio_apply_args[j] = sprintf("%s, a%d", anames[j], j - 1) 
      } else {
        aio_apply_args[j] = sprintf(
          "%s, dcast(a%d, %s(null))", anames[j], j - 1, typ[j])
      }
    }
    result$aio_apply_args = paste(aio_apply_args, collapse=", ")
  }
  
  return(result)
}

.PreprocessArrayType = function(X, type=NULL)
{
  if ("factor" %in% class(X)) {
    X = as.character(X)
  }
  
  ## attr_type: the type of the attribute
  ## load_type: the type used internally for loading the data
  attr_type = load_type = .Rtypes[[typeof(X)]]
  
  if ("Date" %in% class(X))
  {
    X = as.double(as.POSIXct(X, tz="UTC")) # XXX warn UTC?
    attr_type = "datetime"
  }
  if ("integer64" %in% class(X)) {
    load_type = attr_type = "int64"
  }
  if (is.null(load_type)) {
    stop(paste("Unsupported data type. The package supports: ",
               paste(unique(names(.Rtypes)), collapse=" "), ".", sep=""))
  }
  if (has.chars(type)) {
    # allow limited type conversion
    attr_type = only(as.character(type))
  }
  
  return(list(array=X, load_type=load_type, attr_type=attr_type))
}

.get_multipart_post_load_block_size <- function(data, debug, max_byte_size) 
{
  total_length = as.numeric(length(data))
  
  if(max_byte_size < 8) {
    warning('Supplied max_byte_size is less than 8 bytes. Restoring it to default value of 500MB.')
    max_byte_size=500*(10^6)
  }
  
  if(typeof(data) %in% c('integer', 'double')) {
    block_size = floor(max_byte_size / 8)
    if(debug) message("Using ", block_size, " for numeric vector block_size")
  } else {
    est_col_byte_size = max(c(1,nchar(data, type="bytes")), na.rm = T) * as.numeric(total_length)
    split_ratio = est_col_byte_size / max_byte_size
    block_size = ceiling(as.numeric(total_length)/split_ratio)
    if(debug) message("Using ", block_size, " for character vector block_size")
  }
  return(block_size)
}
