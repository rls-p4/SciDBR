## Functions for debugging and logging

# Use options(scidb.debug=TRUE) to enable debug printing.
is.debug <- function() { as.logical(getOption("scidb.debug", FALSE)) }

# Use either options(scidb.trace=TRUE) or options(scidb.debug=2)
# to enable trace printing.
is.trace <- function() {
  getOption("scidb.trace", getOption("scidb.debug", 0) >= 2)
}

# Use options(scidb.trace.http=TRUE) or options(scidb.trace=TRUE)
# to print HTTP communication between SciDBR and the scidb or Shim server.
is.trace.http <- function() {
  is.trace() || getOption("scidb.trace.http", FALSE)
}

# Use options(scidb.trace.api='exported') to trace exported API calls.
#   (TRUE will also work.)
# Use options(scidb.trace.api='internal') to trace internal calls too.
#   (options(scidb.trace=TRUE) will also enable API call tracing.)
is.trace.api <- function() {
  opt <- getOption("scidb.trace.api", "")
  is.trace() || opt == TRUE || has.chars(opt)
}

.Condense <- function(val)
{
  gsub("\\s+", " ", .ToString(val), perl=TRUE)
}

.Trunc <- function(str, newlen)
{
  if (nchar(str) <= newlen) {
    return(str)
  }
  return(paste0(strtrim(str, newlen-3), "..."))
}

.Indent <- function(n)
{
  return(paste0(rep(" ", 2 * n), collapse=""))
}

.FormatNameValuePairs <- function(vec,
                                  name_value_delimiter="=",
                                  item_delimiter=", ")
{
  if (length(vec) == 0) {
    return ("")
  }
  stopifnot(is.character(vec))
  paste0(sapply(seq_along(vec),
                function(ii) {
                  name <- names(vec)[[ii]]
                  if (has.chars(name)) {
                    paste0(name, name_value_delimiter, vec[[ii]])
                  } else {
                    vec[[ii]]
                  }
                }),
         collapse=item_delimiter)
}

## Put a call to .TraceEnter and .TraceExit in any function you want to trace.
##
## R has an alternative trace() facility, but it only displays the arguments 
## to a function in unevaluated form, so it's very difficult to see what 
## parameters the caller actually passed to a given function call.
.TraceEnter <- function(fnname, ...)
{
  if (!is.trace.api()) {
    return()
  }

  args <- sapply(list(...), function(x) .ToString(x))
  invocation <- paste0(fnname, "(", .FormatNameValuePairs(args), ")")
  
  depth <- as.numeric(getOption(".trace_depth", 0)) + 1
  options(.trace_depth = depth)
  max_chars <- as.numeric(getOption("scidb.trace.api.max-length", 99))
  
  msg(tag="[SciDBR-API]",
      .Indent(depth - 1),
      "->(", depth, ") ",
      .Trunc(invocation, max_chars))

  return(invocation)
}

.TraceEnterInternalFn <- function(...)
{
  if (getOption("scidb.trace.api", "") == "internal") {
    return(.TraceEnter(...))
  }
}

.TraceExit <- function(invocation, retval=NULL)
{
  if (!is.trace.api() || !has.chars(invocation)) {
    return()
  }

  depth <- as.numeric(getOption(".trace_depth"))
  max_chars <- getOption("scidb.trace.api.max-length", 99)
  msg(tag="[SciDBR-API]",
      .Indent(depth - 1),
      "<-(", depth, ") ",
      .Trunc(invocation, 30),
      " returns ",
      tryCatch(.Trunc(.ToString(retval), max_chars),
               error=function(err) "<unknown>"))
  options(.trace_depth = depth - 1)
}

.ToString <- function(val)
{
  if (length(val) == 0) {
    return("NULL")
  }
  if (is.character(val)) {
    if (length(val) == 1) {
      return(val)
    }
    return(paste0("(", .FormatNameValuePairs(val), ")"))
  }
  if (inherits(val, "afl")) {
    return(paste0("conn@", attr(val, "connection")$id))
  }
  if (inherits(val, "scidb")) {
    return(val@name)
  }
  
  ## Use toJSON() just because it's able to print many kinds of objects
  ##   including envs - better than format(), as.character(),
  ##   or other alternatives.
  ## Use force=TRUE to treat an unknown class as a plain list/env.
  result <- tryCatch(jsonlite::toJSON(val, force=TRUE, auto_unbox=TRUE),
                     error=function(err) NULL)
  if (has.chars(result)) {
    return(result)
  }
  return("<unknown>")
}

## Functions for writing diagnostic messages
## For now these are to stderr; they could easily be written to a log file instead
msg <- function(..., tag="[SciDBR]") { message(tag, " ", ...) }
msg.debug <- function(..., tag="[SciDBR]") {
  if (is.debug()) msg(tag=tag, ...)
}
msg.trace <- function(..., tag="[SciDBR-trace]") {
  if (is.trace()) msg(tag=tag, ...)
}
msg.trace.http <- function(..., tag="[SciDBR-HTTP]") {
  if (is.trace.http()) msg(tag=tag, ...)
}

.TruncateHttpData <- function(data, content_type)
{
  content_type <- content_type %||% ""
  if (startsWith(content_type, "application/json")) {
    max_chars <- getOption("scidb.trace.http.max-length.json", 2999)
    is_text <- TRUE
  } else if (startsWith(content_type, "text/") || is.character(data)) {
    max_chars <- getOption("scidb.trace.http.max-length.text", 199)
    is_text <- TRUE
  } else {
    max_chars <- getOption("scidb.trace.http.max-length.binary", 19)
    is_text <- FALSE
  }

  if (is_text) {
    text <- if (is.raw(data)) rawToChar(data) else data
    return(paste0(strtrim(text, max_chars),
                  if (nchar(text) > max_chars) "..." else ""))
  }
  return(paste0("(binary) ", paste0(data[1:max_chars], collapse=""),
                if (length(data) > max_chars) "..." else ""))
}

LogSendingHttp <- function(method, uri, headers=NULL, data=NULL, attachments=NULL)
{
  if (!is.trace.http()) {
    return()
  }

  msg.trace.http(">>> Sending HTTP Request: ", method, " ", uri)

  for (header in names(headers)) {
    msg.trace.http("     Header: ", header, ": ", headers[[header]])
  }

  if (has.chars(data)) {
    msg.trace.http("     Data: ",
                   .TruncateHttpData(data, headers[["Content-Type"]]))
  }

  for (iatt in seq_along(attachments)) {
    name <- names(attachments)[[iatt]]
    att <- attachments[[iatt]]
    msg.trace.http("     Attachment ", iatt,
                   " (name=", name, ", content-type=", att$content_type, "): ",
                   .TruncateHttpData(att$data, att$content_type))
  }
}

LogHttpReceived <- function(method, uri, resp)
{
  if (!is.trace.http()) {
    return()
  }
  
  msg.trace.http("<<< Received HTTP status code ", resp$status_code, " from ", method, " ", uri)

  headers <- curl::parse_headers_list(resp$headers)
  for (ii in seq_along(headers)) {
    msg.trace.http("     Header: ", names(headers)[[ii]], ": ", headers[[ii]])
  }

  if (length(resp$content) > 0) {
    content_type <- headers[["content-type"]]
    msg.trace.http("     Data: ", .TruncateHttpData(resp$content, content_type))
  }
}
