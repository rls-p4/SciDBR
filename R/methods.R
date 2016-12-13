.scidbstr = function(object)
{
  name = substr(object@name, 1, 35)
  if(nchar(object@name)>35) name = paste(name, "...", sep="")
  cat("SciDB expression ", name)
  cat("\nSciDB schema ", schema(object), "\n")
  bounds = scidb_coordinate_bounds(object)
  d = data.frame(variable=dimensions(object), dimension=TRUE, type="int64", nullable=FALSE, start=bounds$start, end=bounds$end, chunk=scidb_coordinate_chunksize(object), row.names=NULL, stringsAsFactors=FALSE)
  d = rbind(d, data.frame(variable=scidb_attributes(object),
                          dimension=FALSE,
                          type=scidb_types(object), nullable=scidb_nullable(object), start="", end="", chunk=""))
  cat(paste(utils::capture.output(print(d)), collapse="\n"))
  cat("\n")
}

.aflstr = function(object)
{
  conn = attr(object, "connection")
  message(sprintf("SciDB database connection %s:%s\nUse $ to access AFL operators; `ls` on this object lists SciDB arrays.", conn$host, conn$port))
}

#' Print a summary of a \code{scidb} object
#' @param x a \code{scidb} object
#' @return printed object summary
#' @export
setMethod("print", signature(x="scidb"),
  function(x) {
    show(x)
  })

#' Print a summary of a \code{scidb} object
#' @param object a \code{scidb} object
#' @return printed object summary
#' @export
setMethod("show", "scidb",
  function(object) {
    .scidbstr(object)
  })


#' SciDB dimension and attribute names
#' @param x \code{scidb} array object
#' @return Character vector of names
#' @export
names.scidb = function(x)
{
  c(dimensions(x), scidb_attributes(x))
}

#' Names of array dimensions
#' @param x \code{scidb} array object
#' @return a vector of SciDB array dimension names
#' @export
dimnames.scidb = function(x)
{
  dimensions(x)
}

#' @export
`dim.scidb` = function(x)
{
  ans = c(prod(as.numeric(scidb_coordinate_bounds(x)$length)), length(dimensions(x)) + length(scidb_attributes(x)))
  warnonce("count")
  ans
}

#' @importFrom utils str
#' @export
str.scidb = function(object, ...)
{
  .scidbstr(object)
}

#' @export
str.afl = function(object, ...)
{
  str(unclass(object))
}
setOldClass("afl")
#' Print a summary of a \code{afl} object
#' @param object \code{afl} object
#' @return printed object summary
#' @export
setMethod("show", "afl",
  function(object) {
    .aflstr(object)
  })
 #' Print a summary of a \code{afl} SciDB database connection object
 #' @param x  \code{afl} object
 #' @return printed object summary
 #' @export
setMethod("print", signature(x="afl"),
  function(x) {
    show(x)
  })
#' Print a summary of a \code{afl} SciDB database connection object
#' @param x  \code{afl} object
#' @param ... optional arguments (not used)
#' @return printed object summary
#' @method print afl
#' @export
print.afl = function(x, ...)
{
  .aflstr(x)
}

setGeneric("ls")
#' List contents of a SciDB database
#' @param name \code{afl} SciDB connection object from \code{\link{scidbconnect}}
#' @return a \code{data.frame} listing the contents of the database
#' @export
setMethod("ls", signature(name="afl"), 
  function(name) {
    iquery(name, "list()", return=TRUE)
  })

# AFL help
setGeneric("help")
setOldClass("operator")
#' AFL operator help
#' @param topic \code{afl} operator
#' @return help summary
#' @export
setMethod("help", signature(topic="operator"),
  function(topic) aflhelp(topic))

#' AFL array aliasing
#' @param x an object of class \code{\link{scidb}} (a scidb array or expression)
#' @param y alias name
#' @method  %as% default
#' @export
setGeneric("%as%", function(x, y) standardGeneric("%as%"))
#' AFL arrary aliasing
#' @param x an object of class \code{\link{scidb}} (a scidb array or expression)
#' @param y alias name
#' @return a \code{\link{scidb}} object
#' @note Use the \code{\%as\%} operator in place of the native AFL "as" operator
#' in AFL expressions written in R.
#' @examples
#' \dontrun{
#' # s = scidbconnect()
#' # x = scidb(s, "build(<v:double>[i=1:2,1,0], i)")
#' # x %as% y
#' }
#' @method %as% scidb ANY
#' @export
setMethod("%as%", signature(x="scidb", y="ANY"),
  function(x, y)
  {
    scidb(x@meta$db, sprintf("%s as %s", x@name, as.character(as.list(match.call())$y)))
  },
  valueClass="scidb"
)
