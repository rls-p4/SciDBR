setGeneric("is.scidb", function(x) standardGeneric("is.scidb"))

#' Test if an object has class "scidb"
#' @param x a \code{scidb} object
#' @aliases is.scidb
#' @return \code{TRUE} if \code{x} has class "scidb"
#' @export
setMethod("is.scidb", signature(x="ANY"), 
function(x)
  {
    if(inherits(x, "scidb")) return(TRUE)
    FALSE
  }
)

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
