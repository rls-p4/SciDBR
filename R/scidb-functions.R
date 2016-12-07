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

#' @export
str.scidb = function(object, ...)
{
  .scidbstr(object)
}
