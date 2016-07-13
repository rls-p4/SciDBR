#' Combine attributes from two SciDB arrays
#'
#' Concatenate arrays with common dimension schema by combining their attributes.
#' Equivalent to SciDB's "merge" operator, or \code{\link{aggregate}} with \code{merge=TRUE}.
#' @param x \code{scidb} array object
#' @param y \code{scidb} array object
#' @return \code{scidb} array object
#' @export
cbind.scidb = function(x, y)
{
  if(missing(y))
  {
    newdim=make.unique_(scidb_attributes(x), "j")
    nd = sprintf("%s[%s,%s=0:0,1,0]", build_attr_schema(x) ,build_dim_schema(x,bracket=FALSE), newdim)
    return(redimension(bind(x,newdim,0), nd))
  }
  if(is.scidb(x) && !is.scidb(y)) stop("cbind requires either a single argument or two SciDB arrays")
  i = intersect(dimensions(x), dimensions(y))
  if(length(i) < 1) stop("Non-conformable arrays") # XXX Should really try harder
  merge(x, y, by=i)
}

#' Concatenate two SciDB arrays
#'
#' Concatenate two SciDB arrays with common schema along their last dimension.
#' @param x \code{scidb} array object
#' @param y \code{scidb} array object
#' @return \code{scidb} array object
#' @export
rbind.scidb = function(x, y)
{
  c(x, y)
}


#' SciDB dimension and attribute names
#' @param x \code{scidb} array object
#' @return Character vector of names
#' @export
names.scidb = function(x)
{
  c(dimensions(x), scidb_attributes(x))
}

#' Renamed SciDB attributes and/or dimensions
#' @param x \code{scidb} array object
#' @param value a character vector with new dimension and attribute names, in that order
#' @return \code{scidb} array object
#' @export
`names<-.scidb` = function(x, value)
{
  d = dimensions(x)
  a = scidb_attributes(x)
  if(length(value) != length(d) + length(a)) stop("Wrong number of names")
  v1 = value[1:length(d)]
  v2 = value[(length(d) + 1):length(value)]
  attribute_rename(dimension_rename(x, `new`=v1), `new`=v2)
}

#' Names of array dimensions
#' @param x \code{scidb} array object
#' @return a vector of SciDB array dimension names
#' @export
dimnames.scidb = function(x)
{
  dimensions(x)
}

#' Projection onto an array attribute (variable)
#'
#' Use data frame dollar sign notation to project onto a single array attribute
#' @seealso \code{\link{project}}
#' @param x \code{scidb} array object
#' @param ... optional argument \code{drop=FALSE} not yet supported, but will be in a future version
#' @return \code{scidb} array object
#' @export
`$.scidb` = function(x, ...)
{
  M = match.call()[-(1:2)]
  if(length(M) == 0) return(x)
  a = pmatch(unlist(as.character(M)), scidb_attributes(x))
  if(is.na(a)) return(NULL)
  project(x, scidb_attributes(x)[a])
}

#' Projection onto array attributes (variables) or special return data to R operator
#'
#' Use empty indexing brackets to download the values of the SciDB array to R in a data frame.
#' @seealso \code{\link{project}}
#' @param x \code{scidb} array object
#' @param ... character vector of array attributes
#' @return \code{scidb} array object
#' @note Append the function argument \code{drop=TRUE} to strip SciDB
#' dimensions from the result (still returning a data frame).
#' @examples
#' \dontrun{
#' x <- as.scidb(head(iris))
#' print(x)          # shows SciDB array structure
#' print(x[])        # downloads data back to R
#'
#' x[, "Species"]    # data frame subset notation, returns a new SciDB array object
#' x[, "Species"][]  # Downloads to R
#' }
#' @export
`[.scidb` = function(x, ...)
{
  M = match.call()
  M = M[3:length(M)]
  drop = "drop" %in% names(M)
  if(drop)
  {
    drop = drop && M[["drop"]]
    M = M[-which(names(M) %in% "drop")]
  }
  if(length(M) > 2)
      stop("[,,...] numeric range indexing is no longer supported. Use subset instead.")
  projector = unlist(Map(function(j)
  {
    a = tryCatch(eval(M[j][[1]], parent.frame()), error=function(e) NULL)
    if(is.numeric(a)) a = names(x)[a]
    if(!(is.null(a) || is.character(a)))
      stop("[,,...] numeric range indexing is no longer supported. Use subset instead.")
    a
  }, j=1:length(M)))
  if(length(projector) > 0) return(project(x, projector))

  if(drop) return(scidb_unpack_to_dataframe(x)[, -seq(1, length(dimensions(x)))])
  scidb_unpack_to_dataframe(x)
}

#' @export
`dim.scidb` = function(x)
{
  ans = c(prod(as.numeric(scidb_coordinate_bounds(x)$length)), length(dimensions(x)) + length(scidb_attributes(x)))
  warnonce("count")
  ans
}

#' Set SciDB array dimension shape
#'
#' This function uses \code{\link{reshape_scidb}} to change array
#' dimensionality. Does not work with unbounded arrays.
#' @param x a \code{scidb} object
#' @param value a vector of dimension bounds
#' @note The product of the vector of new dimension bounds must equal the product of the old dimension bounds.
#' @seealso \code{\link{reshape_scidb}}
#' @return a \code{scidb} object
#' @export
`dim<-.scidb` = function(x, value)
{
  reshape_scidb(x, shape=value)
}

#' @export
str.scidb = function(object, ...)
{
  .scidbstr(object)
}
