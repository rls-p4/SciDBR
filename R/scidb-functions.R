#' @export
cbind.scidb = function(x, y)
{
  if(missing(y))
  {
    newdim=make.unique_(x@attributes, "j")
    nd = sprintf("%s[%s,%s=0:0,1,0]", build_attr_schema(x) ,build_dim_schema(x,bracket=FALSE), newdim)
    return(redimension(bind(x,newdim,0), nd))
  }
  if(is.scidb(x) && !is.scidb(y)) stop("cbind requires either a single argument or two SciDB arrays")
  i = intersect(dimensions(x), dimensions(y))
  if(length(i) < 1) stop("Non-conformable arrays") # XXX Should really try harder
  merge(x, y, by=i)
}

#' @export
rbind.scidb = function(x, y)
{
  c(x, y)
}

#' @export
`rownames<-.scidb` = function(x, value)
{
  stop("Not supported")
}

#' @export
`dimnames<-.scidb` = function(x, value)
{
  stop("Not supported. Use names<- to change attribute and dimension names.")
}

#' @export
row.names.scidb = function(x)
{
  NULL
}

#' @export
`row.names<-.scidb` = function(x, value)
{
  stop("Not supported. Use names<- to change attribute and dimension names.")
}

#' @export
names.scidb = function(x)
{
  c(x@dimensions, x@attributes)
}

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

#' @export
dimnames.scidb = function(x)
{
  x@dimensions
}

#' @export
`$.scidb` = function(x, ...)
{
  M = match.call()
  M[1] = call("[.scidb")
  M[2] = x
  M[4] = as.character(M[3])
  M[3] = expression(NULL)
  eval(M)
}

# data.frame subsetting wrapper, limited to special [] case.
# x: A Scidbdf array object
# ...: list of dimensions
# 
#' @export
`[.scidb` = function(x, ...)
{
  M = match.call()
  M = M[3:length(M)]
  i = vapply(1:length(M), function(j) is.null(tryCatch(eval(M[j][[1]], parent.frame()), error=function(e) c())), TRUE)
  if(! all(i)) stop("Use subset and project")
  scidb_unpack_to_dataframe(x)
}

#' @export
#' @S3method dim scidb
`dim.scidb` = function(x)
{
  ans = c(prod(as.numeric(scidb_coordinate_bounds(x)$length)), length(dimensions(x)) + length(scidb_attributes(x)))
  warnonce("count")
  ans
}

#' @export
`dim<-.scidb` = function(x, value)
{
  reshape(x, shape=value)
}

#' @export
str.scidb = function(object, ...)
{
  .scidbstr(object)
}
