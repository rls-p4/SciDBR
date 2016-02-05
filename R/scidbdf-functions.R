cbind.scidbdf = function(x, y)
{
  if(missing(y))
  {
    newdim=make.unique_(x@attributes, "j")
    nd = sprintf("%s[%s,%s=0:0,1,0]", build_attr_schema(x) ,build_dim_schema(x,bracket=FALSE), newdim)
    return(redimension(bind(x,newdim,0), nd))
  }
  if(is.scidbdf(x) && !is.scidbdf(y)) stop("cbind requires either a single argument or two SciDB arrays")
  i = intersect(dimensions(x), dimensions(y))
  if(length(i) < 1) stop("Non-conformable arrays") # XXX Should really try harder
  merge(x, y, by=i)
}

rbind.scidbdf = function(x, y)
{
  c(x, y)
}

colnames.scidbdf = function(x)
{
  c(x@dimensions, x@attributes)
}

rownames.scidbdf = function(x)
{
  NULL
}

`rownames<-.scidbdf` = function(x, value)
{
  stop("Not supported")
}

`dimnames<-.scidbdf` = function(x, value)
{
  stop("Not supported. Use names<- to change attribute and dimension names.")
}

row.names.scidbdf = function(x)
{
  NULL
}

`row.names<-.scidbdf` = function(x, value)
{
  stop("Not supported")
}

names.scidbdf = function(x)
{
  c(x@dimensions, x@attributes)
}

`names<-.scidbdf` = function(x,value)
{
stop("XXX WRITE ME")
  ans = attribute_rename(x,`new`=value)
  ans
}

dimnames.scidbdf = function(x)
{
  x@dimensions
}

`$.scidbdf` = function(x, ...)
{
  M = match.call()
  M[1] = call("[.scidbdf")
  M[2] = x
  M[4] = as.character(M[3])
  M[3] = expression(NULL)
  eval(M)
}

# data.frame subsetting wrapper.
# x: A Scidbdf array object
# ...: list of dimensions
# 
`[.scidbdf` = function(x, ...)
{
  stop("Use subset and project")
}

`dim.scidbdf` = function(x)
{
  ans = as.numeric(scidb_coordinate_bounds(x)$length)
  names(ans) = dimensions(x)
  ans
}

`dim<-.scidbdf` = function(x, value)
{
  reshape(x, shape=value)
}


str.scidbdf = function(object, ...)
{
  .scidbstr(object)
}

ncol.scidbdf = function(x) length(scidb_attributes(x)) + length(dimensions(x))
nrow.scidbdf = function(x) 
{
  ans = as.numeric(scidb_coordinate_bounds(x)$length)
  names(ans) = dimensions(x)
  ans
}

# This is consistent with regular data frames:
length.scidbdf = function(x) ncol(x)
