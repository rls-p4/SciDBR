#' SciDB reshape operator
#' @param x a \code{scidb} object
#' @param schema optional new schema
#' @param shape optional vector of new array coordinate dimensions
#' @param dimnames optional new vector of array coordniate dimension name
#' @param start optional vector of new array starting coordinate index values
#' @param chunks optional vector of new array chunk sizes
#' @return a \code{scidb} object
#' @export
reshape_scidb = function(x, schema, shape, dimnames, start, chunks)
{
  if(!missing(schema))
  {
    if(is.scidb(schema)) schema=schema(schema) # <- that's nutty notation Malkovich!
    query = sprintf("reshape(%s,%s)",x@name,schema)
    return(.scidbeval(query, depend=list(x)))
  }
  if(missing(shape)) stop("Missing dimension shape")
  N = length(shape)
  if(missing(dimnames))
  {
    dimnames=letters[9:(9+N-1)]
  }
  if(missing(chunks))
  {
    chunks = ceiling(1e6^(1/N))
  }
  if(missing(start)) start = rep(0,N)
  shape = shape - 1 + start
  D = build_dim_schema(x, newstart=start, newnames=dimnames, newend=shape, newchunk=chunks)
  query = sprintf("reshape(%s,%s%s)", x@name,build_attr_schema(x), D)
  .scidbeval(query, depend=list(x))
}

#' SciDB repart operator
#' @param x a \code{scidb} object
#' @param schema optional new schema
#' @param upper optional vector of new array coordinate dimensions
#' @param chunk optional vector of new array chunk sizes
#' @param overlap optional vector of new array chunk overlaps
#' @return a \code{scidb} object
#' @export
repart = function(x, schema, upper, chunk, overlap)
{
  if(!missing(schema))
  {
    query = sprintf("repart(%s, %s)", x@name, schema)
    return(.scidbeval(query, depend=list(x)))
  }
  if(missing(upper)) upper = scidb_coordinate_end(x)
  if(missing(chunk)) chunk = scidb_coordinate_chunksize(x)
  if(missing(overlap)) overlap = scidb_coordinate_overlap(x)
  a = build_attr_schema(x)
  schema = sprintf("%s%s", a, build_dim_schema(x,newend=upper,newchunk=chunk,newoverlap=overlap))
  query = sprintf("repart(%s, %s)", x@name, schema)
  .scidbeval(query, depend=list(x))
}

#' SciDB redimension operator wrapper function
#' @param x a \code{scidb} object
#' @param schema optional new schema
#' @param dim optional vector of dimension and/or attribute names to redimension along
#' @note Redimension attributes must be of int64 type.
#' @return a \code{scidb} object
#' @examples
#' \dontrun{
#' x <- as.scidb(iris)
#' y <- redimension(transform(x, i="int64(Petal_Width * 10)"), dim="i")
#' z <- redimension(transform(x, i="int64(Petal_Width * 10)"), dim=c("row", "i"))
#' schema(x)
#' schema(y)
#' schema(z)
#' }
#' @export
redimension = function(x, schema, dim)
{
  if(!(class(x) %in% "scidb")) stop("Invalid SciDB object")
# NB SciDB NULL is not allowed along a coordinate axis prior to SciDB 12.11,
# which could lead to a run time error here.
  if(missing(schema)) schema = NULL
  if(missing(dim)) dim = NULL
  s = schema
  if((class(s) %in% "scidb")) s = schema(s)
  dnames = c()
  if(!is.null(dim))
  {
    d = unique(unlist(dim))
    dnames = vector("list",length(dim))
    ia = which(scidb_attributes(x) %in% d)
    if(is.numeric(d)) id = d
    else id = which(dimensions(x) %in% d)
    if(length(ia) < 1 && length(id) < 1) stop("Invalid dimensions")
    as = build_attr_schema(x, I=-ia)
    if(length(id > 0))
    {
      if(is.null(s)) s = schema(x)
      ds = build_dim_schema(s, I=id, bracket=FALSE) # Note use of s here
    } else
    {
      ds = c()
    }
    if(length(ia) > 0)
    {
# We'll be converting attributes to dimensions here.
# First, we make sure that they are all int64.
      xold = x
      for(nid in x@attributes[ia])
      {
        idx = which(x@attributes %in% nid)
        if(scidb_types(x)[idx] != "int64") stop("redimension attributes must be of type int64")
      }

# Add the new dimension(s)
      a = x@attributes[ia]
      x@attributes = x@attributes[-ia]
      f = paste(paste("min(", a, "), max(", a, ")", sep=""), collapse=",")
      m = cbind(rep("0", length(a)), rep("*", length(a)))
      p = prod(as.numeric(scidb_coordinate_chunksize(x)[id]))
      chunk = ceiling((1e6 / p) ^ (1 / length(ia)))
      new = apply(m, 1, paste, collapse=":")
      new = paste(a, new, sep="=")
      new = paste(new, noE(chunk), "0", sep=",")
      new = paste(new, collapse=",")
      ds = ifelse(length(ds) > 0, paste(ds, new, sep=","), new)
    }
# Re-order new dimension schema to fit order specified in `dim`
    ds = paste(.dimsplit(sprintf("[%s]", ds))[dim], collapse=",")
    s = sprintf("%s[%s]", as, ds)
  }
# Check to see if the new schema is identical to the original schema.
# If so, don't bother with redimension, and return the input
  if(isTRUE(compare_schema(x, s)))
  {
    return(x)
  }
  query = sprintf("redimension(%s,%s)", x@name, s)
  ans = .scidbeval(query, `eval`=FALSE, depend=list(x))
  ans
}
