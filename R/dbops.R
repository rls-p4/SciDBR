# The functions and methods defined below are based closely on native SciDB
# functions, some of which have weak or limited analogs in R. The functions
# defined below work with objects of class scidb (arrays), scidbdf (data
# frames), or scidbexpr (generic scidb query strings). They can be efficiently
# nested by explicitly setting eval=FALSE on inner functions, deferring
# computation until eval=TRUE.

# An internal convenience function that conditionally evaluates a scidb
# query string `expr` (eval=TRUE), returning a scidb object,
# or returns a scidbexpr object (eval=FALSE).
`scidbeval` = function(expr,eval)
{
  if(`eval`)
  {
    newarray = tmpnam()
    query = sprintf("store(%s,%s)",expr,newarray)
    scidbquery(query)
    return(scidb(newarray,gc=TRUE))
  }
  scidbexpr(expr)
}

# Filter the attributes of the scidb, scidbdf, or scidbexpr object to contain
# only those specified in expr.
# X:    a scidb, scidbdf, or scidbexpr object
# expr: a character vector describing the list of attributes to project onto
# eval: a boolean value. If TRUE, the query is executed returning a scidb array.
#       If FALSE, a scidbexpr object describing the query is returned.
`project` = function(X,expr,eval=TRUE)
{
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  query = sprintf("project(%s,%s)", xname,paste(expr,collapse=","))
  scidbeval(query,eval)
}

# This is the SciDB filter operation, not the R timeseries one.
# X is either a scidb, scidbdf, or scidbexpr object.
# expr is a valid SciDB expression (character)
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a scidbexpr object representing the query.
`filter_scidb` = function(X,expr,eval=TRUE)
{
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  query = sprintf("filter(%s,%s)", xname,expr)
  scidbeval(query,eval)
}

# SciDB cross_join wrapper internal function to support merge on various
# classes (scidb, scidbdf, scidbexpr). This is an internal function to support
# merge on various SciDB objects.
# X and Y are SciDB array references of any kind (scidb, scidbdf, scidbexpr)
# by is either a single character indicating a dimension name common to both
# arrays to join on, or a two-element list of character vectors of array
# dimensions to join on.
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a scidbexpr object representing the query.
# Examples:
# merge(X,Y,by='i')
# merge(X,Y,by=list('i','i'))  # equivalent to last expression
# merge(X,Y,by=list(X=c('i','j'), Y=c('k','l')))
`merge_scidb` = function(X,Y,...)
{
  mc = match.call()
  if(is.null(mc$by)) `by`=list()
  else `by`=mc$by
  if(is.null(mc$eval))
  {
    `eval`=TRUE
  } else `eval`=mc$by
  xname = X
  yname = Y
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  if(class(Y) %in% c("scidbdf","scidb")) yname = Y@name

  query = sprintf("cross_join(%s as __X, %s as __Y", xname, yname)
  if(length(`by`)>1 && !is.list(`by`))
    stop("by must be either a single string describing a dimension to join on or a list in the form list(c('arrayX_dim1','arrayX_dim2'),c('arrayY_dim1','arrayY_dim2'))")
  if(length(`by`)>0)
  {
# Re-order list terms
    b = as.list(unlist(lapply(1:length(`by`[[1]]), function(j) unlist(lapply(`by`, function(x) x[[j]])))))
    cterms = paste(c("__X","__Y"), b, sep=".")
    cterms = paste(cterms,collapse=",")
    query  = paste(query,",",cterms,")",sep="")
  } else
  {
    query  = sprintf("%s)",query)
  }
  scidbeval(query,eval)
}


# aggregate_by_array is internal to the package.
# x is a scidb object.
# by may be a SciDB array or a list whose first element is a SciDB array
# and remaining elements are dimension names (character).
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a scidbexpr object representing the query.
aggregate_by_array = function(x,by,FUN,eval=TRUE)
{
  dims = c()
  if(is.list(by) && length(by)>1)
  {
    dims=unlist(by[-1])
    by=by[[1]]
  }
  j = intersect(x@D$name, by@D$name)
  X = merge(x,by,list(j,j),eval=FALSE)
  n = by@attributes
  x@attributes = union(x@attributes,by@attributes)
  a = x@attributes %in% n
# XXX What if an attribute has negative values? What about chunk sizes? NULLs? Ugh. Also insert reasonable upper bound instead of *?
# XXX Take care of all these issues...
  redim = paste(paste(n,"=0:*,10000,0",sep=""), collapse=",")
  D = paste(build_dim_schema(x,FALSE),redim,sep=",")
  A = x
  A@attributes = x@attributes[!a]
  A@nullable   = x@nullable[!a]
  A@types      = x@types[!a]
  S = build_attr_schema(A)
  D = sprintf("[%s]",D)
  query = sprintf("redimension(%s,%s%s)",X,S,D)
  along = paste(c(dims,n),collapse=",")
  query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  scidbeval(query,eval)
}

# Lots of documentation needed here!
`aggregate_scidb` = function(x,by,FUN,eval=TRUE)
{
  if("scidbexpr" %in% class(x)) x = scidb_from_scidbexpr(x)
  b = `by`
  if(is.list(b)) b = b[[1]]
  if(class(b) %in% c("scidb","scidbdf"))
    return(aggregate_by_array(x,`by`,FUN,eval))

  b = `by`
  if(!all(b %in% c(x@attributes, x@D$name))) stop("Invalid attribute or dimension name in by")
  a = x@attributes %in% b
  query = x@name
  if(any(a))
  {
# We assume attributes are int64 here. Add support for sort/unique/index_lookup.
    n = x@attributes[a]
# XXX What if an attribute has negative values? What about chunk sizes? NULLs? Ugh. Also insert reasonable upper bound instead of *?
# XXX Take care of all these issues...
    redim = paste(paste(n,"=0:*,1000,0",sep=""), collapse=",")
    D = paste(build_dim_schema(x,FALSE),redim,sep=",")
    A = x
    A@attributes = x@attributes[!a]
    A@nullable   = x@nullable[!a]
    A@types      = x@types[!a]
    S = build_attr_schema(A)
    D = sprintf("[%s]",D)
    query = sprintf("redimension(%s,%s%s)",x@name,S,D)
  }
  along = paste(b,collapse=",")
  query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  scidbeval(query,eval)
}


# Build the attibute part of a SciDB array schema from a scidb,
# scidbdf, or scidbexpr object.
`build_attr_schema` = function(A)
{
  if("scidbexpr" %in% class(A)) A = scidb_from_scidbexpr(A)
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  N = rep("",length(A@nullable))
  N[A@nullable] = " NULL"
  N = paste(A@types,N,sep="")
  S = paste(paste(A@attributes,N,sep=":"),collapse=",")
  sprintf("<%s>",S)
}

`noE` = function(w) sapply(w, function(x) sprintf("%.0f",x))

# Build the dimension part of a SciDB array schema from a scidb,
# scidbdf, or scidbexpr object.
`build_dim_schema` = function(A,bracket=TRUE)
{
  if("scidbexpr" %in% class(A)) A = scidb_from_scidbexpr(A)
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  notint = A@D$type != "int64"
  N = rep("",length(A@D$name))
  N[notint] = paste("(",A@D$type,")",sep="")
  N = paste(A@D$name, N,sep="")
  low = noE(A@D$low)
  high = noE(A@D$high)
  R = paste(low,high,sep=":")
  R[notint] = noE(A@D$length)
  S = paste(N,R,sep="=")
  S = paste(S,noE(A@D$chunk_interval),sep=",")
  S = paste(S,noE(A@D$chunk_overlap),sep=",")
  S = paste(S,collapse=",")
  if(bracket) S = sprintf("[%s]",S)
  S
}

# Sort of like cbind for data frames.
`bind` = function(X, name, FUN, eval=TRUE)
{
  aname = X
  if(class(X) %in% c("scidb","scidbdf")) aname=X@name
  if(length(name)!=length(FUN)) stop("name and FUN must be character vectors of identical length")
  expr = paste(paste(name,FUN,sep=","),collapse=",")
  query = sprintf("apply(%s, %s)",aname, expr)
  scidbeval(query,eval)
}

`sort_scidb` = function(X, decreasing = FALSE, ...)
{
  mc = list(...)
  if(!is.null(mc$na.last))
    warning("na.last option not supported by SciDB sort. Missing values are treated as less than other values by SciDB sort.")
  dflag = ifelse(decreasing, 'desc', 'asc')
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  EX = X
  if("scidbexpr" %in% class(X))
  {
    EX = scidb_from_scidbexpr(X)
  }
  if(is.null(mc$attributes))
  {
    if(length(EX@attributes)>1) stop("Array contains more than one attribute. Specify one or more attributes to sort on with the attributes= function argument")
    mc$attributes=EX@attributes
  }
  `eval` = ifelse(is.null(mc$eval), TRUE, mc$eval)
  a = paste(paste(mc$attributes, dflag, sep=" "),collapse=",")
  if(!is.null(mc$chunk_size)) a = paste(a, mc$chunk_size, sep=",")

  query = sprintf("sort(%s,%s)", xname,a)
  scidbeval(query,eval)
}

# S3 methods
`merge.scidb` = function(x,y,...) merge_scidb(x,y,...)
`merge.scidbdf` = function(x,y,...) merge_scidb(x,y,...)
`merge.scidbexpr` = function(x,y,...) merge_scidb(x,y,...)
`filter.scidb` = function(X,expr,eval=TRUE) filter_scidb(X,expr,eval)
`filter.scidbdf` = function(X,expr,eval=TRUE) filter_scidb(X,expr,eval)
`filter.scidbexpr` = function(X,expr,eval=TRUE) filter_scidb(X,expr,eval)
`sort.scidb` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`sort.scidbdf` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`sort.scidbexpr` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
