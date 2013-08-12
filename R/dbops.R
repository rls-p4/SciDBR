# Experimental routines August 2013

`merge.scidbdf` = function(X,Y,by,eval=TRUE) merge.scidb(X,Y,by,eval)

# SciDB cross_join wrapper
# X and Y are SciDB array references of any kind
# by is either a single character indicating a dimension name common to both
# arrays to join on, or a two-element list of character vectors of array
# dimensions to join on. Examples:
# merge(X,Y,by='i')
# merge(X,Y,by=list('i','i'))  # equivalent to last expression
# merge(X,Y,by=list(X=c('i','j'), Y=c('k','l')))
`merge.scidb` = function(X,Y,by,eval=TRUE)
{
  if(missing(`by`)) `by`=list()

  query = sprintf("cross_join(%s as __X, %s as __Y", X@name, Y@name)
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
  if(`eval`)
  {
    newarray = tmpnam()
    query = sprintf("store(%s,%s)",query,newarray)
    scidbquery(query)
    return(scidb(newarray,gc=TRUE))
  }
  query
}


# by may be a SciDB array or a list whose first element is a SciDB array
# and remaining elements are dimension names (character).
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
  redim = paste(paste(n,"=0:*,1000,0",sep=""), collapse=",")
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
  if(`eval`)
  {
    newarray = tmpnam()
    query = sprintf("store(%s,%s)",query,newarray)
    scidbquery(query)
    return(scidb(newarray,gc=TRUE))
  }
  query
}

aggregate.scidb = function(x,by,FUN,eval=TRUE)
{
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
  if(`eval`)
  {
    newarray = tmpnam()
    query = sprintf("store(%s,%s)",query,newarray)
    scidbquery(query)
    return(scidb(newarray,gc=TRUE))
  }
  query
}

`build_attr_schema` = function(A)
{
  N = rep("",length(A@nullable))
  N[A@nullable] = " NULL"
  N = paste(A@types,N,sep="")
  S = paste(paste(A@attributes,N,sep=":"),collapse=",")
  sprintf("<%s>",S)
}

`noE` = function(w) sapply(w, function(x) sprintf("%.0f",x))

`build_dim_schema` = function(A,bracket=TRUE)
{
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
