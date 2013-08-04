# Experimental routines August 2013
# DOCUMENT ME

`cross_join` = function(X,Y,...)
{
  M = match.call()
  doeval = ifelse(is.null(M$eval),TRUE,M$eval)
  M = M[-c(1,2,3)]
  if(!is.null(names(M))) M = M[!(names(M) %in% c("eval"))]

  query = sprintf("cross_join(%s as __X, %s as __Y", X@name, Y@name)
  if(length(M)>0)
  {
    E = parent.frame()
    i = lapply(1:length(M), function(j) tryCatch(eval(M[j][[1]],E),error=function(e)as.character(M[j][[1]])))
    cterms = paste(c("__X","__Y"), i, sep=".")
    cterms = paste(cterms,collapse=",")
    query  = paste(query,",",cterms,")",sep="")
  } else
  {
    query  = sprintf("%s)",query)
  }
  newarray = tmpnam()
  if(eval)
  {
    query = sprintf("store(%s,%s)",query,newarray)
    scidbquery(query)
    return(scidb(newarray,gc=TRUE))
  }
  query
}

# x:   A SciDB array
# by:  A list of dimension and/or attribute names in x to aggregate along
# FUN: A valid SciDB aggregation expression (string)
`aggregate.by` = function(x, by, FUN)
{
  b = by
  if(!all(b %in% c(x@attributes, x@D$name))) stop("Invalid attribute or dimension name in by")
  a = x@attributes %in% b
  query = x@name
  if(any(a))
  {
# We assume attributes are int64 here. Add support later for sort/unique/index_lookup.
    n = x@attributes[a]
# XXX What if an attribute has negative values? What about chunk sizes? NULLs? Ugh.
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
