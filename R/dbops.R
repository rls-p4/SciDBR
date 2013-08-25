# The functions and methods defined below are based closely on native SciDB
# functions, some of which have weak or limited analogs in R. The functions
# defined below work with objects of class scidb (arrays), scidbdf (data
# frames), or scidbexpr (generic scidb query strings). They can be efficiently
# nested by explicitly setting eval=FALSE on inner functions, deferring
# computation until eval=TRUE.

# Filter the attributes of the scidb, scidbdf, or scidbexpr object to contain
# only those specified in expr.
# X:    a scidb, scidbdf, or scidbexpr object
# attributes: a character vector describing the list of attributes to project onto
# eval: a boolean value. If TRUE, the query is executed returning a scidb array.
#       If FALSE, a scidbexpr object describing the query is returned.
`project` = function(X,attributes,eval)
{
  if(missing(`eval`))
  {
# Note: project is implemented as a function in the package. It occupies
# a sole position on the stack reported by sys.nframe.
    nf   = sys.nframe()
    `eval` = !called_from_scidb(nf)
  }
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  query = sprintf("project(%s,%s)", xname,paste(attributes,collapse=","))
  scidbeval(query,eval,lastclass=checkclass(X))
}

# This is the SciDB filter operation, not the R timeseries one.
# X is either a scidb, scidbdf, or scidbexpr object.
# expr is a valid SciDB expression (character)
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a scidbexpr object representing the query.
`filter_scidb` = function(X,expr,eval)
{
  if(missing(`eval`))
  {
# Note the difference here with project above. filter_scidb is implemented
# as a method ('subset') and its position on the stack is in this case three
# levels deep. So we subtract 2 from sys.nframe to get to the first position
# that represents this function.
    nf   = sys.nframe() - 2
    `eval` = !called_from_scidb(nf)
  }
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  query = sprintf("filter(%s,%s)", xname,expr)
  scidbeval(query,eval,lastclass=checkclass(X))
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
  mc = list(...)
  if(is.null(mc$by)) `by`=list()
  else `by`=mc$by
  if(is.null(mc$eval))
  {
    nf   = sys.nframe() - 2
    `eval` = !called_from_scidb(nf)
  } else `eval`=mc$eval
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
  scidbeval(query,eval,lastclass=checkclass(X))
}


`aggregate_scidb` = function(x,by,FUN,eval=TRUE)
{
browser()
  if("scidbexpr" %in% class(x)) x = scidb_from_scidbexpr(x)
  b = `by`
  if(is.list(b)) b = b[[1]]
  if(class(b) %in% c("scidb","scidbdf"))
  {
# We are grouping by attributes in another SciDB array `by`. We assume that
# x and by have conformable dimensions to join along.
    j = intersect(x@D$name, b@D$name)
    X = merge(x,b,by=list(j,j),eval=FALSE)
    x = scidb_from_scidbexpr(X)
    n = by@attributes
    by = list(n)
  }

# XXX A bug in SciDB 13.6 unpack prevents us from using eval=FALSE for now.
  if(!eval) stop("eval=FALSE not yet supported by aggregate due to a bug in SciDB 13.6")

  b = `by`
  new_dim_name = make.names_(c(unlist(b),"row"))
  new_dim_name = new_dim_name[length(new_dim_name)]
  if(!all(b %in% c(x@attributes, x@D$name))) stop("Invalid attribute or dimension name in by")
  a = x@attributes %in% b
  query = x@name
# Handle group by attributes with redimension. We don't use a redimension
# aggregate, however, because some of the other group by variables may
# already be dimensions.
  if(any(a))
  {
# First, we check to see if any of the attributes are not int64. In such cases,
# we use index_lookup to create a factorized version of the attribute to group
# by in place of the original specified attribute. This creates a new virtual
# array x with additional attributes.
    types = x@attributes[a]
    nonint = types != "int64"
    if(any(nonint))
    {
# We assume attributes are int64 here. Add support for sort/unique/index_lookup.
# XXX XXX
    }

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
    query = sprintf("redimension(substitute(%s,build(<_i_:int64>[_j_=0:0,1,0],-1)),%s%s)",x@name,S,D)
  }
  along = paste(b,collapse=",")
# XXX
# We use unpack to always return a data frame (a 1D scidb array)
# OK, as of SciDB 13.6 unpack has a bug that prevents it from working often. Saving
# to a temporary array first seems to be a workaround for this problem. This sucks.
#  query = sprintf("unpack(aggregate(%s, %s, %s),%s)",query, FUN, along, new_dim_name)
  query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  temp = scidbeval(query,TRUE)
  query = scidbexpr(sprintf("unpack(%s,%s)",temp@name,new_dim_name), lastclass="scidbdf")
# XXX
  scidbeval(query,eval)
}

`index_lookup` = function(X, I, attr, new_attr=paste(attr,"index",sep="_"), eval)
{
  if(missing(`eval`))
  {
    nf   = sys.nframe()
    `eval` = !called_from_scidb(nf)
  }
  xname = X
  if(class(X) %in% c("scidb","scidbdf")) xname=X@name
  iname = I
  if(class(I) %in% c("scidb","scidbdf")) iname=I@name
  query = sprintf("index_lookup(%s as __cazart__, %s, __cazart__.%s, %s)",xname, iname, attr, new_attr)
  scidbeval(query,eval,lastclass=checkclass(X))
}

# Sort of like cbind for data frames.
`bind` = function(X, name, FUN, eval)
{
  if(missing(`eval`))
  {
    nf   = sys.nframe()
    `eval` = !called_from_scidb(nf)
  }
  aname = X
  if(class(X) %in% c("scidb","scidbdf")) aname=X@name
  if(length(name)!=length(FUN)) stop("name and FUN must be character vectors of identical length")
  expr = paste(paste(name,FUN,sep=","),collapse=",")
  query = sprintf("apply(%s, %s)",aname, expr)
  scidbeval(query,eval,lastclass=checkclass(X))
}

`unique_scidb` = function(x, incomparables=FALSE,...)
{
  nf   = sys.nframe()  - 2
  `eval` = !called_from_scidb(nf)
  mc = list(...)
  `eval` = ifelse(is.null(mc$eval), `eval`, mc$eval)
  if(incomparables!=FALSE) warning("The incomparables option is not available yet.")
  xname = x
  if(class(x) %in% c("scidbdf","scidb")) xname = x@name
  query = sprintf("uniq(%s)",xname)
  scidbeval(query,eval,lastclass=checkclass(x))
}

`sort_scidb` = function(X, decreasing = FALSE, ...)
{
  nf   = sys.nframe() - 2  # XXX Note! sort is a method and is on a deeper stack.
  `eval` = !called_from_scidb(nf)
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
  `eval` = ifelse(is.null(mc$eval), `eval`, mc$eval)
  a = paste(paste(mc$attributes, dflag, sep=" "),collapse=",")
  if(!is.null(mc$chunk_size)) a = paste(a, mc$chunk_size, sep=",")

  query = sprintf("sort(%s,%s)", xname,a)
  scidbeval(query,eval,lastclass=checkclass(X))
}

# S3 methods
`merge.scidb` = function(x,y,...) merge_scidb(x,y,...)
`merge.scidbdf` = function(x,y,...) merge_scidb(x,y,...)
`merge.scidbexpr` = function(x,y,...) merge_scidb(x,y,...)
`sort.scidb` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`sort.scidbdf` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`sort.scidbexpr` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`unique.scidb` = function(x,incomparables=FALSE,...) unique_scidb(x,incomparables,...)
`unique.scidbdf` = function(x,incomparables=FALSE,...) unique_scidb(x,incomparables,...)
`unique.scidbexpr` = function(x,incomparables=FALSE,...) unique_scidb(x,incomparables,...)
`subset.scidb` = function(x,subset,...) filter_scidb(x,expr=subset,...)
`subset.scidbdf` = function(x,subset,...) filter_scidb(x,expr=subset,...)
`subset.scidbexpr` = function(x,subset,...) filter_scidb(x,expr=subset,...)
