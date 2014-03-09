#
#    _____      _ ____  ____
#   / ___/_____(_) __ \/ __ )
#   \__ \/ ___/ / / / / __  |
#  ___/ / /__/ / /_/ / /_/ / 
# /____/\___/_/_____/_____/  
#
#
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

# The functions and methods defined below are based closely on native SciDB
# functions, some of which have weak or limited analogs in R. The functions
# defined below work with objects of class scidb (arrays), scidbdf (data
# frames). They can be efficiently nested by explicitly setting eval=FALSE on
# inner functions, deferring computation until eval=TRUE.
`reshape_scidb` = function(data, shape, dimnames, chunks, `eval`=FALSE)
{
  if(missing(shape)) stop("Missing dimension shape")
  N = length(shape)
  if(missing(dimnames))
  {
    dimnames=letters[9:(9+N-1)]
  }
  if(missing(chunks))
  {
    chunks = rep(1000,N)
  }
  D = paste(paste(dimnames,"=",0,":",shape-1,",",chunks,",0",sep=""),collapse=",")
  D = sprintf("[%s]",D)
  query = sprintf("reshape(%s,%s%s)",data@name,build_attr_schema(data),D)
  .scidbeval(query,eval,depend=list(data))
}

# SciDB rename wrapper
# Note! that the default garbage collection option here is to *not* remove.
rename = function(A, name=A@name, gc)
{
  if(!(inherits(A,"scidb") || inherits(A,"scidbdf"))) stop("`A` must be a scidb object.")
  if(missing(gc)) gc = FALSE
  if(A@name != name) scidbquery(sprintf("rename(%s,%s)",A@name, name))
  A@gc$remove=gc
  A@name = name
  A@gc$name = name
  A
}

`unpack_scidb` = function(x, `eval`=FALSE)
{
  dimname = make.unique_(c(x@D$name,x@attributes), "i")
  query = sprintf("unpack(%s, %s)",x@name, dimname)
  .scidbeval(query,eval,depend=list(x))
}

`attribute_rename` = function(x, old, `new`, `eval`=FALSE)
{
  query = sprintf("attribute_rename(%s,%s)",x@name,
    paste(paste(old,new,sep=","),collapse=","))
  .scidbeval(query,eval,depend=list(x))
}

`dimension_rename` = function(x, old, `new`, `eval`=FALSE)
{
  if(!(is.scidb(x) || is.scidbdf(x))) stop("Requires a scidb or scidbdf object")
  dnames = x@D$name
  idx = which(dnames %in% old)
  dnames[idx] = `new`
  if(length(idx)!=1) stop("Invalid old dimension name specified")
  query = sprintf("cast(%s, %s%s)", x@name, build_attr_schema(x),
             build_dim_schema(x, newnames=dnames))
  .scidbeval(query,eval,depend=list(x))
}

slice = function(x, d, n, `eval`=FALSE)
{
  if(!(is.scidb(x) || is.scidbdf(x))) stop("Requires a scidb or scidbdf object")
  N = length(x@D$name)
  limits = rep("null",N)
  i = d
  if(is.character(d))
  {
    i = which(x@D$name %in% d)
  }
  if(length(i)==0 || i>N)
  {
    stop("Invalid dimension specified")
  }
  limits[i] = noE(n)
  query = sprintf("slice(%s, %s)",x@name,paste(paste(d,n,sep=","),collapse=","))
  .scidbeval(query,eval,depend=list(x))
}

substitute = function(x, value, `attribute`, `eval`=FALSE)
{
  if(!(is.scidb(x) || is.scidbdf(x))) stop("Requires a scidb or scidbdf object")
  if(!any(x@nullable)) return(x)
  if(missing(attribute))
  {
    attribute = ""
  }
  if(missing(value))
    value = sprintf("build(%s[i=0:0,1,0],i)",build_attr_schema(x,I=1))
  if(nchar(attribute)<1)
    query = sprintf("substitute(%s,%s)",x@name, value)
  else
    query = sprintf("substitute(%s,%s,%s)",x@name, value, attribute)
  .scidbeval(query, `eval`, depend=list(x))
}

subarray = function(x, limits, schema, `eval`=FALSE)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  if(missing(limits)) limits=paste(rep("null",2*length(x@D$name)),collapse=",")
  if(!missing(schema))
  {
    if(!is.character(schema)) schema = schema(schema)
    limits = paste(coordinate_bounds(schema),collapse=",")
  }
  query = sprintf("subarray(%s,%s)",x@name,limits)
  .scidbeval(query, `eval`, depend=list(x))
}

cast = function(x, s, `eval`=FALSE)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
# Default cast strips "Not nullable" array property
  if(missing(s)) s = extract_schema(scidb_from_schemastring(x@schema))
  query = sprintf("cast(%s,%s)",x@name,s)
  .scidbeval(query,eval,depend=list(x))
}

repart = function(x, upper, chunk, overlap, `eval`=FALSE)
{
  a = build_attr_schema(x)
  if(missing(upper)) upper = x@D$start + x@D$length - 1
  if(missing(chunk)) chunk = x@D$chunk_interval
  if(missing(overlap)) overlap = x@D$chunk_overlap
  y = x
  y@D$length = upper - y@D$start + 1
  y@D$chunk_interval = chunk
  y@D$chunk_overlap = overlap
  d = build_dim_schema(y)
  query = sprintf("repart(%s, %s%s)", x@name, a, d)
  .scidbeval(query,eval,depend=list(x))
}

# SciDB redimension wrapper
# Either supply s or dim. dim is a list of new dimensions made up
# from the attributes and existing dimensions. Reduce is a scidb
# aggregation expression.
redimension = function(x, s, dim, FUN, `eval`=FALSE)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
# NB SciDB NULL is not allowed along a coordinate axis prior to SciDB 12.11,
# which could lead to a run time error here.
  if(missing(s)) s = NULL
  if(missing(dim)) dim = NULL
  if(is.null(s) && is.null(dim) ||
    (!is.null(s) && !is.null(dim)))
  {
    stop("Exactly one of s or dim must be specified")
  }
  if((class(s) %in% c("scidb","scidbdf"))) s = schema(s)
  if(!is.null(dim))
  {
    d = unlist(dim)
    ia = which(x@attributes %in% d)
    id = which(x@D$name %in% d)
    if(length(ia)<1 && length(id)<1) stop("Invalid dimensions")
    as = build_attr_schema(x, I=-ia)
    if(length(id>0))
    {
      ds = build_dim_schema(x, I=id, bracket=FALSE)
    } else
    {
      ds = c()
    }
    if(length(ia)>0)
    {
# Add the new dimension(s)
      a = x@attributes[ia]
      x@attributes = x@attributes[-ia]
      f = paste(paste("min(",a,"), max(",a,")",sep=""),collapse=",")
      m = matrix(aggregate(x, FUN=f, unpack=FALSE)[],ncol=2,byrow=TRUE)
      p = prod(x@D$chunk_interval)
      chunk = ceiling((1e6/p)^(1/length(ia)))
      new = apply(m,1,paste,collapse=":")
      new = paste(a,new,sep="=")
      new = paste(new, chunk, "0", sep=",")
      new = paste(new,collapse=",")
      ds = ifelse(length(ds)>0,paste(ds,new,sep=","),new)
    }
    s = sprintf("%s[%s]",as,ds)
  }
  if(!missing(FUN))
  {
    if(!is.function(FUN)) stop("`FUN` must be a function")
    fn = .scidbfun(FUN)
    if(is.null(fn))
      stop("`FUN` requires an aggregate function")
    reduce = paste(sprintf("%s(%s) as %s",fn,x@attributes,x@attributes),
               collapse=",")
    s = sprintf("%s,%s", s, reduce)
  }
  query = sprintf("redimension(%s,%s)",x@name,s)
  .scidbeval(query,eval,depend=list(x))
}

# SciDB build wrapper, intended to act something like the R 'array' function.
build = function(data, dim, names, type="double",
                 start, name, chunksize, overlap, gc=TRUE, `eval`=FALSE)
{
# Special case:
  if(is.scidb(dim) || is.scidbdf(dim))
  {
    schema = sprintf("%s%s",build_attr_schema(dim,I=1),build_dim_schema(dim))
    query = sprintf("build(%s,%s)",schema,data)
    ans = .scidbeval(query,eval)
# We know that the output of build is not sparse
    attr(ans,"sparse") = FALSE
    return(ans)
  }
  if(missing(start)) start = rep(0,length(dim))
  if(missing(overlap)) overlap = rep(0,length(dim))
  if(missing(chunksize))
  {
    chunksize = rep(ceiling(1e6^(1/length(dim))),length(dim))
  }
  if(length(start)!=length(dim)) stop("Mismatched dimension/start lengths")
  if(length(chunksize)!=length(dim)) stop("Mismatched dimension/chunksize lengths")
  if(length(overlap)!=length(dim)) stop("Mismatched dimension/overlap lengths")
  if(missing(names))
  {
    names = c("val", letters[9:(8+length(dim))])
  }
# No scientific notation please
  chunksize = noE(chunksize)
  overlap = noE(overlap)
  dim = noE(dim + (start - 1))
  start = noE(start)
  schema = paste("<",names[1],":",type,">",sep="")
  schema = paste(schema, paste("[",paste(paste(paste(
        paste(names[-1],start,sep="="), dim, sep=":"),
        chunksize, overlap, sep=","), collapse=","),"]",sep=""), sep="")
  query = sprintf("build(%s,%s)",schema,data)
  if(missing(name)) return(.scidbeval(query,eval))
  ans = .scidbeval(query,eval,name)
# We know that the output of build is not sparse
  attr(ans,"sparse") = FALSE
  ans
}

# Count the number of non-empty cells
count = function(x)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  iquery(sprintf("count(%s)",x@name),return=TRUE)$count
}

# Filter the attributes of the scidb, scidbdf object to contain
# only those specified in expr.
# X:    a scidb, scidbdf object
# attributes: a character vector describing the list of attributes to project onto
# eval: a boolean value. If TRUE, the query is executed returning a scidb array.
#       If FALSE, a promise object describing the query is returned.
project = function(X,attributes,`eval`=FALSE)
{
  xname = X
  if(is.numeric(attributes))
    attributes = X@attributes[attributes]
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  query = sprintf("project(%s,%s)", xname,paste(attributes,collapse=","))
  .scidbeval(query,eval,depend=list(X))
}

# This is the SciDB filter operation, not the R timeseries one.
# X is either a scidb, scidbdf object.
# expr is a valid SciDB expression (character)
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a promise object representing the query.
`filter_scidb` = function(X,expr,`eval`=FALSE)
{
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  query = sprintf("filter(%s,%s)", xname,expr)
  .scidbeval(query,eval,depend=list(X))
}



`index_lookup` = function(X, I, attr, new_attr, `eval`=FALSE)
{
  if(missing(attr)) attr = X@attributes[[1]]
  if(missing(new_attr)) new_attr=paste(attr,"index",sep="_")
  xname = X
  if(class(X) %in% c("scidb","scidbdf")) xname=X@name
  iname = I
  if(class(I) %in% c("scidb","scidbdf")) iname=I@name
  query = sprintf("index_lookup(%s as __cazart__, %s, __cazart__.%s, %s)",xname, iname, attr, new_attr)
  .scidbeval(query,eval,depend=list(X,I))
}

# Sort of like cbind for data frames.
bind = function(X, name, FUN, `eval`=FALSE)
{
  aname = X
  if(class(X) %in% c("scidb","scidbdf")) aname=X@name
# Auto-generate names like X_n:
  if(missing(name))
  {
    name = make.unique_(c(X@attributes,X@D$name), rep("X",length(FUN)))
  }
  if(length(name)!=length(FUN)) stop("name and FUN must be character vectors of identical length")
  expr = paste(paste(name,FUN,sep=","),collapse=",")
  query = sprintf("apply(%s, %s)",aname, expr)
  .scidbeval(query,eval,depend=list(X))
}

`unique_scidb` = function(x, incomparables=FALSE, sort=TRUE, ...)
{
  mc = list(...)
  `eval` = ifelse(is.null(mc$eval), FALSE, mc$eval)
  if(incomparables!=FALSE) warning("The incomparables option is not available yet.")
  if(any(x@attributes %in% "i"))
  {
    new_attrs = x@attributes
    new_attrs = new_attrs[x@attributes %in% "i"] = make.unique_(x@attributes,"i")
    x = attribute_rename(x,x@attributes,new_attrs)
  }
  if(sort)
  {
# XXX XXX There is a problem here if there is an attribute named 'n' (see sort function
# below)...this must be fixed.
    rs = sprintf("%s[n=0:%s,%s,0]",build_attr_schema(x,I=1),.scidb_DIM_MAX,noE(min(1e6,prod(dim(x)))))
    if(length(x@attributes)>1)
    {
      query = sprintf("uniq(redimension(sort(project(%s,%s)),%s))",x@name,x@attributes[[1]],rs)
    }
    else
    {
      query = sprintf("uniq(redimension(sort(%s),%s))",x@name,rs)
    }
  } else
  {
    query = sprintf("uniq(%s)",x@name)
  }
  .scidbeval(query,eval,depend=list(x),`data.frame`=TRUE)
}

`sort_scidb` = function(X, decreasing = FALSE, ...)
{
  mc = list(...)
  if(!is.null(mc$na.last))
    warning("na.last option not supported by SciDB sort. Missing values are treated as less than other values by SciDB sort.")
  dflag = ifelse(decreasing, 'desc', 'asc')
# Check for ridiculous SciDB name conflict problem
  if(any(X@attributes %in% "n"))
  {
    new_attrs = X@attributes
    new_attrs = new_attrs[X@attributes %in% "n"] = make.unique_(X@attributes,"n")
    X = attribute_rename(X,X@attributes,new_attrs)
  }
  if(is.null(mc$attributes))
  {
    if(length(X@attributes)>1) stop("Array contains more than one attribute. Specify one or more attributes to sort on with the attributes= function argument")
    mc$attributes=X@attributes
  }
  `eval` = ifelse(is.null(mc$eval), FALSE, mc$eval)
  a = paste(paste(mc$attributes, dflag, sep=" "),collapse=",")
  if(!is.null(mc$chunk_size)) a = paste(a, mc$chunk_size, sep=",")

  rs = sprintf("%s[n=0:%s,%s,0]",build_attr_schema(X,I=1),.scidb_DIM_MAX,noE(min(1e6,prod(dim(X)))))
  query = sprintf("redimension(sort(%s,%s),%s)", X@name,a,rs)
  .scidbeval(query,eval,depend=list(X))
}

# S3 methods
`merge.scidb` = function(x,y,...) merge_scidb(x,y,...)
`merge.scidbdf` = function(x,y,...) merge_scidb(x,y,...)
`sort.scidb` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`sort.scidbdf` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`unique.scidb` = function(x,incomparables=FALSE,...) unique_scidb(x,incomparables,...)
`unique.scidbdf` = function(x,incomparables=FALSE,...) unique_scidb(x,incomparables,...)
`subset.scidb` = function(x,subset,...) filter_scidb(x,expr=subset,...)
`subset.scidbdf` = function(x,subset,...) filter_scidb(x,expr=subset,...)
