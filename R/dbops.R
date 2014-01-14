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

`unpack_scidb` = function(x, `eval`=FALSE)
{
  dimname = make.unique_(c(x@D$name,x@attributes), "i")
  query = sprintf("unpack(%s, %s)",x@name, dimname)
  .scidbeval(query,eval,depend=list(x))
}

`attribute_rename` = function(x, old, `new`, `eval`=FALSE)
{
  query = sprintf("attribute_rename(%s,%s,%s)",x@name,old,new)
  .scidbeval(query,eval,depend=list(x))
}

`slice` = function(x, d, n, `eval`=FALSE)
{
  query = sprintf("slice(%s, %s, %d)", x@name, d, n)
  .scidbeval(query, `eval`, depend=list(x))
}

`substitute` = function(x, value, `attribute`, `eval`=FALSE)
{
  if(missing(attribute)) attribute = x@attribute
  if(missing(value)) value = "build(<v:double>[i=0:0,1,0],nan)"
  query = sprintf("substitute(%s,%s,%s)",x@name, value, attribute)
  .scidbeval(query, `eval`, depend=list(x))
}

`cast` = function(x, s, `eval`=FALSE)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
# Default cast strips "Not nullable" array property
  if(missing(s)) s = scidb:::extract_schema(scidb:::scidb_from_schemastring(x@schema))
  query = sprintf("cast(%s,%s)",x@name,s)
  .scidbeval(query,eval,depend=list(x))
}

`repart` = function(x, upper, chunk, overlap, `eval`=FALSE)
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

`redimension` = function(x, s, `eval`=FALSE)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
# NB SciDB NULL is not allowed along a coordinate axis prior to SciDB 12.11,
# which could lead to a run time error here.
  query = sprintf("redimension(%s,%s)",x@name,s)
  .scidbeval(query,eval,depend=list(x))
}

# SciDB build wrapper, intended to act something like the R 'array' function.
`build` = function(data, dim, names, type="double",
                 start, name, chunksize, overlap, gc=TRUE, `eval`=FALSE)
{
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
  start = sprintf("%.0f",start)
  chunksize = sprintf("%.0f",chunksize)
  overlap = sprintf("%.0f",overlap)
  dim = sprintf("%.0f", dim-1)
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
`count` = function(x)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  iquery(sprintf("count(%s)",x@name),return=TRUE)$count
}

# The new (SciDB 13.9) cumulate
`cumulate` = function(x, expression, dimension, `eval`=FALSE)
{
  if(missing(dimension)) dimension = x@D$name[[1]]
  query = sprintf("cumulate(%s, %s, %s)",x@name,expression,dimension)
  .scidbeval(query,eval,depend=list(x))
}

# Filter the attributes of the scidb, scidbdf object to contain
# only those specified in expr.
# X:    a scidb, scidbdf object
# attributes: a character vector describing the list of attributes to project onto
# eval: a boolean value. If TRUE, the query is executed returning a scidb array.
#       If FALSE, a promise object describing the query is returned.
`project` = function(X,attributes,`eval`=FALSE)
{
  xname = X
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

# SciDB cross_join wrapper internal function to support merge on various
# classes (scidb, scidbdf). This is an internal function to support
# merge on various SciDB objects.
# X and Y are SciDB array references of any kind (scidb, scidbdf)
# by is either a single character indicating a dimension name common to both
# arrays to join on, or a two-element list of character vectors of array
# dimensions to join on.
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a promise object representing the query.
# Examples:
# merge(X,Y,by='i')
# merge(X,Y,by=list('i','i'))  # equivalent to last expression
# merge(X,Y,by=list(X=c('i','j'), Y=c('k','l')))
`merge_scidb` = function(X,Y,...)
{
  mc = list(...)
  if(is.null(mc$by)) `by`=list()
  else `by`=mc$by
  `eval` = ifelse(is.null(mc$eval), FALSE, mc$eval)
  xname = X@name
  yname = Y@name


  query = sprintf("cross_join(%s as __X, %s as __Y", xname, yname)
  if(length(`by`)>1 && !is.list(`by`))
    stop("by must be either a single string describing a dimension to join on, or a list of attributes or dimensions in the form list(c('arrayX_dim1','arrayX_dim2',...),c('arrayY_dim1','arrayY_dim2',...))")
  if(length(`by`)>0)
  {
    b = unlist(lapply(1:length(`by`[[1]]), function(j) unlist(lapply(`by`, function(x) x[[j]]))))
    if(any(X@attributes %in% b) && length(b)==2)
    {
# Special case, join on attributes. Right now limited to one attribute per
# array cause I am lazy and this is incredibly complicated.
      XI = index_lookup(X,unique(sort(project(X,`by`[[1]]),attributes=`by`[[1]],decreasing=FALSE),sort=FALSE),`by`[[1]], `eval`=FALSE)
      YI = index_lookup(Y,unique(sort(project(X,`by`[[1]]),attributes=`by`[[1]],decreasing=FALSE),sort=FALSE),`by`[[2]], `eval`=FALSE)

# Note! Limited to inner-join for now.
      new_dim_name = make.names_(c(X@D$name,Y@D$name,"row"))
      new_dim_name = new_dim_name[length(new_dim_name)]
      a = XI@attributes %in% paste(b,"index",sep="_")
      n = XI@attributes[a]
      redim = paste(paste(n,"=-1:*,100000,0",sep=""), collapse=",")
      S = scidb:::build_attr_schema(X)
      D = sprintf("[%s,%s]",redim,scidb:::build_dim_schema(X,bracket=FALSE))
      q1 = sprintf("redimension(substitute(%s,build(<_i_:int64>[_j_=0:0,1,0],-1),%s),%s%s)",XI@name,n,S,D)

      a = YI@attributes %in% paste(b,"index",sep="_")
      n = YI@attributes[a]
      redim = paste(paste(n,"=-1:*,100000,0",sep=""), collapse=",")
      S = scidb:::build_attr_schema(Y)
      D = sprintf("[%s,%s]",redim,scidb:::build_dim_schema(Y,bracket=FALSE))
      D2 = sprintf("[%s,_%s]",redim,scidb:::build_dim_schema(Y,bracket=FALSE))
      q2 = sprintf("cast(redimension(substitute(%s,build(<_i_:int64>[_j_=0:0,1,0],-1),%s),%s%s),%s%s)",YI@name,n,S,D,S,D2)

      query = sprintf("unpack(cross_join(%s as _cazart, %s as _yikes, _cazart.%s_index, _yikes.%s_index),%s)",q1,q2,by[[1]],by[[2]],new_dim_name)

    } else
    {
# Join on dimensions.
# Re-order list terms
      b = as.list(unlist(lapply(1:length(`by`[[1]]), function(j) unlist(lapply(`by`, function(x) x[[j]])))))
      cterms = paste(c("__X","__Y"), b, sep=".")
      cterms = paste(cterms,collapse=",")
      query  = paste(query,",",cterms,")",sep="")
    }
  } else
  {
    query  = sprintf("%s)",query)
  }
  .scidbeval(query,eval,depend=list(X,Y))
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
`bind` = function(X, name, FUN, `eval`=FALSE)
{
  aname = X
  if(class(X) %in% c("scidb","scidbdf")) aname=X@name
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
  xname = x@name
  if(sort)
  {
    if(length(x@attributes)>1)
      query = sprintf("uniq(sort(project(%s,%s)))",xname,x@attributes[[1]])
    else
      query = sprintf("uniq(sort(%s))",xname,x@attributes[[1]])
  } else
  {
    query = sprintf("uniq(%s)",xname)
  }
  .scidbeval(query,eval,depend=list(x),`data.frame`=TRUE)
}

`sort_scidb` = function(X, decreasing = FALSE, ...)
{
  nf   = sys.nframe() - 2  # XXX Note! sort is a method and is on a deeper stack.
  mc = list(...)
  if(!is.null(mc$na.last))
    warning("na.last option not supported by SciDB sort. Missing values are treated as less than other values by SciDB sort.")
  dflag = ifelse(decreasing, 'desc', 'asc')
  xname = X
  if(class(X) %in% c("scidbdf","scidb")) xname = X@name
  EX = X
  if(is.null(mc$attributes))
  {
    if(length(EX@attributes)>1) stop("Array contains more than one attribute. Specify one or more attributes to sort on with the attributes= function argument")
    mc$attributes=EX@attributes
  }
  `eval` = ifelse(is.null(mc$eval), FALSE, mc$eval)
  a = paste(paste(mc$attributes, dflag, sep=" "),collapse=",")
  if(!is.null(mc$chunk_size)) a = paste(a, mc$chunk_size, sep=",")

  query = sprintf("sort(%s,%s)", xname,a)
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
