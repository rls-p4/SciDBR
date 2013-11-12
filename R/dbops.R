# The functions and methods defined below are based closely on native SciDB
# functions, some of which have weak or limited analogs in R. The functions
# defined below work with objects of class scidb (arrays), scidbdf (data
# frames). They can be efficiently nested by explicitly setting eval=FALSE on
# inner functions, deferring computation until eval=TRUE.


# SciDB redimension wrapper
redimension = function(x, s, eval)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  if(missing(`eval`))
  {
    nf   = sys.nframe()
    `eval` = !called_from_scidb(nf)
  }
  sc = scidb_from_schemastring(s)
  query = paste("substitute(",x@name,",build(<___i:int64>[___j=0:0,1,0],0),",paste(sc@D$name,collapse=","),")",sep="")
  query = sprintf("redimension(%s,%s)",query,s)
  scidbeval(query,eval)
}

# SciDB build wrapper, intended to act something like the R 'array' function.
build = function(data, dim, names, type="double",
                 start, name, chunksize, overlap, gc=TRUE, eval)
{
  if(missing(`eval`))
  {
    nf   = sys.nframe()
    `eval` = !called_from_scidb(nf)
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
  start = sprintf("%.0f",start)
  chunksize = sprintf("%.0f",chunksize)
  overlap = sprintf("%.0f",overlap)
  dim = sprintf("%.0f", dim-1)
  schema = paste("<",names[1],":",type,">",sep="")
  schema = paste(schema, paste("[",paste(paste(paste(
        paste(names[-1],start,sep="="), dim, sep=":"),
        chunksize, overlap, sep=","), collapse=","),"]",sep=""), sep="")
  query = sprintf("build(%s,%s)",schema,data)
  if(missing(name)) return(scidbeval(query,eval))
  scidbeval(query,eval,name)
}

# Count the number of non-empty cells
`count` = function(x)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  iquery(sprintf("count(%s)",x@name),return=TRUE)$count
}

# The new (SciDB 13.9) cumulate
`cumulate` = function(x, expression, dimension, eval)
{
  if(missing(`eval`))
  {
    nf   = sys.nframe()
    `eval` = !called_from_scidb(nf)
  }
  if(missing(dimension)) dimension = x@D$name[[1]]
  query = sprintf("cumulate(%s, %s, %s)",x@name,expression,dimension)
  scidbeval(query,eval)
}

# Filter the attributes of the scidb, scidbdf object to contain
# only those specified in expr.
# X:    a scidb, scidbdf object
# attributes: a character vector describing the list of attributes to project onto
# eval: a boolean value. If TRUE, the query is executed returning a scidb array.
#       If FALSE, a promise object describing the query is returned.
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
  scidbeval(query,eval)
}

# This is the SciDB filter operation, not the R timeseries one.
# X is either a scidb, scidbdf object.
# expr is a valid SciDB expression (character)
# eval=TRUE means run the query and return a scidb object.
# eval=FALSE means return a promise object representing the query.
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
  scidbeval(query,eval)
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
    stop("by must be either a single string describing a dimension to join on, or a list of attributes or dimensions in the form list(c('arrayX_dim1','arrayX_dim2',...),c('arrayY_dim1','arrayY_dim2',...))")
  if(length(`by`)>0)
  {
    b = unlist(lapply(1:length(`by`[[1]]), function(j) unlist(lapply(`by`, function(x) x[[j]]))))
    if(any(X@attributes %in% b) && length(b)==2)
    {
# Special case, join on attributes. Right now limited to one attribute per
# array cause I am lazy and this is incredibly complicated.
      XI = index_lookup(X,unique(sort(project(X,`by`[[1]]),attributes=`by`[[1]],decreasing=FALSE),sort=FALSE),`by`[[1]], eval=FALSE)
      YI = index_lookup(Y,unique(sort(project(X,`by`[[1]]),attributes=`by`[[1]],decreasing=FALSE),sort=FALSE),`by`[[2]], eval=FALSE)

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
  scidbeval(query,eval)
}


# x:   A scidb, scidbdf object
# by:  A character vector of dimension and or attribute names of x, or,
#      a scidb or scidbdf object that will be cross_joined to x and then
#      grouped by attribues of by.
# FUN: A SciDB aggregation expresion
`aggregate_scidb` = function(x,by,FUN,eval)
{
  b = `by`
  if(is.list(b)) b = b[[1]]
  if(class(b) %in% c("scidb","scidbdf"))
  {
# We are grouping by attributes in another SciDB array `by`. We assume that
# x and by have conformable dimensions to join along.
    j = intersect(x@D$name, b@D$name)
    X = merge(x,b,by=list(j,j),eval=FALSE)
    n = by@attributes
    by = list(n)
  }

  if(missing(`eval`))
  {
    nf   = sys.nframe() - 2  # Note! this is a method and is on a deeper stack.
    `eval` = !called_from_scidb(nf)
  }
# A bug in SciDB 13.6 unpack prevents us from using eval=FALSE for now.
  if(!eval && !compare_versions(options("scidb.version")[[1]],13.9)) stop("eval=FALSE not yet supported by aggregate due to a bug in SciDB <= 13.6")

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
    nonint = x@types != "int64" & a
    if(any(nonint))
    {
# Use index_lookup to factorize non-integer indices, creating new enumerated
# attributes to sort by. It's probably not a great idea to have too many.
      idx = which(nonint)
      oldatr = x@attributes
      for(j in idx)
      {
        atr     = oldatr[j]
# Adjust the FUN expression to include the original attribute
        FUN = sprintf("%s, min(%s) as %s", FUN, atr, atr)
# Factorize atr
        x       = index_lookup(x,unique(sort(project(x,atr)),sort=FALSE),atr)
# Name the new attribute and sort by it instead of originally specified one.
        newname = paste(atr,"index",sep="_")
        newname = make.unique_(oldatr,newname)
        b[which(b==atr)] = newname
      }
# XXX XXX length(idx) > 1???
    }

# Reset in case things changed above
    a = x@attributes %in% b
    n = x@attributes[a]
# XXX What about chunk sizes? NULLs? Ugh. Also insert reasonable upper bound instead of *? XXX Take care of all these issues...
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

# We use unpack to always return a data frame (a 1D scidb array). As of
# SciDB 13.6 unpack has a bug that prevents it from working often. Saving to a
# temporary array first seems to be a workaround for this problem. This sucks.
  query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  if(!compare_versions(options("scidb.version")[[1]],13.9))
  {
    temp = scidbeval(query,TRUE)
    query = sprintf("unpack(%s,%s)",temp@name,new_dim_name)
  } else
  {
    query = sprintf("unpack(%s,%s)",query,new_dim_name)
  }
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
  scidbeval(query,eval)
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
  scidbeval(query,eval)
}

`unique_scidb` = function(x, incomparables=FALSE, sort=TRUE, ...)
{
  nf   = sys.nframe()  - 2
  `eval` = !called_from_scidb(nf)
  mc = list(...)
  `eval` = ifelse(is.null(mc$eval), `eval`, mc$eval)
  if(incomparables!=FALSE) warning("The incomparables option is not available yet.")
  xname = x@name
  if(sort)
  {
    query = sprintf("uniq(sort(project(%s,%s),%s))",xname,x@attributes[[1]],x@attributes[[1]])
  } else
  {
    query = sprintf("uniq(%s)",xname)
  }
  scidbeval(query,eval)
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
  if(is.null(mc$attributes))
  {
    if(length(EX@attributes)>1) stop("Array contains more than one attribute. Specify one or more attributes to sort on with the attributes= function argument")
    mc$attributes=EX@attributes
  }
  `eval` = ifelse(is.null(mc$eval), `eval`, mc$eval)
  a = paste(paste(mc$attributes, dflag, sep=" "),collapse=",")
  if(!is.null(mc$chunk_size)) a = paste(a, mc$chunk_size, sep=",")

  query = sprintf("sort(%s,%s)", xname,a)
  scidbeval(query,eval)
}

# S3 methods
`merge.scidb` = function(x,y,...) merge_scidb(x,y,...)
`merge.scidbdf` = function(x,y,...) merge_scidb(x,y,...)
`sort.scidb` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`sort.scidbdf` = function(x,decreasing=FALSE,...) sort_scidb(x,decreasing,...)
`unique.scidb` = function(x,incomparables=FALSE,sort=TRUE,...) unique_scidb(x,incomparables,sort,...)
`unique.scidbdf` = function(x,incomparables=FALSE,sort=TRUE,...) unique_scidb(x,incomparables,sort,...)
`subset.scidb` = function(x,subset,...) filter_scidb(x,expr=subset,...)
`subset.scidbdf` = function(x,subset,...) filter_scidb(x,expr=subset,...)
