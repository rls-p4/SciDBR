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

# Array redimension, repart, reshape operations.

reshape_scidb = function(data, schema, shape, dimnames, start, chunks, `eval`=FALSE)
{
  if(!missing(schema))
  {
    if(is.scidb(schema)||is.scidbdf(schema)) schema=schema(schema)
    query = sprintf("reshape(%s,%s)",data@name,schema)
    return(.scidbeval(query,eval,depend=list(data)))
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
  D = build_dim_schema(data, newstart=start, newnames=dimnames, newend=shape, newchunk=chunks)
  query = sprintf("reshape(%s,%s%s)",data@name,build_attr_schema(data),D)
  .scidbeval(query,eval,depend=list(data))
}

repart = function(x, schema, upper, chunk, overlap, `eval`=FALSE)
{
  if(!missing(schema))
  {
    query = sprintf("repart(%s, %s)", x@name, schema)
    return(.scidbeval(query,eval,depend=list(x)))
  }
  if(missing(upper)) upper = scidb_coordinate_end(x)
  if(missing(chunk)) chunk = scidb_coordinate_chunksize(x)
  if(missing(overlap)) overlap = scidb_coordinate_overlap(x)
  a = build_attr_schema(x)
  schema = sprintf("%s%s", a, build_dim_schema(x,newend=upper,newchunk=chunk,newoverlap=overlap))
  query = sprintf("repart(%s, %s)", x@name, schema)
  .scidbeval(query,eval,depend=list(x))
}

# SciDB redimension wrapper
#
# Either supply schema or dim. dim is a list of new dimensions made up from the
# attributes and existing dimensions. FUN is a limited scidb aggregation
# expression.
redimension = function(x, schema, dim, FUN, `eval`=FALSE)
{
  if(!(class(x) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
# NB SciDB NULL is not allowed along a coordinate axis prior to SciDB 12.11,
# which could lead to a run time error here.
  if(missing(schema)) schema = NULL
  if(missing(dim)) dim = NULL
  s = schema
  if(is.null(s) && is.null(dim) ||
    (!is.null(s) && !is.null(dim)))
  {
    stop("Exactly one of schema or dim must be specified")
  }
  if((class(s) %in% c("scidb","scidbdf"))) s = schema(s)
  if(!is.null(dim))
  {
    d = unlist(dim)
    ia = which(scidb_attributes(x) %in% d)
    id = which(dimensions(x) %in% d)
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
# We'll be converting attributes to dimensions here.
# First, we make sure that they are all int64. If not, we add a new
# auxiliary attribute with index_lookup and dimension along that instead.
      reindexed = FALSE
      xold = x
      for(nid in x@attributes[ia])
      {
        idx = which(x@attributes %in% nid)
        if(scidb_types(x)[idx] != "int64")
        {
          reindexed = TRUE
          newat = sprintf("%s_index",nid)
          newat = make.unique_(x@attributes, newat)
          x = index_lookup(x, unique(xold[,nid]), nid, newat)
          d[d %in% nid] = newat
        }
      }
      if(reindexed)
      {
        ia = which(x@attributes %in% d)
        as = build_attr_schema(x, I=-ia)
      }

# Add the new dimension(s)
      a = x@attributes[ia]
      x@attributes = x@attributes[-ia]
      f = paste(paste("min(",a,"), max(",a,")",sep=""),collapse=",")
      m = matrix(aggregate(x, FUN=f, unpack=FALSE)[],ncol=2,byrow=TRUE)
      p = prod(as.numeric(scidb_coordinate_chunksize(x)[-id]))
      chunk = ceiling((1e6/p)^(1/length(ia)))
      new = apply(m,1,paste,collapse=":")
      new = paste(a,new,sep="=")
      new = paste(new, noE(chunk), "0", sep=",")
      new = paste(new,collapse=",")
      ds = ifelse(length(ds)>0,paste(ds,new,sep=","),new)
    }
    s = sprintf("%s[%s]",as,ds)
  }
# Check to see if the new schema is identical to the original schema.
# If so, don't bother with redimension, and return the input
  if(isTRUE(compare_schema(x,s)))
  {
    return(x)
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

# Apply a heuristic method bring the schema of x and y into reasonable
# conformance. The heuristic favors name matching, then resorts to
# positional matching. Partial matches are allowed.
conform = function(x, y, dimension.only=TRUE)
{
  if(!dimension.only) stop("Sorry, not yet supported")
  if((class(x) %in% c("scidb","scidbdf"))) stop("x must be a scidb or scidbdf object")
  if(!is.character(x)) stop("Invalid x")
  if(!is.character(y)) stop("Invalid y")

# Find attibute and dimension names in common
  ai = intersect(scidb_attributes(x), scidb_attributes(y))
  ad = intersect(dimensions(x), dimensions(y))

# First work on bringing dimensions into agreement...
  if(length(ad)>0) # by name
  {
    adx = which(dimensions(x) %in% ad)
    ady = which(dimensions(y) %in% ad)
    s   = lapply(1:length(dim(x)), function(i)
            {
              if(i %in% adx) build_dim_schema(y,I=ady[i],bracket=FALSE)
              else build_dim_schema(x,I=i,bracket=FALSE)
            })
  } else # positional
  {
    s   = lapply(1:length(dim(x)), function(i)
            {
              if(i < length(dim(y))) build_dim_schema(y,I=i,bracket=FALSE)
              else build_dim_schema(x,I=i,bracket=FALSE)
            })
  }
  s1 = sprintf("%s[%s]",build_attr_schema(x),paste(s,collapse=","))
  s1b = lapply(scidb_coordinate_bounds(s1), as.numeric)
  xb = lapply(scidb_coordinate_bounds(x), as.numeric)
  s1b = lapply(s1b, function(x) {a=x;a[is.na(x)]=Inf;a})
  xb = lapply(xb, function(x) {a=x;a[is.na(x)]=Inf;a})
# Check for no  op.
  if(isTRUE(compare_schema(x,s1)))
  {
    return(x)
  }
# Check to see if the new dimension bounds lie strictly within the old ones.
# If so, we use reshape instead of redimension.
  strict = all(s1b$start >= xb$start) && all(s1b$end <= xb$end)
  if(strict)
  {
    query = sprintf("reshape(between(%s, %s), %s)", x@name,
              between_coordinate_bounds(s1), s1)
    return(.scidbeval(query, eval=FALSE))
  }
# XXX ... still working on this...
}
