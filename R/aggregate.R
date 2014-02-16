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

# Nifty aggregation-related functions

`sweep_scidb` = function(x, MARGIN, STATS, FUN="-", `eval`=FALSE, `name`)
{
  if(!is.scidb(x)) stop("x must be a scidb object")
  if(!is.scidb(STATS) && !is.scidbdf(STATS)) stop("STATS must be a scidb or scidbdf object")
  if(length(MARGIN)!=1) stop("MARGIN must indicate a single dimension")
  if(length(STATS@D$name)>1) stop("STATS must be a one-dimensional SciDB array")
  if(is.numeric(MARGIN)) MARGIN = x@D$name[MARGIN]
  if(missing(`name`)) `name` = x@attribute
  if(!(MARGIN %in% STATS@D$name))
  {
# Make sure coordinate axis along MARGIN are named the same in each array
    old = sprintf("%s=",STATS@D$name[1])
    new = sprintf("%s=",MARGIN)
    schema = gsub(old,new,STATS@schema)
    query = sprintf("cast(%s,%s)",STATS@name,schema)
    STATS = .scidbeval(query,eval=FALSE, depend=list(x))
  }
# Check for potential attribute name conflicts and adjust.
  if(length(intersect(x@attributes, STATS@attributes))>0)
  {
    STATS = cast(STATS,paste(build_attr_schema(STATS,"V_"),build_dim_schema(STATS)),`eval`=FALSE)
  }
  if(nchar(FUN)==1)
  {
    FUN = sprintf("%s %s %s",x@attribute, FUN, STATS@attribute)
  }
  substitute(
  attribute_rename(
    project(
      bind(
        merge(x,STATS,by=MARGIN,eval=FALSE)
        ,"_sweep",FUN,eval=FALSE),"_sweep",eval=FALSE),
    "_sweep", `name`, eval=FALSE), eval=`eval`)
}

# A very limited version of R's apply.
`apply_scidb` = function(X,MARGIN,FUN,`eval`=FALSE,`name`,...)
{
  if(!is.scidb(X)) stop("X must be a scidb object")
  if(length(MARGIN)!=1) stop("MARGIN must indicate a single dimension")
  if(is.numeric(MARGIN)) MARGIN = X@D$name[MARGIN]
  if(missing(`name`)) `name` = X@attribute
# Check for common function names and map to SciDB expressions
  if(is.function(FUN))
  {
    M = match.call()
    fn = .scidbfun(FUN)
    if(is.null(fn))
      stop("Apply requires a valid SciDB aggregate function")
    FUN = sprintf("%s(%s)",fn,X@attribute)
  }
  Y = aggregate(X,MARGIN,FUN,eval=FALSE,unpack=FALSE)
  a = Y@attributes[length(Y@attributes)]
  attribute_rename(project(Y,a,eval=FALSE),a, `name`, eval=`eval`)
}

# x:   A scidb, scidbdf object
# by:  A list of character vector of dimension and or attribute names of x, or,
#      a scidb or scidbdf object that will be cross_joined to x and then
#      grouped by attribues of by.
# FUN: A SciDB aggregation expresion
`aggregate_scidb` = function(x,by,FUN,`eval`=FALSE,window,variable_window,unpack)
{
  if(missing(unpack)) unpack=TRUE
  if(missing(`by`))
  {
    `by`=""
  }
  if(!is.list(`by`)) `by`=list(`by`)
# Check for common function names and map to SciDB expressions
  if(is.function(FUN))
  {
    M = match.call()
    fn = .scidbfun(FUN)
    cb = unlist(by[lapply(by,is.character)]) # may be empty
    if(is.null(fn))
      stop("Aggregate requires a valid SciDB aggregate function")
    FUN = paste(paste(fn,"(",setdiff(x@attributes,cb),")",sep=""),collapse=",")
  }

  if(class(`by`[[1]]) %in% c("scidb","scidbdf"))
  {
# We are grouping by attributes in another SciDB array `by`. We assume that
# x and by have conformable dimensions to join along!
    j = intersect(x@D$name, by[[1]]@D$name)
# Check for and resolve attribute name conflicts:
    nn = make.unique_(x@attributes, by[[1]]@attributes)
    if(!isTRUE(all.equal(by[[1]]@attributes,nn)))
    {
      `by`[[1]]=attribute_rename(`by`[[1]],old=by[[1]]@attributes,new=nn)
    }
    x = merge(x,`by`[[1]],by=j,eval=FALSE,depend=list(x,`by`[[1]]))
    n = by[[1]]@attributes
    `by`[[1]] = n
  }
# A bug up to SciDB 13.6 unpack prevents us from using eval=FALSE
  if(!eval && !compare_versions(options("scidb.version")[[1]],13.9)) stop("eval=FALSE not supported by aggregate due to a bug in SciDB <= 13.6")

  b = `by`
  new_dim_name = make.names_(c(unlist(b),"row"))
  new_dim_name = new_dim_name[length(new_dim_name)]
  if(!all(b %in% c(x@attributes, x@D$name, "")))
  {
# Check for numerically-specified coordinate axes and replace with dimension
# labels.
    for(k in 1:length(b))
    {
      if(is.numeric(b[[k]]))
      {
        b[[k]] = x@D$name[b[[k]]]
      }
    }
  }
  if(!all(b %in% c(x@attributes, x@D$name, ""))) stop("Invalid attribute or dimension name in by")
  a = x@attributes %in% b
  query = x@name
# Handle group by attributes with redimension. We don't use a redimension
# aggregate, however, because some of the other group by variables may already
# be dimensions.
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
      unpack = TRUE
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
    }

# Reset in case things changed above
    a = x@attributes %in% b
    n = x@attributes[a]
# XXX XXX XXX
# XXX What about chunk sizes? Also insert reasonable upper bound instead of *? XXX Take care of all these issues...
# XXX XXX XXX
    redim = paste(paste(n,"=0:",.scidb_DIM_MAX,",1000,0",sep=""), collapse=",")
    D = paste(build_dim_schema(x,FALSE),redim,sep=",")
    A = x
    A@attributes = x@attributes[!a]
    A@nullable   = x@nullable[!a]
    A@types      = x@types[!a]
    S = build_attr_schema(A)
    D = sprintf("[%s]",D)
    query = sprintf("redimension(substitute(%s,build(<v:int64>[_i=0:0,1,0],-1),%s),%s%s)",x@name,paste(n,collapse=","),S,D)
  }
  along = paste(b,collapse=",")

# We use unpack to always return a data frame (a 1D scidb array). EXCEPT when
# aggregating along a single integer coordinate axis (not along attributes or
# multiple axes).
  if(!missing(window))
  {
    unpack = FALSE
    query = sprintf("window(%s, %s, %s)",query,paste(window,collapse=","),FUN)
  } else if(!missing(variable_window))
  {
    unpack = FALSE
    query = sprintf("variable_window(%s, %s, %s, %s)",query,along,paste(variable_window,collapse=","),FUN)
  } else
  if(nchar(along)<1)
    query = sprintf("aggregate(%s, %s)", query, FUN)
  else
    query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  if(unpack) query = sprintf("unpack(%s,%s)",query,new_dim_name)
  .scidbeval(query,eval,gc=TRUE,depend=list(x))
}

# The new (SciDB 13.9) cumulate
`cumulate` = function(x, expression, dimension, `eval`=FALSE)
{
  if(missing(dimension)) dimension = x@D$name[[1]]
  query = sprintf("cumulate(%s, %s, %s)",x@name,expression,dimension)
  .scidbeval(query,eval,depend=list(x))
}
