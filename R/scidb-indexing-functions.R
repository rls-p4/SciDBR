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

# A utility function that returns TRUE if entries in the numeric vector j
# are sequential and in increasing order.
checkseq = function(j)
{
  if(!is.numeric(j)) return(FALSE)
  !any(diff(j)!=1)
}

# Returns a function that evaluates to a list of bounds
between = function(a,b)
{
  if(missing(b))
  {
    if(length(a)==2)
    {
      b = a[[2]]
      a = a[[1]]
    } else stop("between requires two arguments or a single argument with two elements")
  }
  function() list(a,b)
}

`$.scidb` = function(x, ...)
{
  M = match.call()
  a = as.character(M[3])
  project(x, a)
}

# dimfilter: The workhorse array subsetting function
# INPUT
# x: A scidb object
# i: a list of index expressions
# eval: (logical) if TRUE, return a new SciDB array, otherwise just a promise
# drop: (logical) if TRUE, delete dimensions of an array with only one level
# redim: (logical) default TRUE, if false the supress bounding redimension
# OUTPUT
# a scidb object
#
# dimfilter distinguishes between four kinds of indexing operations:
# 'si' sequential numeric index range, for example c(1,2,3,4,5)
# 'bi' special between index range, that is a function or a list
#      that returns upper/lower limits
# 'ui' not specified range (everything, by R convention)
# 'ci' other, for example c(3,1,2,5) or c(1,1)
#
dimfilter = function(x, i, eval, drop, redim)
{
  if(missing(redim)) redim=TRUE
# Partition the indices into class:
# Identify sequential, numeric indices
  si = sapply(i, checkseq)
# Identify explicit between-type indices (functions, lists)
  bi = sapply(i, function(x) inherits(x,"function"))
# Unspecified range
  ui = sapply(i,is.null)
# Identify everything else
  ci = !(si | bi | ui)

  r = lapply(1:length(bi), function(j)
    {
      if(bi[j])
      {
# Just use the provided between-style range
        unlist(i[j][[1]]())
      }
      else if(si[j])
      {
# sequential numeric or lookup-type range
        noE(c(min(i[j][[1]]),max(i[j][[1]])))
      }
      else
       {
# Unspecified range or special index (ci case), which we handle later.
         c('null','null')
       }
    })
  ranges = r
  r = unlist(lapply(r,noE))
  everything = all(r %in% "null")
  ro = r[seq(from=1,to=length(r),by=2)]
  re = r[seq(from=2,to=length(r),by=2)]
  r = paste(c(ro,re),collapse=",")
  q = sprintf("between(%s,%s)",x@name,r)
# XXXX XXX XXX XXX
  new_dimnames = c()
  new_depend = x
  if(!everything)
  {
    newstart = unlist(lapply(ranges,function(z)z[1]))
    newstart[newstart=="null"] = NA
    ina = is.na(as.numeric(newstart))
    if(any(ina))
    {
      newstart[ina] = scidb_coordinate_start(x)[ina]
    }
    newend = unlist(lapply(ranges,function(z)z[2]))
    newend[newend=="null"] = NA
    ina = is.na(newend)
    if(any(ina))
    {
      newend[ina] = scidb_coordinate_end(x)[ina]
    }
# Propagate dimension labels
    if(!is.null(dimnames(x)))
    {
      new_dimnames =  vector(mode="list",length=length(dim(x)))
      for(j in 1:length(new_dimnames))
      {
        if(!is.null(dimnames(x)[[j]]))
        {
          new_dimnames[[j]] = scidb(sprintf("project(join(redimension(%s,%s%s) as x, %s as y), y.%s)",q,build_attr_schema(x),build_dim_schema(x,I=j), dimnames(x)[[j]]@name, scidb_attributes(dimnames(x)[[j]])[[1]]))
          new_depend = c(new_depend, dimnames(x)[[j]])
        }
      }
    }
    if(redim)
    {
      q = sprintf("redimension(%s, %s%s)", q, build_attr_schema(x),
          build_dim_schema(x,newend=newend,newstart=newstart))
    }
  }
# Return a new scidb array reference
  ans = .scidbeval(q,eval=FALSE,gc=TRUE,`data.frame`=FALSE,depend=new_depend)
#  dimnames(ans) = new_dimnames # no--it does too much housekeeping for us,
# we know that we already have a conformable schema so just update the dimname
# directly:
  ans@gc$dimnames = new_dimnames
  if(any(ci)) 
  {
    assign("dimnames",dimnames(x),envir=ans@gc)
    return(special_index(ans, ans@name, i, ci, eval, drop))
  }
# Drop singleton dimensions if instructed to
  if(drop)
  {
    ans = drop_dim(ans)
  }
  if(`eval`)
  {
    ans = scidbeval(ans)
  }
  ans
}

# Helper function to drop dimensions of length 1 (cf R's drop)
# XXX DROP DIMNAMES TOO
drop_dim = function(ans)
{
  i = as.numeric(scidb_coordinate_bounds(ans)$length) == 1
  if(all(i))
  {
    i[1] = FALSE
  }
  if(any(i))
  {
    i = which(i)
    A = build_attr_schema(ans)
    D = build_dim_schema(ans,I=-i)
    query = sprintf("redimension(%s, %s%s)",ans@name,A,D)
    ans = .scidbeval(query,`eval`=FALSE,depend=list(ans))
  }
  ans
}

# XXX Lots of cleanup required in this function...
special_index = function(x, query, i, idx, eval, drop=FALSE)
{
  swap = NULL
  dependencies = x
  xdims = dimensions(x)
  xstart = scidb_coordinate_start(x)
  xend = scidb_coordinate_end(x)
  xchunk = scidb_coordinate_chunksize(x)
  xoverlap = scidb_coordinate_overlap(x)

  for(j in 1:length(idx))
  {
    N = xdims[j]
    dimlabel = make.unique_(xdims, sprintf("%s_1",N))
    if(is.list(swap[[1]]))
    {
      slabels = unlist(lapply(swap,function(x)x[[2]]))
      dimlabel = make.unique_(slabels, dimlabel)
    } else if(!is.null(swap))
    {
      dimlabel = make.unique_(swap[[2]], dimlabel)
    }
    if(idx[[j]])
    {
# Check for and try to adjust this akward case:
      if(is.scidbdf(i[[j]])) i[[j]] = project(i[[j]],1)
      if(is.numeric(i[[j]]))
      {
# Special index case 1: non-contiguous numeric indices
        tmp = data.frame(noE(unique(i[[j]])))
        if(nrow(tmp)!=length(i[[j]]))
        {
          warning("The scidb package doesn't yet support repeated indices in subarray selection")
        }
        names(tmp) = N
        i[[j]] = as.scidb(tmp, types="int64", dimlabel=dimlabel,
                        start=xstart[j],
                        chunkSize=xchunk[j],
                        rowOverlap=xoverlap[j], nullable=FALSE)
        swap = c(swap, list(list(old=N, new=dimlabel, length=length(tmp[,1]), start=xstart[j])))
        dependencies = c(dependencies, i[[j]])
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", i[[j]]@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as _cazart1, %s as _cazart2, _cazart1.%s, _cazart2.%s)",query, Q1, N, N)
      } else if(is.character(i[[j]]))
      {
# Case 2: character labels, consult a lookup array if possible
         lkup = replaceNA(x@gc$dimnames[[j]])
         if(length(dim(lkup))>1)
         {
# Hmmm. The lookup array has more than one dimension! Let's just use the first.
           lkup = slice(lkup,2:length(dim(lkup)),scidb_coordinate_start(lkup)[-1])
         }
         tmp = data.frame(i[[j]], stringsAsFactors=FALSE)
         names(tmp) = N
         tmp_1 = as.scidb(tmp, types="string", dimlabel=dimlabel,
                        start=xstart[j],
                        chunkSize=xchunk[j],
                        rowOverlap=xoverlap[j], nullable=FALSE)
        i[[j]] = attribute_rename(project(index_lookup(tmp_1, lkup, new_attr='_cazart'),"_cazart"),"_cazart",N)
        dependencies = c(dependencies, tmp_1)
        swap = c(swap, list(list(old=N, new=dimlabel, length=length(tmp[,1]), start=xstart[j])))
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", i[[j]]@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as _cazart1, %s as _cazart2, _cazart1.%s, _cazart2.%s)",query, Q1, N, N)
      } else if(is.scidb(i[[j]]))
      {
# Case 3. A SciDB array, really just a densified cross_join selector.
# If it's Boolean, convert it to a sparse array for cross_join indexing
        if(scidb_types(i[[j]])[[1]] == "bool")
        {
          i[[j]] = i[[j]] %==% TRUE
        }
        tmp = bind(i[[j]],N,dimensions(i[[j]])[1],eval=FALSE)
        tmp = sort(project(tmp, length(scidb_attributes(tmp)),eval=FALSE),eval=FALSE)
# Insane scidb name conflict problems, check for and resolve them.
        tmp = attribute_rename(tmp, old=1, new=N)
        tmpaname = make.unique_(dimlabel, scidb_attributes(tmp))
        cst = paste(build_attr_schema(tmp,newnames=tmpaname),build_dim_schema(tmp,newnames=dimlabel))
        tmp = cast(tmp,cst,eval=0)
        cnt = count(tmp)
        dependencies = c(dependencies, tmp)
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", tmp@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as _cazart1, %s as _cazart2, _cazart1.%s, _cazart2.%s)",query, Q1, N, N)
# Note start=0 comes from the sort...
        swap = c(swap, list(list(old=N, new=dimlabel, length=cnt, start=0)))
      }
    } else # No special index in this coordinate
    {
      len = scidb_coordinate_bounds(x)$length
      swap = c(swap,list(list(old=N, new=N, length=len[j], start=xstart[j])))
    }
  }
  nn = sapply(swap, function(x) x[[2]])
  nl = sapply(swap, function(x) x[[3]])
  ns = sapply(swap, function(x) x[[4]])
  query = sprintf("redimension(%s, %s%s)",query, build_attr_schema(x), build_dim_schema(x,newstart=ns,newnames=nn,newlen=nl))
  ans = .scidbeval(query, eval=FALSE, depend=dependencies)
  if(drop)
  {
    ans = drop_dim(ans)
  }
  if(`eval`)
  {
    ans = scidbeval(ans)
  }
  ans
}


# Materialize the single-attribute scidb array x as an R array.
materialize = function(x, drop=FALSE)
{
# If x has multiple attributes, warn.
  if(length(x@attributes)>1)
  {
    warning("The array contains multiple SciDB attributes, returning as an unpacked dataframe.")
    return(iquery(x, return=TRUE,n=Inf))
  }
  type = names(.scidbtypes[.scidbtypes==scidb_types(x)])
# Check for types that are not fully supported yet.
  xstart = as.numeric(scidb_coordinate_start(x))
  attr = .get_attribute(x)
  if(length(type)<1)
  {
    u = unpack(x)[]
    ans = tryCatch(
      {
        array(dim=dim(x))
      },error = function(e)
      {
        n = length(dim(x))
        array(dim=apply(u[,1:n,drop=FALSE],2,function(x){max(x)+1}))
      })
    i = as.matrix(u[,1:length(dim(x))])
    for(j in 1:length(dim(x))) i[,j] = i[,j] + 1 - xstart[j]
    ans[i] = u[,ncol(u)]
    return(ans)
  }


# Set array index origin to zero. We need the zero origin here to reconstruct
# array indices in R.
  d     = dim(x)
  ndim  = length(dimensions(x))
  N     = paste(rep("null",2*ndim),collapse=",")
  query = sprintf("subarray(project(%s,%s),%s)",x@name,attr,N)

# Unpack into a staging data frame
  data  = scidb_unpack_to_dataframe(query)
  nelem = nrow(data)
  if(is.null(nelem)) nelem = 0
  p     = prod(d)
# Adjust indexing origin
  data[,1:ndim] = data[,1:ndim] + 1

# Check for sparse matrix or sparse vector case. The tryCatch guards
# against unsupported types in R's sparse Matrix package and returns the
# raw data frames in bad cases.
  if(ndim==2 && nelem < p)
  {
    ans = tryCatch(
            Matrix::sparseMatrix(i=data[,1],j=data[,2],x=data[,3],dims=d),
            error=function(e) {warning(e,"Note: The R sparse Matrix package does not support certain value types like\ncharacter strings");data})
    return(ans)
  } else if(ndim==1 && nelem < p)
  {
    ans = tryCatch(
            Matrix::sparseVector(i=data[,1],x=data[,2],length=p),
            error=function(e) {warning(e,"Note: The R sparse Matrix package does not support certain value types like\ncharacter strings");data})
    return(ans)
  } else if(nelem < p)
  {
# Don't know how to represent this in R! (R only knows sparse vectors or arrays)
    warning("Note: R does not natively support sparse n-d objects for n>2. Returning data as a data frame.")
    return(data)
  }
# OK, we have a dense array of some kind
  if(length(d)==1) return(data[,2])  # A vector

  ans = array(NA, dim=d)  # A matrix or n-d array
  ans[as.matrix(data[,1:ndim])] = data[,ndim+1]

# Handle coordinate labels
  if(!is.null(dimnames(x)))
  {
    dimnames(ans) = lapply(1:ndim, function(j)
    {
      if(is.null(dimnames(x)[[j]])) return(NULL)
      if(!is.scidb(dimnames(x)[[j]])) return(NULL)
      dn = iquery(dimnames(x)[[j]]@name, re=TRUE, binary=TRUE)
      dn[,1] = dn[,1]  - dn[1,1] + 1
      nm =  rep("",dim(x)[j])
      nm[dn[,1]] = dn[,2]
      nm
    })
  }
  ans
}
