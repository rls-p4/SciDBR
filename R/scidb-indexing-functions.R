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


# dimfilter: The workhorse array subsetting function
# INPUT
# x: A scidb object
# i: a list of index expressions
# eval: (logical) if TRUE, return a new SciDB array, otherwise just a promise
# drop: (logical) if TRUE, delete dimensions of an array with only one level
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
dimfilter = function(x, i, eval, drop)
{
# Partition the indices into class:
# Identify sequential, numeric indices
  si = sapply(i, checkseq)
# Identify explicit between-type indices (functions, lists)
  bi = sapply(i, function(x) inherits(x,"function"))
# Unspecified range
  ui = sapply(i,is.null)
# Identify everything else
  ci = !(si | bi | ui)

  if(length(x@attribute)<1) x@attribute=x@attributes[1]
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
        c(min(i[j][[1]]),max(i[j][[1]]))
      }
      else
       {
# Unspecified range or special index (ci case), which we handle later.
#         c(x@D$start[j],x@D$start[j] + x@D$length[j] - 1)
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
  if(!everything)
  {
    newstart = unlist(lapply(ranges,function(z)z[1]))
    newstart[newstart=="null"] = NA
    newstart = as.numeric(newstart)
    ina = is.na(newstart)
    if(any(ina))
    {
      newstart[ina] = x@D$start[ina]
    }
    newlen = unlist(lapply(ranges,function(z)z[2]))
    newlen[newlen=="null"] = NA
    newlen = as.numeric(newlen) - newstart + 1
    ina = is.na(newlen)
    if(any(ina))
    {
      newlen[ina] = x@D$length[ina]
    }
    q = sprintf("redimension(%s, %s%s)", q, build_attr_schema(x),
          build_dim_schema(x,newlen=newlen,newstart=newstart))
  }
# Return a new scidb array reference
  ans = .scidbeval(q,eval=FALSE,gc=TRUE,attribute=x@attribute,`data.frame`=FALSE,depend=x)
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

# Helper function to drop dimensions of any scidb object
drop_dim = function(ans)
{
  i = ans@D$length==1
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
  for(j in 1:length(idx))
  {
    N = x@D$name[[j]]
    dimlabel = make.unique_(x@D$name,paste(N,"_1",sep=""))
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
      if(is.numeric(i[[j]]))
      {
# Special index case 1: non-contiguous numeric indices
        tmp = data.frame(as.integer(unique(i[[j]])))
        if(nrow(tmp)!=length(i[[j]]))
        {
          warning("The scidb package doesn't yet support repeated indices in subarray selection")
        }
        names(tmp) = N
        i[[j]] = as.scidb(tmp, types="int64", dimlabel=dimlabel,
                        start=x@D$start[[j]],
                        chunkSize=x@D$chunk_interval[[j]],
                        rowOverlap=x@D$chunk_overlap[[j]])
        swap = c(swap, list(list(old=N, new=dimlabel, length=length(tmp[,1]))))

        Q1 = sprintf("redimension(%s,<%s:int64>%s)", i[[j]]@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as _cazart1, %s as _cazart2, _cazart1.%s, _cazart2.%s)",query, Q1, N, N)
      } else if(is.character(i[[j]]))
      {
# Case 2: character labels, consult a lookup array if possible
         lkup = substitute(x@gc$dimnames[[j]])
         tmp = data.frame(i[[j]], stringsAsFactors=FALSE)
         names(tmp) = N
         tmp_1 = as.scidb(tmp, types="string", dimlabel=dimlabel,
                        start=x@D$start[[j]],
                        chunkSize=x@D$chunk_interval[[j]],
                        rowOverlap=x@D$chunk_overlap[[j]], nullable=FALSE)
        i[[j]] = attribute_rename(project(index_lookup(tmp_1, lkup, new_attr='_cazart'),"_cazart"),"_cazart",N)
        dependencies = c(dependencies, tmp_1)
        swap = c(swap, list(list(old=N, new=dimlabel, length=length(tmp[,1]))))
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", i[[j]]@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as _cazart1, %s as _cazart2, _cazart1.%s, _cazart2.%s)",query, Q1, N, N)
      } else if(is.scidb(i[[j]]))
      {
# Case 3. A SciDB array, really just a densified cross_join selector.
        tmp = sort(project(bind(i[[j]],N,i[[j]]@D$name[[1]],eval=0),N,eval=0),eval=0)
# Insane scidb name conflict problems, check for and resolve them.
        tmpaname = make.unique_(dimlabel, tmp@attributes)
        cst = paste(build_attr_schema(tmp,newnames=tmpaname),build_dim_schema(tmp,newnames=dimlabel))
        tmp = cast(tmp,cst,eval=0)
        cnt = count(tmp)
        dependencies = c(dependencies, tmp)
        Q1 = sprintf("redimension(%s,<%s:int64>%s)", tmp@name,dimlabel,build_dim_schema(x,I=j,newnames=N))
        query = sprintf("cross_join(%s as _cazart1, %s as _cazart2, _cazart1.%s, _cazart2.%s)",query, Q1, N, N)
        swap = c(swap, list(list(old=N, new=dimlabel, length=cnt)))
      }
    } else # No special index in this coordinate
    {
      swap = c(swap,list(list(old=N, new=N, length=x@D$length[[j]])))
    }
  }
  nn = sapply(swap, function(x) x[[2]])
  nl = sapply(swap, function(x) x[[3]])
  newstart = x@D$start
  query = sprintf("redimension(%s, %s%s)",query, build_attr_schema(x), build_dim_schema(x,newstart=newstart,newnames=nn,newlen=nl))
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
  type = names(.scidbtypes[.scidbtypes==x@type])
  if(length(type)<1)
  {
    stop("Unsupported data type. Try using the iquery function.")
  }

# Set origin to zero and project. We need the zero origin here to reconstruct
# array indices in R. We don't wrap subarray in sg here because there is no
# chance that this will be involved in a gemm query later.
  N = paste(rep("null",2*length(x@D$name)),collapse=",")
  query = sprintf("subarray(project(%s,%s),%s)",x@name,x@attribute,N)

# Unpack
  query = sprintf("unpack(%s,%s)",query,"__row")

# We're always assuming missing values may exist
  i = paste(rep("int64",length(x@dim)),collapse=",")
#  nl = x@nullable[x@attribute==x@attributes][[1]]
  nl = TRUE
  N = ifelse(nl,"NULL","")

  savestring = sprintf("(%s,%s %s)",i,x@type,N)

  sessionid = tryCatch(
                scidbquery(query, save=savestring, async=FALSE, release=0),
                error = function(e) {stop(e)})
# Release the session on exit
  on.exit( GET("/release_session",list(id=sessionid)) ,add=TRUE)
  n = 0

  r = URI("/read_bytes",list(id=sessionid,n=n))
  BUF = getBinaryURL(r, .opts=list('ssl.verifypeer'=0))

  ndim = as.integer(length(x@D$name))
  type = eval(parse(text=paste(names(.scidbtypes[.scidbtypes==x@type]),"()")))
  len  = as.integer(.typelen[names(.scidbtypes[.scidbtypes==x@type])])
  len  = len + nl # Type length
  i64 = 0L;
# Special int64 cases:
  if(x@type=="int64")
  {
    i64 = 1L;
    warning("Coercing SciDB int64 values to R double precision real numeric values.")
  }
  if(x@type=="uint64")
  {
    i64 = 2L;
    warning("Coercing SciDB uint64 values to R double precision real numeric values.")
  }

  nelem = length(BUF) / (ndim*8 + len)
  stopifnot(nelem==as.integer(nelem))
  A = tryCatch(
    {
      .Call("scidbparse",BUF,ndim,as.integer(nelem),type,as.integer(nl),i64,PACKAGE="scidb")
    },
    error = function(e){stop(e)})

  p = prod(x@D$length)

# Check for sparse matrix case
  if(ndim==2 && nelem<p)
  {
    return(Matrix::sparseMatrix(i=A[[2]][,1]+1,j=A[[2]][,2]+1,x=A[[1]],dims=x@D$length))
  } else if(ndim==1 && nelem <p)
  {
    return(Matrix::sparseVector(x=A[[1]],i=A[[2]][,1]+1,length=p))
  } else if(nelem<p)
  {
# Don't know how to represent this in R!
    names(A)=c("values","coordinates")
    return(A)
  }
#  aperm(array(data=A[[1]], dim=x@D$length, dimnames=x@D$name),
#        perm=seq(from=length(x@D$length),to=1,by=-1))
  ans = array(NA, dim=x@dim)
  ans[A[[2]]+1] = A[[1]]
  if(length(dim(ans))==1)
  {
    ans=as.vector(ans)
  }
  return(ans)
}
