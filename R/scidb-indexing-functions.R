#/*
#**
#* BEGIN_COPYRIGHT
#*
#* This file is part of SciDB.
#* Copyright (C) 2008-2013 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify
#* it under the terms of the AFFERO GNU General Public License as published by
#* the Free Software Foundation.
#*
#* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
#* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
#* the AFFERO GNU General Public License for the complete license terms.
#*
#* You should have received a copy of the AFFERO GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#*
#* END_COPYRIGHT
#*/

# This file contains functions that map R indexing operations to SciDB queries.
# Examples include subarray and cross_join wrappers.  They are a bit
# complicated in order to support empty-able and NID arrays.

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


# Return a query that converts selected NIDs to integer for a single-attrbute
# SciDB array.
# A a scidb object
# idx a list of dimensions numbers to drop NID, or a boolean vector of length
#     equal to the number of dimensions with TRUE indicating drop NID.
# schema_only means only return the new schema string.
# query is a query to be wrapped in drop_nid, defaults to array name.
# nullable is  either "NULL" or "" and indicates nullable on the attribute.
selectively_drop_nid = function(A, idx, schema_only=FALSE, query, nullable)
{
  if(missing(nullable)) nullable = ""
  if(inherits(A,"scidb"))
  {
    D = A@D
    if(missing(nullable))
      nullable = ifelse(A@nullable[A@attributes == A@attribute][[1]],"NULL","")
  }
  else stop("A must be a SciDB reference object")
  if(missing(query)) query = A@name
  if(missing(idx)) idx = rep(TRUE,length(D$name))
  if(is.numeric(idx)) idx = is.na(match(1:length(D$name),idx))
# XXX This is a problem...SciDB can index up to 2^62 - 1, but we can only
# really resolve smaller numbers here since we don't have 64-bit integers in R.
  j=which(D$length>=2^62)
  if(length(j)>0) D$length[j] = .scidb_DIM_MAX
  query = sprintf("project(%s, %s)",query, A@attribute)
#  if(!any(idx)) return(query) # XXX screws up schema_only requests...
# Pass-through non-nid indices
  idx = idx | (D$type %in% "int64")

  schema = lapply(1:length(D$name), function(j) {
    dlen = D$length[j]
    if(idx[j]) {
# Drop NID
      if(is.character(dlen))
        sprintf("%s=%.0f:%s,%.0f,%.0f",D$name[j],D$start[j],dlen,D$chunk_interval[j],D$chunk_overlap[j])
      else
        sprintf("%s=%.0f:%.0f,%.0f,%.0f",D$name[j],D$start[j],dlen+D$start[j]-1,D$chunk_interval[j],D$chunk_overlap[j])
    } else {
# Don't drop NID
      if(is.character(dlen))
        sprintf("%s(%s)=%s,%.0f,%.0f",D$name[j],D$type[j],dlen,D$chunk_interval[j],D$chunk_overlap[j])
      else
        sprintf("%s(%s)=%.0f,%.0f,%.0f",D$name[j],D$type[j],dlen,D$chunk_interval[j],D$chunk_overlap[j])
    }
  })
  schema = paste(schema,collapse=",")
  schema = sprintf("<%s:%s %s>[%s]",A@attribute,A@type,nullable,schema)
  if(schema_only) return(schema)
  sprintf("cast(%s,%s)",query, schema)
}



# dimfilter: The workhorse array subsetting function
# INPUT
# x: A SciDB array reference object
# i: a list of index expressions
# OUTPUT
# returns a new SciDB reference array
#
# dimfilter distinguishes between four kinds of indexing operations:
# 'si' sequential numeric index range, for example c(1,2,3,4,5)
# 'bi' special between index range, that is a function or a list
#      that returns upper/lower limits
# 'ui' not specified range (everything, by R convention)
# 'ci' lookup-style range, a non-sequential numeric or labeled set, for example
#      c(3,3,1,5,3)   or  c('a1','a3')
#
dimfilter = function(x, i)
{
# Partition the indices into class:
# Identify sequential, numeric indices
  si = sapply(i, scidb:::checkseq)
# Identify explicit between-type indices (functions, lists)
  bi = sapply(i, function(x) inherits(x,"function"))
# Unspecified range
  ui = sapply(i,is.null)
# Identify lookup-type indices
  ci = !(si | bi | ui)

# Check for mismatched range and dimension types.
  rt = rangetype(x,i, si, bi, ci)
  q = rt$query
  r = lapply(1:length(bi), function(j)
    {
      if(bi[j])
      {
# Just use the provided range
        rx = unlist(i[j][[1]]())
        if(inherits(rx,"character"))
          rx = paste("'",rx,"'",sep="")
        rx
      }
      else if(si[j] || ci[j])
      {
# numeric or lookup-type range
        rx = c(min(i[j][[1]]),max(i[j][[1]]))
        if(inherits(rx,"character"))
          rx = paste("'",rx,"'",sep="")
        rx
      }
      else
       {
# Unspecified range
         if(x@D$type[j]=="string")
         {
# XXX Is there a better way that avoids the query?
           mx = iquery(sprintf("max(%s:%s)",x@name,x@D$name[j]),return=TRUE)[1,2]
           c("''",sprintf("'%s'",mx))
         }
         else
         {
           c(x@D$start[j],x@D$start[j] + x@D$length[j] - 1)
         }
       }
    })
  r = unlist(lapply(r,function(x)sprintf("%.0f",x)))
  ro = r[seq(from=1,to=length(r),by=2)]
  re = r[seq(from=2,to=length(r),by=2)]
  r = paste(c(ro,re),collapse=",")
  q = sprintf("between(%s,%s)",q,r)

# Return a new scidb array reference
  if(any(ci)) 
  {
    ans = lookup_subarray(x,q,i,ci,rt$mask)
  }
  else
  {
    ans = tmpnam()
#    q = sprintf("store(%s,%s)",q,ans)
# We use subarray here to set the origin of the new array
    q = sprintf("store(subarray(%s,%s),%s)",q,r,ans)
    iquery(q)
  }
  scidb(ans,gc=TRUE,`data.frame`=FALSE)
}

# x: SciDB array reference
# q: A query string with a between statement that bounds the selection
# i: list of requested indices from user
# ci: logical vector of length(i) indicating which ones are lookup-type
# mask
lookup_subarray = function(x, q, i, ci, mask)
{
# Create ancillary arrays for each dimension index list.
  n = length(ci)
  if(n>2) stop ("This kind of indexing not yet supported in the R package yet for arrays of dimension > 2, sorry. Use numeric indices instead.")
  xdim = unlist(lapply(ci, function(j) tmpnam()))
  on.exit(scidbremove(xdim[!is.na(xdim)],error=function(e) invisible()),add=TRUE)
  for(j in 1:n)
  {
    if(ci[[j]])
    {
# User-specified indices
      X = data.frame(i[[j]])
      names(X) = "xxx__a"
      if(!is.numeric(i[[j]]))
      {
        df2scidb(X,types="string",nullable=FALSE,name=xdim[j],dimlabel=x@D$name[[j]])
        mask[j] = TRUE
        warning("Dimension labels were dropped.")
      } else
      {
        df2scidb(X,types="int64",nullable=FALSE,name=xdim[j],dimlabel=x@D$name[[j]])
      }
    } else # Not lookup-style index
    {
      if(x@D$type[[j]]=="string")
      {
        q1=sprintf("create_array(%s,<value:string>[%s(string)=*,%.0f,0])",xdim[j],x@D$name[[j]],x@D$chunk_interval[[j]])
        iquery(q1)
        q2=sprintf("redimension_store(apply(%s:%s,%s,value),%s)",x@name,x@D$name[[j]],x@D$name[[j]],xdim[j])
        iquery(q2)
      } else # assume integer dimension
      {
         if(!is.null(i[[j]]))
           q1 = sprintf("build(<xxx__a:int64>[%s=%.0f:%.0f,%.0f,0],%s)",x@D$name[[j]],min(i[[j]]),max(i[[j]]),x@D$chunk_interval[[j]],x@D$name[[j]])
         else
           q1 = sprintf("build(<xxx__a:int64>[%s=%.0f:%.0f,%.0f,0],%s)",x@D$name[[j]],x@D$low[[j]],x@D$high[[j]],x@D$chunk_interval[[j]],x@D$name[[j]])
         q1 = sprintf("store(%s,%s)",q1,xdim[j])
         iquery(q1)
      }
    }
  }

  ans = tmpnam()
  if(n==1) q = sprintf("lookup(%s, %s)",xdim[1],q)
  else     q = sprintf("lookup(cross(%s,%s),%s)",xdim[1],xdim[2],q)
  lb = x@D$type
  ub = x@D$type
# Adjust for NIDs selectively dropped
  if(any(mask))
  {
    lb[mask] = "int64"
    ub[mask] = "int64"
  }
  li = lb == "int64"
  lb[li] = .scidb_DIM_MIN
  lb[!li] = "null"
  ui = ub == "int64"
  ub[ui] = .scidb_DIM_MAX
  ub[!ui] = "null"   # XXX only approx upper bound
  lb = paste(lb, collapse=",")
  ub = paste(ub, collapse=",")
  q = sprintf("store(subarray(%s,%s,%s),%s)",q,lb,ub,ans)
  iquery(q)
  ans
}

# x: A SciDB array reference
# i: A list of index ranges
# si: A vector of logicals equal to length of i showing positions with
#     sequential numeric index types.
# bi: A vector showing positions of i with between/subarray-style index types
# ci: A vector showing positions of i with lookup-style types
# Returns a list with two elements:
# $query:  A starting query
# $mismatch: A logical value. TRUE means the schema differs from the
#            input array.
# mask: A vector set to TRUE in positions that have been cast to int64
#
# The rangetype function checks for mismatches between index range types
# and array schema, for example specifying integer indices instead of
# NID. The function tries to harmonize the types with CAST. It will fail
# with an error if the ranges can't be fixed up with CAST.
rangetype = function(x, i, si, bi, ci)
{
  mismatch = FALSE
  schema   = NA
  q        = x@name
  M        = rep(FALSE,length(x@D$type))
# Check si types
  if(any(si))
  {
    sim = x@D$type != "int64" & si
    M   = M | sim
    if(any(sim))
    {
      mismatch = TRUE
      schema = selectively_drop_nid(x,sim, schema_only=TRUE)
      q = selectively_drop_nid(x,sim)
    }
  }
# Check ci types
  if(any(ci))
  {
    mask = x@D$type =="string" & sapply(i,class)=="numeric"
    M   = M | mask
    if(any(mask))
    {
      mismatch = TRUE
      schema = selectively_drop_nid(x,mask, schema_only=TRUE)
      q = selectively_drop_nid(x,mask)
    }
  }
# Check bi types XXX to do

  if(!mismatch) q = sprintf("project(%s,%s)",q,x@attribute)
  list(mismatch=mismatch, query=q, schema=schema, mask=M)
}


# Materialize the single-attribute scidb array x as an R array.
materialize = function(x, drop=FALSE)
{
  type = names(.scidbtypes[.scidbtypes==x@type])
  if(length(type)<1) stop("Unsupported data type. Try using the iquery function instead.")
  tval = vector(mode=type,length=1)
# Dispense with NID
  query = selectively_drop_nid(x,rep(TRUE,length(x@D$type)),nullable="NULL")

# Set origin to zero
  l1 = length(dim(x))
  lb = paste(rep("null",l1),collapse=",")
  ub = paste(rep("null",l1),collapse=",")
  query = sprintf("subarray(%s,%s,%s)",query,lb,ub)

# Unpack
  query = sprintf("unpack(%s,%s)",query,"__row")

  i = paste(rep("int64",length(x@dim)),collapse=",")
#  nl = x@nullable[x@attribute==x@attributes][[1]]
  nl = TRUE
  N = ifelse(nl,"NULL","")

  savestring = sprintf("&save=(%s,%s %s)",i,x@type,N)

  sessionid = tryCatch(
                scidbquery(query, save=savestring, async=FALSE, release=0),
                error = function(e) {stop(e)})
# Release the session on exit
  on.exit( GET(paste("/release_session?id=",sessionid,sep=""),async=FALSE) ,add=TRUE)
  host = get("host",envir=.scidbenv)
  port = get("port",envir=.scidbenv)
  n = 0

  r = sprintf("http://%s:%d/read_bytes?id=%s&n=%.0f",host,port,sessionid,n)
  BUF = getBinaryURL(r)

  ndim = as.integer(length(x@D$name))
  type = eval(parse(text=paste(names(.scidbtypes[.scidbtypes==x@type]),"()")))
  len  = as.integer(.typelen[names(.scidbtypes[.scidbtypes==x@type])])
  len  = len + nl # Type length

  nelem = length(BUF) / (ndim*8 + len)
  stopifnot(nelem==as.integer(nelem))
  A = tryCatch(
    {
      .Call("scidbparse",BUF,ndim,as.integer(nelem),type,N)
    },
    error = function(e){stop(e)})

  p = prod(x@D$length)

# Check for sparse matrix case
  if(ndim==2 && nelem<p)
  {
    require("Matrix")
    return(sparseMatrix(i=A[[2]][,1]+1,j=A[[2]][,2]+1,x=A[[1]],dims=x@D$length))
  } else if(nelem<p)
  {
# Don't know how to represent this in R!
    names(A)=c("coordinates","values")
    return(A)
  }
  array(data=A[[1]], dim=x@D$length, dimnames=x@D$name)
}
