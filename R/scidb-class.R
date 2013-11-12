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

# A general SciDB array class for R. It's a hybrid S4 class with some S3
# methods. The class can represent SciDB arrays and array promises.
# slots:
# name = any SciDB expression that can produce an array 
# schema = the corresponding SciDB array schema for 'name' above
# D = dimensions data derived from the schema
# dim = R dim vector derived from the schema
# length = number of elements derived from the schema
# attribute = attribute in use or 0-length string, in which case the 
#             1st listed attribute is used specified by user for  objects that
#             can only work with one attribute at a time (linear algebra)
# attributes = table (data frame) of array attributes parsed from schema
# type = SciDB type of the attribute in use (character)
# types = list of SciDB types of all attributes (character)
# gc = environment
#      If gc$remove = TRUE, remove SciDB array when R gc is run on object.

setClassUnion("numericOrNULL", c("numeric", "NULL")) 
setClass("scidb",
         representation(name="character",
                        schema="character",
                        D="list",
                        dim="numericOrNULL",
                        length="numeric",
                        attribute="character",
                        attributes="character",
                        nullable="logical",
                        type="character",
                        types="character",
                        gc="environment"),
         S3methods=TRUE)

setMethod("%*%",signature(x="scidb", y="scidb"),
  function(x,y)
  {
    scidbmultiply(x,y)
  },
  valueClass="scidb"
)

setMethod("%*%",signature(x="scidb", y="ANY"),
  function(x,y)
  {
    if(!inherits(y,"scidb"))
    {
      on.exit(tryCatch(scidbremove(y@name),error=function(e)invisible()))
      y = as.scidb(cbind(y),name=basename(tempfile(pattern="array")), rowChunkSize=x@D$chunk_interval[2],start=c(x@D$start[[1]],0L))
    }
    scidbmultiply(x,y)
  },
  valueClass="scidb"
)

setMethod("%*%",signature(x="ANY", y="scidb"),
  function(x,y)
  {
    if(!inherits(x,"scidb"))
    { # Lazy evaluation saves the day...
      on.exit(tryCatch(scidbremove(x@name),error=function(e)invisible()))
      x = as.scidb(cbind(x),name=basename(tempfile(pattern="array")),colChunkSize=y@D$chunk_interval[1],start=c(0L,y@D$start[[2]]))
    }
    scidbmultiply(x,y)
  },
  valueClass="scidb"
)


setMethod("crossprod",signature(x="scidb", y="ANY"),
  function(x,y)
  {
    if(is.null(y)) y = x
    t(x) %*% y
  },
  valueClass="scidb"
)

setMethod("crossprod",signature(x="ANY", y="scidb"),
  function(x,y)
  {
    if(is.null(x)) x = y
    t(x) %*% y
  },
  valueClass="scidb"
)

setMethod("tcrossprod",signature(x="scidb", y="ANY"),
  function(x,y)
  {
    if(is.null(y)) y = x
    x %*% t(y)
  },
  valueClass="scidb"
)

setMethod("tcrossprod",signature(x="ANY", y="scidb"),
  function(x,y)
  {
    if(is.null(x)) x = y
    x %*% t(y)
  },
  valueClass="scidb"
)


# The remaining functions return data to R:
setGeneric("sum")
setMethod("sum", signature(x="scidb"),
function(x)
{
  iquery(sprintf("sum(%s)",x@name),return=TRUE)[,2]
})

setGeneric("mean")
setMethod("mean", signature(x="scidb"),
function(x)
{
  iquery(sprintf("avg(%s)",x@name),return=TRUE)[,2]
})

setGeneric("min")
setMethod("min", signature(x="scidb"),
function(x)
{
  iquery(sprintf("min(%s)",x@name),return=TRUE)[,2]
})

setGeneric("max")
setMethod("max", signature(x="scidb"),
function(x)
{
  iquery(sprintf("max(%s)",x@name),return=TRUE)[,2]
})

setGeneric("sd")
setMethod("sd", signature(x="scidb"),
function(x)
{
  iquery(sprintf("stdev(%s)",x@name),return=TRUE)[,2]
})

setGeneric("var")
setMethod("var", signature(x="scidb"),
function(x)
{
  iquery(sprintf("var(%s)",x@name),return=TRUE)[,2]
})

setGeneric("diag")
setMethod("diag", signature(x="scidb"),
function(x)
{
  if(length(dim(x))!=2) stop("diag requires a matrix argument")
  ans = tmpnam()
  name = make.names_(c(x@attribute,"diag"))[2]
  schema = extract_schema(x,x@attribute,x@type,x@nullable[x@attributes %in% x@attribute])
  query  = sprintf("build_sparse(%s,1,%s=%s)",schema,x@D$name[1],x@D$name[2])
  query  = sprintf("join(%s as _A1,%s as _A2)",x@name,query)
  query  = sprintf("project(unpack(%s,%s),_A1.%s)",query,name,x@attribute)
  query  = sprintf("subarray(%s,0,%.0f)",query, min(x@D$length)-1)
  query  = sprintf("store(%s,%s)",query,ans)
  iquery(query)
  scidb(ans)
})

setGeneric("head")
setMethod("head", signature(x="scidb"),
function(x, n=6L, ...)
{
  m = x@D$start
  p = m + n - 1
  limits = lapply(1:length(m), function(j) seq(m[j],p[j]))
  do.call(scidb:::dimfilter,args=list(x=x,i=limits))[]
})

setGeneric("tail")
setMethod("tail", signature(x="scidb"),
function(x, n=6L, ...)
{
  p = x@D$start + x@D$length - 1
  m = x@D$start + x@D$length - n
  m = unlist(lapply(1:length(m),function(j) max(m[j],x@D$start[j])))
  limits = lapply(1:length(m), function(j) seq(m[j],p[j]))
  do.call(scidb:::dimfilter,args=list(x=x,i=limits))[]
})


setGeneric('is.scidb', function(x) standardGeneric('is.scidb'))
setMethod('is.scidb', signature(x='ANY'),
  function(x) 
  {
    if(inherits(x, "scidb")) return(TRUE)
    FALSE
  }
)
#setMethod('is.scidb', definition=function(x) return(FALSE))

setGeneric('print', function(x) standardGeneric('print'))
setMethod('print', signature(x='scidb'),
  function(x) {
    cat("A reference to a ",paste(nrow(x),ncol(x),sep="x"),
        "SciDB array\n")
  })

setMethod('show', 'scidb',
  function(object) {
    atr=object@attribute
    if(is.null(dim(object)) || length(dim(object))==1)
      cat("Reference to a SciDB vector of length",object@length,"\n")
    else
      cat("A reference to a ",
          paste(object@dim,collapse="x"),
          "SciDB array\n")
  })

setGeneric("image", function(x,...) x)
setMethod("image", signature(x="scidb"),
function(x, grid=c(x@D$chunk_interval[1], x@D$chunk_interval[2]), op=sprintf("sum(%s)",x@attribute), na=0, ...)
{
  if(length(dim(x))!=2) stop("Sorry, array must be two-dimensional")
  if(length(grid)!=2) stop("The grid parameter must contain two values")
  blocks = c(x@D$high[1] - x@D$low[1] + 1, x@D$high[2] - x@D$low[2] + 1)
  blocks = blocks/grid
  query = sprintf("regrid(project(%s,%s),%.0f,%.0f,%s)",x@name,x@attribute,blocks[1],blocks[2],op)
  A = iquery(query,return=TRUE,n=Inf)
  A[is.na(A[,3]),3] = na
  m = max(A[,1]) + 1
  n = max(A[,2]) + 1
  B = matrix(0,m,n)
  B[A[,1] + A[,2]*m + 1] = A[,3]
  xlbl=(1:ncol(B))*blocks[2]
  xat=seq(from=0,to=1,length.out=ncol(B))
  ylbl=(nrow(B):1)*blocks[1]
  yat=seq(from=0,to=1,length.out=nrow(B))
  image(B,xaxt='n',yaxt='n',...)
  axis(side=1,at=xat,labels=xlbl)
  axis(side=2,at=yat,labels=ylbl)
  B
})

# x:   A SciDB array
# by:  A list of dimension and/or attribute names in x to aggregate along
# FUN: A valid SciDB aggregation expression (string)
setOldClass("aggregate")
setGeneric("aggregate")
setMethod("aggregate", signature(x="scidb"), aggregate_scidb)

setOldClass("sweep")
setGeneric("sweep")
setMethod("sweep", signature(x="scidb"), sweep_scidb)

svd_scidb = function(x)
{
  u = sprintf("%s_U",x@name)
  d = sprintf("%s_S",x@name)
  v = sprintf("%s_V",x@name)
  iquery(sprintf("store(gesvd(%s,'left'),%s)",x@name,u))
  iquery(sprintf("store(gesvd(%s,'values'),%s)",x@name,d))
  iquery(sprintf("store(gesvd(%s,'right'),%s)",x@name,v))
  list(u=scidb(u,gc=TRUE),d=scidb(d,gc=TRUE),v=scidb(v,gc=TRUE))
}

setOldClass("svd")
setGeneric("svd")
setMethod("svd", signature(x="scidb"), svd_scidb)
