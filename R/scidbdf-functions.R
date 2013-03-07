#/*
#**
#* BEGIN_COPYRIGHT
#*
#* Copyright (C) 2008-2012 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify it under the
#* terms of the GNU General Public License as published by the Free Software
#* Foundation version 3 of the License.
#*
#* This software is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY, NON-INFRINGEMENT, OR
#* FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for the
#* complete license terms.
#*
#* You should have received a copy of the GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
#*
#* END_COPYRIGHT
#*/

colnames.scidbdf = function(x)
{
  x@D$attributes
}

rownames.scidbdf = function(x)
{
  if(x@D$type[1] != "string") return(c(x@D$start[1],x@D$start[1]+x@D$length[1]-1))
  if(x@D$length[1] > options("scidb.max.array.elements"))
    stop("Result will be too big. Perhaps try a manual query with an iterative result.")
  Q = sprintf("scan(%s:%s)",x@name,x@D$name[1])
  iquery(Q,return=TRUE,n=x@D$length[1]+1)[,2]
}

names.scidbdf = function(x)
{
  colnamesdf(x)
}

dimnames.scidbdf = function(x)
{
  list(rownames.scidbdf(x), colnames.scidbdf(x))
}

# Flexible array subsetting wrapper.
# x: A Scidbdf array object
# ...: list of dimensions
# iterative: return a data.frame iterator
# 
`[.scidbdf` = function(x, ..., iterative=FALSE)
{
  M = match.call()
  drop = ifelse(is.null(M$drop),TRUE,M$drop)
  M = M[3:length(M)]
  if(!is.null(names(M))) M = M[!(names(M) %in% c("drop","iterative"))]
# i shall contain a list of requested index values
  i = lapply(1:length(M), function(j) tryCatch(eval(M[j][[1]],parent.frame()),error=function(e)c()))
# User wants this materialized to R...
  if(all(sapply(i,is.null)))
    if(iterative)
     stop("CAZART\n")
    else
      return(iquery(sprintf("scan(%s)",x@name),ret=TRUE,n=x@D$length+1))
# Not materializing, return a SciDB array
  if(length(i)!=length(dim(x))) stop("Dimension mismatch")
stop("CAZART\n")
}

`dim.scidbdf` = function(x)
{
  if(length(x@dim)==0) return(NULL)
  x@dim
}

`dim<-.scidbdf` = function(x, value)
{
  stop("unsupported")
}


`str.scidbdf` = function(object, ...)
{
  cat("SciDB array name: ",object@name)
  cat("\nAttributes: ",object@attributes)
  cat("\nRow dimension:\n")
  cat(paste(capture.output(print(data.frame(object@D))),collapse="\n"))
  cat("\n")
}

`ncol.scidbdf` = function(x) x@dim[2]
`nrow.scidbdf` = function(x) x@dim[1]
`dim.scidbdf` = function(x) {if(length(x@dim)>0) return(x@dim); NULL}
`length.scidbdf` = function(x) x@length
