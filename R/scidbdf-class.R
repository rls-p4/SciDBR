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

setClass("scidbdf",
         representation(call="call",
                        name="character",
                        D="list",
                        dim="numericOrNULL",
                        length="numeric",
                        attributes="character",
                        nullable="logical",
                        types="character",
                        colClasses="character",
                        gc="environment"),
         S3methods=TRUE)

setGeneric("head")
setMethod("head", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start,x@D$start + n - 1),`return`=TRUE,colClasses=x@colClasses)[,-1]
})

setGeneric("tail")
setMethod("tail", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start + x@D$length - n - 1,x@D$start + x@D$length-1),`return`=TRUE, colClasses=x@colClasses)[,-1]
})

setGeneric('is.scidbdf', function(x) standardGeneric('is.scidbdf'))
setMethod('is.scidbdf', signature(x='scidbdf'),
  function(x) return(TRUE))
setMethod('is.scidbdf', definition=function(x) return(FALSE))

setGeneric('print', function(x) standardGeneric('print'))
setMethod('print', signature(x='scidbdf'),
  function(x) {
    l = length(x@attributes)
    v = ifelse(l<2, "variable", "variables")
    cat(sprintf("SciDB array %s: %.0f obs. of %d %s.\n",x@name, (x@D$high - x@D$low + 1), length(x@attributes), v))
  })

setMethod('show', 'scidbdf',
  function(object) {
    l = length(object@attributes)
    v = ifelse(l<2, "variable", "variables")
    cat(sprintf("SciDB array %s: %.0f obs. of %d %s.\n",object@name, (object@D$high - object@D$low + 1), length(object@attributes),v))
  })

setMethod("aggregate", signature(x="scidbdf"), aggregate_scidb)
