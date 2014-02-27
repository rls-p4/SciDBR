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

setMethod("head", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start,x@D$start + n - 1),`return`=TRUE,colClasses=scidbdfcc(x))[,-1]
})

setMethod("tail", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iquery(sprintf("between(%s,%.0f,%.0f)",x@name,x@D$start + x@D$length - n - 1,x@D$start + x@D$length-1),`return`=TRUE, colClasses=scidbdfcc(x))[,-1]
})

setMethod("Filter",signature(f="character",x="scidbdf"),
  function(f, x)
  {
    filter_scidb(x,f)
  })

setGeneric('is.scidbdf', function(x) standardGeneric('is.scidbdf'))
setMethod('is.scidbdf', signature(x='scidbdf'),
  function(x) return(TRUE))
setMethod('is.scidbdf', definition=function(x) return(FALSE))

setMethod('print', signature(x='scidbdf'),
  function(x) {
    show(x)
  })

setMethod("na.locf",signature(object="scidbdf"), na.locf_scidb)
setMethod("hist",signature(x="scidbdf"), hist_scidb)

setMethod('show', 'scidbdf',
  function(object) {
    l = length(object@attributes)
    v = ifelse(l<2, "variable", "variables")
    cat(sprintf("SciDB 1-D array: %.0f obs. of %d %s.\n", (object@D$length), length(object@attributes),v))
  })

setMethod("aggregate", signature(x="scidbdf"), aggregate_scidb)

scidbdf_grand = function(x, op)
{
  query = sprintf("aggregate(%s, %s(%s) as %s)", x@name, op, x@attributes[1], x@attributes[1])
  iquery(query, `return`=TRUE)[,2]
}

# The following methods return data to R
setMethod("sum", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "sum")
})

setMethod("median", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "median")
})

setMethod("mean", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "avg")
})

setMethod("min", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "min")
})

setMethod("max", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "max")
})

setMethod("sd", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "stdev")
})

setMethod("var", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "var")
})

log.scidbdf = function(x, base=exp(1))
{
  log_scidb(x,base) 
}

setMethod("sin", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "sin")
  })
setMethod("cos", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "cos")
  })
setMethod("tan", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "tan")
  })
setMethod("asin", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "asin")
  })
setMethod("acos", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "acos")
  })
setMethod("atan", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "atan")
  })
setMethod("abs", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "abs")
  })
# Non-traditional masking binary comparison operators
setMethod("%<%",signature(x="scidbdf", y="ANY"),
  function(x,y)
  {
    .compare(x,y,"<",traditional=FALSE)
  },
  valueClass="scidbdf"
)
setMethod("%>%",signature(x="scidbdf", y="ANY"),
  function(x,y)
  {
    .compare(x,y,">",traditional=FALSE)
  },
  valueClass="scidbdf"
)
setMethod("%<=%",signature(x="scidbdf", y="ANY"),
  function(x,y)
  {
    .compare(x,y,"<=",traditional=FALSE)
  },
  valueClass="scidbdf"
)
setMethod("%>=%",signature(x="scidbdf", y="ANY"),
  function(x,y)
  {
    .compare(x,y,">=",traditional=FALSE)
  },
  valueClass="scidbdf"
)
setMethod("%==%",signature(x="scidbdf", y="ANY"),
  function(x,y)
  {
    .compare(x,y,"==",traditional=FALSE)
  },
  valueClass="scidbdf"
)
