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

# cf glm.fit
glm_scidb = function(x,y,`weights`=NULL,`family`=gaussian())
{
  nobs = length(y)
  got_glm = length(grep("glm",.scidbenv$ops[,2]))>0
  xchunks = as.numeric(scidb_coordinate_chunksize(x))
  if(missing(`weights`)) `weights`=NULL
  if(is.numeric(`weights`))
  {
    `weights` = as.scidb(as.double(weights),chunkSize=xchunks[1])
  } else
  {
    weights = build(1.0,nrow(x),start=as.numeric(scidb_coordinate_start(x)[1]),chunksize=xchunks[1])
  }
  if(!is.scidb(y))
  {
    y = as.scidb(y)
  }
  if(!got_glm)
  {
    stop("The Paradigm4 glm operator was not found.")
  }
  x = substitute(x)
  y = substitute(y)
  `weights` = substitute(`weights`)
  dist = family$family
  link = family$link
# GLM has a some data partitioning requirements to look out for:
  if(xchunks[2]<dim(x)[2])
  {
    x = repart(x,chunk=c(xchunks[1],dim(x)[2]))
  }
  xchunks = as.numeric(scidb_coordinate_chunksize(x))
  ychunks = as.numeric(scidb_coordinate_chunksize(y))
  if((ychunks[1] != xchunks[1]) )
  {
    y = repart(y, chunk=xchunks[1])
  }
  query = sprintf("glm(%s,%s,%s,'%s','%s')",
           x@name, y@name, weights@name, dist, link)
  M = .scidbeval(query,eval=TRUE,gc=TRUE)
  m1 = M[,0][] # Cache 1st column
  ans = list(
    coefficients = M[0,],
    stderr = M[1,],
    tval = M[2,],
    pval = M[3,],
    aic = m1[12],
    null.deviance = m1[13],
    res.deviance = m1[15],
    dispersion = m1[5],
    df.null = m1[6],
    df.residual = m1[7],
    converged = m1[10]==1,
    totalObs = m1[8],
    nOK = m1[9],
    loglik = m1[14],
    rss = m1[16],
    iter = m1[18],
    weights = weights,
    family = family,
    y = y,
    x = x
  )
  ans
}
