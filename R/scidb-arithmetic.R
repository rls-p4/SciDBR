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
# Element-wise operations
Ops.scidb = function(e1,e2) {
  switch(.Generic,
    '^' = .binop(e1,e2,"^"),
    '+' = .binop(e1,e2,"+"),
    '-' = .binop(e1,e2,"-"),
    '*' = .binop(e1,e2,"*"),
    '/' = .binop(e1,e2,"/"),
    '<' = .binop(e1,e2,"<"),
    '<=' = .binop(e1,e2,"<="),
    '>' = .binop(e1,e2,">"),
    '>=' = .binop(e1,e2,">="),
    '==' = .binop(e1,e2,"="),
    '!=' = .binop(e1,e2,"<>"),
    default = stop("Unsupported binary operation.")
  )
}

# e1 and e2 must each already be SciDB arrays.
scidbmultiply = function(e1,e2)
{
# Check for availability of spgemm
  P4 = length(grep("spgemm",scidb:::.scidbenv$ops[,2]))>0
  SPARSE = FALSE

# XXX Handle this differently. Perhaps a manual flag instead?
# XXX For now, sparse is disabled.
#  if(P4)
#  {
#    e1_count = count(e1)
#    e2_count = count(e2)
#    if(e1_count < prod(dim(e1)) || e2_count < prod(dim(e2)))
#      SPARSE = TRUE
#  }

  a1 = e1@attribute
  a2 = e2@attribute
  op1 = e1@name
  op2 = e2@name
  if(length(e1@attributes)>1)
    op1 = sprintf("project(%s,%s)",e1@name,a1)
  if(length(e2@attributes)>1)
    op2 = sprintf("project(%s,%s)",e2@name,a2)

# We use subarray to handle starting index mismatches (subarray
# returns an array with dimension indices starting at zero).
  l1 = length(dim(e1))
  lb = paste(rep("null",l1),collapse=",")
  ub = paste(rep("null",l1),collapse=",")
  op1 = sprintf("subarray(%s,%s,%s)",op1,lb,ub)
  l2 = length(dim(e2))
  lb = paste(rep("null",l2),collapse=",")
  ub = paste(rep("null",l2),collapse=",")
  op2 = sprintf("subarray(%s,%s,%s)",op2,lb,ub)

#  e1@D$chunk_interval[[2]], e1@D$chunk_overlap[[2]],

  dnames = make.names_(c(e1@D$name[[1]],e2@D$name[[2]]))

  CHUNK_SIZE = options("scidb.gemm_chunk_size")[[1]]

  if(!SPARSE)
  {
# Adjust the arrays to conform to GEMM requirements
    op1 = sprintf("repart(%s,<%s:%s>[%s=0:%.0f,%d,0,%s=0:%.0f,%d,0])",op1,a1,e1@type[1],e1@D$name[[1]],e1@D$length[[1]]-1,CHUNK_SIZE,e1@D$name[[2]],e1@D$length[[2]]-1,CHUNK_SIZE)
    op2 = sprintf("repart(%s,<%s:%s>[%s=0:%.0f,%d,0,%s=0:%.0f,%d,0])",op2,a2,e2@type[1],e2@D$name[[1]],e2@D$length[[1]]-1,CHUNK_SIZE,e2@D$name[[2]],e2@D$length[[2]]-1,CHUNK_SIZE)
    osc = sprintf("<%s:%s>[%s=0:%.0f,%d,0,%s=0:%.0f,%d,0]",a1,e1@type[1],dnames[[1]],e1@D$length[[1]]-1,CHUNK_SIZE,dnames[[2]],e2@D$length[[2]]-1,CHUNK_SIZE)
    op3 = sprintf("build(%s,0)",osc)
  }

# Decide which multiplication algorithm to use
  if(SPARSE && !P4)
    query = sprintf("multiply(%s, %s)", op1, op2)
  else if (SPARSE && P4)
    query = sprintf("spgemm(%s, %s)", op1, op2)
  else
    query = sprintf("gemm(%s, %s, %s)",op1,op2,op3)

# Repartition the output back to conform with inputs
#  schema = sprintf(
#           "<gemm:double>[%s=0:%.0f,%.0f,%.0f, %s=0:%.0f,%.0f,%.0f]",
#            dnames[[1]],
#            e1@D$length[[1]]-1,
#            e1@D$chunk_interval[[1]],
#            e1@D$chunk_overlap[[1]],
#            dnames[[2]],
#            e2@D$length[[2]]-1,
#            e2@D$chunk_interval[[2]],
#            e2@D$chunk_overlap[[2]])
#  query = sprintf("cast(repart(%s,%s),%s)",query,schema,schema)
  query = sprintf("cast(%s,%s)",query,osc)
  .scidbeval(query,gc=TRUE,eval=FALSE,depend=list(e1,e2))
}

# Element-wise binary operations
.binop = function(e1,e2,op)
{
  e1s = e1
  e2s = e2
  e1a = "scalar"
  e2a = "scalar"
  depend = c()
# Check for non-scidb object arguments and convert to scidb
  if(!inherits(e1,"scidb") && length(e1)>1) {
    x = tmpnam()
    e1 = as.scidb(e1,name=x,gc=TRUE)
  }
  if(!inherits(e2,"scidb") && length(2)>1) {
    x = tmpnam()
    e2 = as.scidb(e2,name=x,gc=TRUE)
  }
# We evaluate SciDB arguments because binary aritmetic operations require
# an outer join to handle sparse arrays, and the outer join will end up
# potentially replicating work unnecessarily.
  if(inherits(e1,"scidb"))
  {
    e1 = scidbeval(e1,gc=TRUE)
    e1a = e1@attribute
    depend = c(depend, e1)
  }
  if(inherits(e2,"scidb"))
  {
    e2 = scidbeval(e2,gc=TRUE)
    e2a = e2@attribute
    depend = c(depend, e2)
  }
# OK, we've got two scidb arrays, op them. v holds the new attribute name.
  v = make.unique_(e1a, "v")

# We use subarray to handle starting index mismatches...
  q1 = q2 = ""
  l1 = length(dim(e1))
  lb = paste(rep("null",l1),collapse=",")
  ub = paste(rep("null",l1),collapse=",")
  if(inherits(e1,"scidb"))
  {
    q1 = sprintf("subarray(project(%s,%s),%s,%s)",e1@name,e1@attribute,lb,ub)
  }
  l = length(dim(e2))
  lb = paste(rep("null",l),collapse=",")
  ub = paste(rep("null",l),collapse=",")
  if(inherits(e2,"scidb"))
  {
    q2 = sprintf("subarray(project(%s,%s),%s,%s)",e2@name,e2@attribute,lb,ub)
  }
# Adjust the 2nd array to be schema-compatible with the 1st:
  if(l==2 && l1==2)
  {
    schema = sprintf(
       "<%s:%s>[%s=%.0f:%.0f,%.0f,%.0f,%s=%.0f:%.0f,%.0f,%.0f]",
       e2a, e2@type[[1]],
       e2@D$name[[1]], 0, e2@D$length[[1]] - 1,
                          e1@D$chunk_interval[[1]], e1@D$chunk_overlap[[1]],
       e2@D$name[[2]], 0, e2@D$length[[2]] - 1,
                          e1@D$chunk_interval[[2]], e1@D$chunk_overlap[[2]])
    q2 = sprintf("repart(%s, %s)", q2, schema)

# Handle sparsity by cross-merging data (full outer join):
    q1 = sprintf("merge(%s,project(apply(%s,__zero__,%s(0)),__zero__))",q1,q2,e1@type)
    q2 = sprintf("merge(%s,project(apply(%s,__zero__,%s(0)),__zero__))",q2,q1,e2@type)
  }
  p1 = p2 = ""
# Syntax sugar for exponetiation (map the ^ infix operator to pow):
  if(op=="^")
  {
    p1 = "pow("
    op = ","
    p2 = ")"
  }
# Handle special scalar multiplication case:
  if(length(e1s)==1)
    Q = sprintf("apply(%s,%s, %s %.15f %s %s %s)",q2,v,p1,e1s,op,e2a,p2)
  else if(length(e2s)==1)
    Q = sprintf("apply(%s,%s,%s %s %s %.15f %s)",q1,v,p1,e1a,op,e2s,p2)
  else
  {
    Q = sprintf("join(%s as e1, %s as e2)", q1, q2)
    Q = sprintf("apply(%s, %s, %s e1.%s %s e2.%s %s)", Q,v,p1,e1a,op,e2a,p2)
  }
  Q = sprintf("project(%s, %s)",Q,v)
  .scidbeval(Q, eval=FALSE, gc=TRUE, depend=depend)
}

# Very basic comparisons. See also filter.
# e1: A scidb array
# e2: A scalar or a scidb array. If a scidb array, the return .joincompare(e1,e2,op) (q.v.)
# op: A comparison infix operator character
#
# Return a scidb object
# Can throw a query error.
.compare = function(e1,e2,op)
{
  if(!inherits(e1,"scidb")) stop("Sorry, not yet implemented.")
  if(inherits(e2,"scidb")) return(.joincompare(e1,e2,op))
  type = names(.scidbtypes[.scidbtypes==e1@type])
  if(length(type)<1) stop("Unsupported data type.")
  op = gsub("==","=",op,perl=TRUE)
  tval = vector(mode=type,length=1)
  query = sprintf("filter(%s, %s %s %s)",e1@name, e1@attribute, op, e2)
  .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(e1))
}

.joincompare = function(e1,e2,op)
{
  stop("Yikes! Not implemented yet...")
}

tsvd = function(x,nu)
{
  m = ceiling(nrow(x)/1e6)
  n = ceiling(ncol(x)/1e6)
  schema = sprintf("[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0]",
                     x@D$name[1], nrow(x)-1, m,
                     x@D$name[2], ncol(x)-1, ncol(x))
  tschema = sprintf("[%s=0:%.0f,%.0f,0,%s=0:%.0f,%.0f,0]",
                     x@D$name[2], ncol(x)-1, n,
                     x@D$name[1], nrow(x)-1, nrow(x))
  schema = sprintf("%s%s",scidb:::build_attr_schema(x), schema)
  tschema = sprintf("%s%s",scidb:::build_attr_schema(x), tschema)
  query  = sprintf("tsvd(redimension(unpack(%s,row),%s), redimension(unpack(transpose(%s),row),%s), %d, 0.001, 20)", x@name, schema, x@name, tschema, nu)
  narray = scidb:::.scidbeval(query, eval=TRUE, gc=TRUE)
  ans = list(u=slice(narray, "matrix", 0,eval=FALSE),
             d=slice(narray, "matrix", 1,eval=FALSE),
             v=slice(narray, "matrix", 2,eval=FALSE), narray)
  ans
}
