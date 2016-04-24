# Miscellaneous tests

check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a,b,check.attributes=FALSE,check.names=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)

# Upload/download binary blobs
  data("iris")
  x = as.scidb(serialize(iris, NULL))
  y = unserialize(x[][[2]][[1]])
  stopifnot(isTRUE(all.equal(iris, y)))

# Misc.
  x = as.scidb(iris)
  check(nrow(unique(project(x, "Species"))[]), 3)
  check( nrow(xgrid(regrid(x, 10, "avg(Petal_Length) as Petal_Length"), 10, "max(Petal_Length)")),
         150)

# issue 81
  x = scidb_from_schemastring("<v:double>[i=1:2,1,0],1")
  check(is.scidb(x), TRUE)

# issue 90 (new stuff from Alex)
# Note! We don't test the prototypes (limit, grouped_aggregate)
  x = scidb("build(<v:double>[i=0:999,500,0,j=0:999,500,0], sin(i-j))")
  y = gemm(x, x, x)
  xd = x[]
  X = matrix(nrow=1000, ncol=1000)
  X[as.matrix(xd[,1:2] + 1)] = xd[,3]
  yd = y[]
  Y = matrix(nrow=1000, ncol=1000)
  Y[as.matrix(yd[,1:2] + 1)] = yd[,3]
  check(X %*% X + X, Y)

  s = gesvd(x, "values")[]
  check(s$sigma, svd(X)$d)
}
