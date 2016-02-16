# 'scidb' object tests

check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a, b, check.attributes=FALSE, check.names=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host) > 0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)

# upload
  x = as.scidb(iris)
# binary download
  a = x[][,-1]  # less SciDB row dimension
  lapply(1:4,  function(j) check(all.equal(iris[,j], a[,j]), TRUE))  # less factor column
  check(all.equal(as.character(iris[,5]), a[,5]), TRUE)  # character column
# iquery binary download
  a = iquery(x, return=TRUE, binary=TRUE)[, -1]
  lapply(1:4,  function(j) check(all.equal(iris[,j], a[,j]), TRUE))  # less factor column
  check(all.equal(as.character(iris[,5]), a[,5]), TRUE)  # character column
# iquery CSV download
  a = iquery(x, return=TRUE, binary=FALSE)[, -1]
  lapply(1:4,  function(j) check(all.equal(iris[,j], a[,j]), TRUE))  # less factor column
  check(all.equal(as.character(iris[,5]), a[,5]), TRUE)  # character column

# head
  a = head(x, 3)
  b = iqdf(x, 3, 1)
  c = iqdf(x, 10, 0.25)
  
# garbage collection
  rm(x)
  gc()

# misc schema-modifying ops
  x = as.scidb(iris)
  check(is.scidb(x), TRUE)
  check(length(dim(x)), 2)
  check(nrow(x), 150)
  y = cast(x, schema(x))
  redimension(x, schema(x))
  y = transform(x, i="iif(Species='setosa', 1, iif(Species='versicolor', 2, 3))")
  z = redimension(y, dim=c("row", "i"))
  check(length(dimensions(z)), 2)

  repart(x, schema(x))
  repart(x, schema(x), '*', 2, 1) 
  reshape(x, schema(x))
  rehsape(x, shape=150)

  unbound(x)
  replaceNA(x)

# join
  y = merge(x, x)
  check(ncol(y), 11)
  y = merge(x, x, merge=TRUE)
  check(ncol(y), 6)
  y = merge(x, z)
  check(ncol(y), 12)

}
