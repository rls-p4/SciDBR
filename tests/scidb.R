# 'scidb' object tests

check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a,b,check.attributes=FALSE,check.names=FALSE))
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
  lapply(1:4,  check(all.equal(iris[,j], a[,j])))  # less factor column
  check(all.equal(as.character(iris[,5]), a[,5]))  # character column
# iquery binary download
  a = iquery(x, return=TRUE, binary=TRUE)[, -1]
  lapply(1:4,  check(all.equal(iris[,j], a[,j])))  # less factor column
  check(all.equal(as.character(iris[,5]), a[,5]))  # character column
# iquery CSV download
  a = iquery(x, return=TRUE, binary=FALSE)[, -1]
  lapply(1:4,  check(all.equal(iris[,j], a[,j])))  # less factor column
  check(all.equal(as.character(iris[,5]), a[,5]))  # character column

}
