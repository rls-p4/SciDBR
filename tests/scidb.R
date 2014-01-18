# Basic unit tests for the scidb matrix object appear here.  The tests require
# a local SciDB database connection. If a connection is not available, tests
# are passed.

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)
  scidblist()
# Dense matrix tests
  set.seed(1)
  A = matrix(rnorm(50*40),50)
  B = matrix(rnorm(40*40),40)
  X = as.scidb(A, chunkSize=c(30,40))
  Y = as.scidb(B, chunkSize=c(17,20))
# Matrix multiplication
  stopifnot(all.equal(A%*%B, (X %*% Y)[],check.attributes=FALSE))
# Transpose
  stopifnot(all.equal(crossprod(A),(t(X) %*% X)[], check.attributes=FALSE))
# Crossprod
  stopifnot(all.equal(crossprod(A),(crossprod(X))[], check.attributes=FALSE))
# Arithmetic on mixed R/SciDB objects
  x = rnorm(40);
  stopifnot(all.equal((X %*% x)[,drop=FALSE], A %*% x, check.attributes=FALSE))
# Scalar multiplication
  stopifnot(all.equal(2*A, (2*X)[],check.attributes=FALSE))


# Databasey ops
  data("iris")
  x = as.scidb(iris)
# Aggregation by a non-integer attribute
  stopifnot(all.equal(aggregate(iris$Petal.Length,by=list(iris$Species),FUN=mean)[,2],
                aggregate(project(x,c('Petal_Length','Species')), by = 'Species', FUN='avg(Petal_Length)')[][,2]))

# Please write more tests following this pattern...
}
