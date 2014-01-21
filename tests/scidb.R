# scidb array object tests
# Set the SCIDB_TEST_HOST system environment variable to the hostname or IP
# address of SciDB to run these tests. If SCIDB_TEST_HOST is not set the tests
# are not run.

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)
# Upload dense matrix to SciDB
  set.seed(1)
  A = matrix(rnorm(50*40),50)
  B = matrix(rnorm(40*40),40)
  X = as.scidb(A, chunkSize=c(30,40))
  Y = as.scidb(B, chunkSize=c(17,20))
  stopifnot(all.equal(X[],A,check.attributes=FALSE))
  stopifnot(all.equal(Y[],B,check.attributes=FALSE))

# Dense matrix multiplication
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

# Numeric array subsetting
  stopifnot(all.equal((X %*% X[0,,drop=TRUE])[,drop=FALSE], A %*% A[1,], check.attributes=FALSE))
  stopifnot(all.equal(X[c(5,15,1),c(25,12,11)][], A[c(6,16,2),c(26,13,12)],check.attributes=FALSE))
  stopifnot(all.equal(diag(Y)[], diag(B),check.attributes=FALSE))

# Aggregation
  stopifnot(all.equal(
            sweep(B,MARGIN=2,apply(B,2,mean)),
            sweep(Y,MARGIN=2,apply(Y,2,"avg(val)"))[], check.attributes=FALSE))




# Databasey ops
  data("iris")
  x = as.scidb(iris)
# Aggregation by a non-integer attribute
  stopifnot(all.equal(aggregate(iris$Petal.Length,by=list(iris$Species),FUN=mean)[,2],
                aggregate(project(x,c('Petal_Length','Species')), by = 'Species', FUN='avg(Petal_Length)')[][,2]))

# Please write more tests following this pattern...
}
