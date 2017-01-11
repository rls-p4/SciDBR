check = function(a, b)
{
  print(match.call())
  stopifnot(all.equal(a, b, check.attributes=FALSE, check.names=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host) > 0)
{
  db = scidbconnect(host)
  options(scidb.debug=TRUE)

# 1 Data movement tests

# upload data frame
  x = as.scidb(db, iris)
# binary download
  check(iris[, 1:4], as.R(x)[, -1][, 1:4])
# iquery binary download
  check(iris[, 1:4], iquery(db, x, return=TRUE)[, -1][, 1:4])
# iquery CSV download
  check(iris[, 1:4], iquery(db, x, return=TRUE, binary=FALSE)[, -1][, 1:4])
# as.R attributes only
  check(as.R(x)[,2],  as.R(x, attributes_only=TRUE)[,1])

# upload vector
  check(1:5, as.R(as.scidb(db, 1:5))[,2])
# upload matrix
  x = matrix(rnorm(100), 10)
  check(x, matrix(as.R(as.scidb(db, x))[,3], 10, byrow=TRUE))
# upload csparse matrix
  x = Matrix::sparseMatrix(i=sample(10, 10), j=sample(10, 10),x=runif(10))
  y = as.R(as.scidb(db, x))
  check(x, Matrix::sparseMatrix(i=y$i + 1, j=y$j + 1, x=y$val))
# issue #126
  df = as.data.frame(matrix(runif(10*100), 10, 100))
  sdf = as.scidb(db, df)
  check(df, as.R(sdf)[, -1])

# upload n-d array
# XXX WRITE ME, not implemented yet

# garbage collection
  gc()


# 2 AFL tests

# Issue #128
 i = 4
 j = 6
 x = db$build("<v:double>[i=1:2,2,0, j=1:3,1,0]", i * j)
 check(as.R(x)$v, c(1, 2, 2, 4, 3, 6))
 x = db$apply(x, w, R(i) * R(j))
 check(as.R(x)$w, rep(24, 6))


# 3 Miscellaneous tests

}
