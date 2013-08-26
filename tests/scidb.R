# Basic unit tests for the scidb matrix object appear here.  The tests require
# a local SciDB database connection. If a connection is not available, tests
# are passed.

library("scidb")
OK = tryCatch(is.null(scidbconnect()), error=function(e) FALSE)

# expr must be a character string representing an expression that returns
# true upon success. If the expression throws an error, FALSE is returned.
test = function(expr)
{
  if(!OK) return(TRUE)  # SciDB is not available, pass.
  tryCatch(eval(parse(text=expr)), error=function(e) stop(e))
}

options(scidb.debug=TRUE)
test("scidblist(); TRUE")

# Dense matrix tests
set.seed(1)
A = matrix(rnorm(50*40),50)
B = matrix(rnorm(40*40),40)
X = as.scidb(A,rowChunkSize=3,colChunkSize=19)
Y = as.scidb(B)
# Matrix multiplication
test("isTRUE(all.equal(A %*% B, (X %*% Y)[],check.attributes=FALSE))")
# Transpose
test("isTRUE(all.equal(crossprod(A),(t(X) %*% X)[], check.attributes=FALSE))")
# Crossprod
test("isTRUE(all.equal(crossprod(A),(crossprod(X))[], check.attributes=FALSE))")
# Mixed arithmetic
test("x=rnorm(40);isTRUE(all.equal((X %*% x)[,drop=FALSE], A %*% x, check.attributes=FALSE))")
# Scalar multiplication
test("isTRUE(all.equal(2*A, (2*X)[],check.attributes=FALSE))")
# Please write more tests following this pattern...



# dbops
data("iris")
x = as.scidb(iris)
# Aggregation by a non-integer attribute
test("isTRUE(all.equal(aggregate(iris$Petal.Length,by=list(iris$Species),FUN=mean)[,2],
                aggregate(project(x,c('Petal_Length','Species')), by = 'Species', FUN='avg(Petal_Length)')[][,2]))")
# Sort
test("isTRUE(all.equal(project(sort(x,attributes='Petal_Length'),'Petal_Length')[][,1],sort(iris$Petal.Length)))")
# Please write more tests following this pattern...

# Cleanup
rm(list=ls())
gc()
