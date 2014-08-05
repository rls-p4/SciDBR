# scidb array object tests
# Set the SCIDB_TEST_HOST system environment variable to the hostname or IP
# address of SciDB to run these tests. If SCIDB_TEST_HOST is not set the tests
# are not run.

check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a,b,check.attributes=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)
  oplist = scidbls(type="operators")
  got = function(op)
  {
    length(grep(op,oplist))>0
  }

# Upload dense matrix to SciDB
  set.seed(1)
  A = matrix(rnorm(50*40),50)
  B = matrix(rnorm(40*40),40)
  X = as.scidb(A, chunkSize=c(30,40), start=c(1,0))
  Y = as.scidb(B, chunkSize=c(17,20), star=c(2,3))
  check(X[],A)
  check(Y[],B)

# n-d array
  x = build("k",dim=c(3,3,3),names=c("x","i","j","k"), type="double")
  check(x[][,,2] , matrix(1,3,3))

# Dense matrix multiplication
  check(A%*%B, (X%*%Y)[])
# Transpose
  check(crossprod(A), (t(X)%*%X)[])
# Crossprod, tcrossprod
  check(crossprod(A), crossprod(X)[])
  check(tcrossprod(B), tcrossprod(B)[])
# Arithmetic on mixed R/SciDB objects
  x = rnorm(40);
  check((X%*%x)[,drop=FALSE], A%*%x)
# Scalar multiplication
  check(2*A, (2*X)[])
# Elementwise addition
  check(A+A, (X+X)[])
# SVD
  check(svd(A)$d, as.vector(svd(X)$d[]))

# Numeric array subsetting
  check((X %*% X[1,,drop=TRUE])[,drop=FALSE], A %*% A[1,])
  check(X[c(6,16,2),c(25,12,11)][], A[c(6,16,2),c(26,13,12)])
  check(as.vector(diag(Y)[]), diag(B))

# Filtering
  W = subset(X,"val>0")
# Sparse elementwise addition and scalar multiplication
  D = (W + 2*W)[]
  w = W[]
  w = w + 2*w
  check(sum(D-w), 0)

# some binary operations
# XXX ADD **lots** more tests here
  a = as.scidb(Matrix::sparseMatrix(
               sample(10,100,replace=TRUE),sample(10,100,replace=TRUE),x=runif(100)))
  b = build(pi,c(10,10),names=c("a","b","c"),chunksize=c(3,2))
  check(sum((a*b)[] - a[]*b[]), 0)
  check(sum((a+b)[] - (a[]+b[])), 0)
  check(sum((a+2)[] - (a[]+2)), 0)
  a*apply(a,2,mean)
  apply(a,2,mean)*a


# Aggregation
  check( sweep(B,MARGIN=2,apply(B,2,mean)),
         sweep(Y,MARGIN=2,apply(Y,2,mean))[])

# Join
# We need 'subarray' here to reset the origin of Y to zero to match
# diag(Y)--diag always returns a zero indexed vector.
  check(project(bind(merge(subarray(Y),diag(Y),by.x="i",by.y="i_1"),"v","val*val_1"),"v")[], diag(B)*B)

# On different dimensions
  x = as.scidb(rnorm(5))
  a = as.scidb(data.frame(p=1:5),start=0)
  merge(x,a,by.x=dimensions(x),by.y=dimensions(a))

# Add many more join/merge checks here...

# Sparse upload, count
  S = Matrix::sparseMatrix(sample(100,200,replace=TRUE),sample(100,200,replace=TRUE),x=runif(200))
  Z = as.scidb(S,start=c(1,5))
  check(count(Z), Matrix::nnzero(S))

# Check that image does not fail
  image(Z,plot=FALSE)

# spgemm
  if(got("spgemm"))
  {
    x = crossprod(Z)
  }

# Misc
  z = atan(tan(abs(acos(cos(asin(sin(Z)))))))
  s = atan(tan(abs(acos(cos(asin(sin(S)))))))
  check(sum(s-z[]), 0)

# Labeled indices
  L = c(letters,LETTERS)
  i = as.scidb(data.frame(L[1:nrow(X)]), start=0)
  j = as.scidb(data.frame(L[1:ncol(X)]), start=0)
  rownames(X) = i
  colnames(X) = j
  rownames(A) = L[1:nrow(A)]
  colnames(A) = L[1:ncol(A)]
  check(X[c("F","v","f"),c("N","a","A")][], A[c("F","v","f"),c("N","a","A")])

# 4d labels and auto promotion of labels to SciDB arrays
  X = build(0,dim=c(3,4,5,6))
  rownames(X) = letters[1:3]
  dimnames(X)[[2]] = letters[1:4]
  dimnames(X)[[3]] = letters[1:5]
  dimnames(X)[[4]] = letters[1:6]
  i = count(X[c("a","b"),"a",c("d","e"),"a"])
  check(i,4)

# Indices not at the origin, and general ranged index check
  a = build("random()",c(5,5),start=c(-3,-2), eval=TRUE)
  check(count(a[-3:-2,0:2]),6)

# Pseudo-uint64 support! And also simplified aggregation function syntax
# for apply.
  check(sum(apply(a,2,count)),25)

# Aggregation, just trying to catch errors
  A = build("random()%10",c(100,100))
  p = build("random()%2",100)
  aggregate(A,by=p,mean) # Aggregate by another array
  aggregate(A,by=2,mean) # Positional dimension index
  aggregate(A,FUN=mean)  # Grand aggregate

# More special index tests, sort, apply, densify giant sparse sort result.
  a = build("i+j",c(5,5),type="double")
  s = sort(a)
  i = apply(s,1,count)
  check(s[i][], sort(a[]))

# GLM (requires SciDB p4 plugin)
  if(got("glm"))
  {
    x = as.scidb(matrix(rnorm(5000*20),nrow=5000))
    y = as.scidb(rnorm(5000))
    M = glm.fit(x, y)
  }

# slice
  x = build("i+j+k",c(3,4,5),type="double")
  check(slice(x,c("i","j"),c(2,3))[0][], 5)

# na.locf
# Write me!

# hist
# Write me!

# order
  x = build("random()%100", 100, type="double", eval=TRUE, start=1)
  check(as.numeric(order.scidb(x)[]), order(x[]))

# rank
  check(rank(x)[,2][], rank(x[])) 

# quantile, from a failure test case reported by Alex
  scidbrm("_qtest",force=TRUE)
  iquery("create_array(_qtest,<value:double> [tumor_type_id=0:25,1,0,sample_id=0:17999,1000,0,illuminahiseq_rnaseq_probe_id=0:44999,1000,0]")
  x = scidb("_qtest")
  y = quantile(x)[]
  scidbrm("_qtest",force=TRUE)

# Another quantile failure case reported by Alex
x = read.csv(file=textConnection('"tumor_type_id","sample_id","agilentg4502a_07_1_probe_id","value"
7,2742,13317,1.1024545
7,2963,8060,0.9827
7,2609,13709,-0.18572727
7,2643,13772,0.56753844
7,2629,2126,3.6668334
7,2643,10996,0.35366666
7,2594,10300,-0.534
7,2680,4252,-0.842
7,2678,17062,-1.1738
7,2765,13244,-2.1102'))
a = redimension(as.scidb(x,types=c("int64","int64","int64","double")), dim=names(x)[1:3], eval=TRUE)
check(quantile(a)[][,2], quantile(x$value))


# Another merge test courtesy Alex Polyiakov
  x = build(1,c(2,2))
  a = build(2,5,names=c("a","j"))
  z = merge(x,a,by="j")
  check(count(z),4)

# Complicated cross_join filtering
  set.seed(1)
  X = as.scidb( matrix(rnorm(20),nrow=5) )
  rownames(X) = as.scidb( data.frame(letters[1:5]), start=0)
  X[c("b","a","d"), ]
  idx = rownames(X) > "b"
  check(nrow(X[idx, ]),3)
}
gc()

