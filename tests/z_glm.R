# glm tests

library(scidb)
check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a,b,check.attributes=FALSE,check.names=FALSE))
}

host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  set.seed(1)
  x = as.scidb(matrix(rnorm(5000*20),nrow=5000))
  y = as.scidb(rnorm(5000))
  M = glm.fit(x, y)

  counts = c(18,17,15,20,10,20,25,13,12)
  outcome = gl(3,1,9)
  treatment = gl(3,3)
  d.AD = data.frame(treatment, outcome, counts)
  glm.D93 = glm(counts ~ outcome + treatment, family = poisson(),data=d.AD, y=TRUE)

  d.AD_sci = as.scidb(d.AD)
  glm.D93_sci = glm(counts ~ outcome + treatment, family = poisson(), data=d.AD_sci)
  summary(glm.D93_sci)
  print(glm.D93_sci)
  predict(glm.D93_sci, newdata=d.AD_sci)
  check(summary(glm.D93)$aic, glm.D93_sci$aic)
}
