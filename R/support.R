`cross_join` = function(X,Y,...)
{
  M = match.call()
  M = M[-c(1,2,3)]
  query = sprintf("cross_join(%s as __X, %s as __Y", X@name, Y@name)
  if(length(M)>0)
  {
    E = parent.frame()
    i = lapply(1:length(M), function(j) tryCatch(eval(M[j][[1]],E),error=function(e)c()))
    cterms = paste(c("__X","__Y"), i, sep=".")
    cterms = paste(cterms,collapse=",")
    query  = paste(query,cterms,")",sep="")
  } else
  {
    query  = sprintf("%s)",query)
  }
  newarray = tmpnam()
  query = sprintf("store(%s,%s)",query,newarray)
  scidbquery(query)
  return(scidb(newarray,gc=TRUE))
}
