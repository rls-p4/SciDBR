# Probability distribution and quantile functions
# These functions require the Paradigm4 extensions to SciDB.

setOldClass("phyper")
setGeneric("phyper", function(x, ...) stats::phyper(x,...))
setMethod("phyper", signature(x="scidb_or_scidbdf"),
  function(x, q, m, n, k, new="p",`eval`=FALSE)
  {
    query = sprintf("hygecdf(%s,%s,%s,%s)",q,m,n,k)
    bind(x, new, query, `eval`=eval)
  })

setOldClass("qhyper")
setGeneric("qhyper", function(x, ...) stats::qhyper(x,...))
setMethod("qhyper", signature(x="scidb_or_scidbdf"),
  function(x, p, m, n, k, new="q", `eval`=FALSE)
  {
    query = sprintf("ihygecdf(%s,%s,%s,%s)",p,m,n,k)
    bind(x, new, query, `eval`=eval)
  })
