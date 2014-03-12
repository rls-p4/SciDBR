# Probability distribution and quantile functions
# These functions require the Paradigm4 extensions to SciDB.

setOldClass("phyper")
setGeneric("phyper", function(x, ...) stats::phyper(x,...))
setMethod("phyper", signature(x="scidbOrScidbdf"),
  function(x, q, m, n, k, new="p")
  {
    query = sprintf("hygecdf(%s,%s,%s,%s)",q,m,n,k)
    bind(x, new, query)
  })

setOldClass("qhyper")
setGeneric("qhyper", function(x, ...) stats::qhyper(x,...))
setMethod("qhyper", signature(x="scidbOrScidbdf"),
  function(x, q, m, n, k, new="q")
  {
    query = sprintf("ihygecdf(%s,%s,%s,%s)",q,m,n,k)
    bind(x, new, query)
  })
