\name{scidb-package}
\alias{scidb-package}
\docType{package}
\title{ SciDB interface package.  }
\description{The scidb package provides basic functions
that move data between R and SciDB and issue SciDB queries.
}
\details{
\tabular{ll}{
Package: \tab scidb\cr
Type: \tab Package\cr
Version: \tab 0.0\cr
Date: \tab 2012-07-18\cr
License: \tab GPL3\cr
Depends: \tab methods\cr
}
Package options:
\itemize{
  \item \code{options(scidb.index.sequence.limit=1000000)} Maximum allowed sequential index limit (for larger use between)
  \item \code{options(scidb.max.array.elements=100000000)} Maximum allowed elements in an array return result
  \item \code{options(scidb.gemm_chunk_size=1000)} Default gemm chunk size
  \item \code{options(scidb.default_shim_port=8080L)} Default shim port
  \item \code{options(scidb.default_shim_host="localhost")} Default shim host
  \item \code{options(scidb.safe_remove=TRUE)} Require \code{force=TRUE} to remove non-session arrays
  \item \code{options(scidb.verifyhost=FALSE)} Set TRUE to reverse host lookup in SSL sessions
  \item \code{options(scidb.interrupt=TRUE)} Set FALSE to disable user interrupt handling
}
}
\author{
B. W. Lewis <blewis@paradigm4.com>

Maintainer: B. W. Lewis <blewis@paradigm4.com>
}
\keyword{ package }
