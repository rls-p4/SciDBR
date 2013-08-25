\name{scidbexpr-class}
\Rdversion{1.1}
\docType{class}
\alias{scidbexpr-class}
\alias{subset,scidbexpr,character-method}

\title{Class \code{"scidbexpr"}}
\description{A class that represents a SciDB query expression. This class is intended to distinguish SciDB expressions from general character strings.}
\section{Methods}{
  \describe{
    \item{subset}{\code{signature(x = "scidbdf", y = "character")}: ... }
    \item{show}{\code{signature(object = "scidbexpr")}: ... }
  }
}
\section{Notes}{
This class is intended to distinguish a SciDB query expression from a general
character string. Objects of this class work with many functions that also
work with scidb and scidbdf objects.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\keyword{classes}
