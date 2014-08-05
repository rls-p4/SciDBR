\name{sort.list-methods}
\docType{methods}
\alias{sort.list,scidb-method}
\alias{order,scidb-method}
\title{Methods for Function \code{sort.list} in Package \pkg{scidb}}
\description{
\code{order} and \code{sort.list}
returns a permutation which rearranges its first argument
into ascending or descending order, breaking ties by further
arguments. 
}

\usage{
\S4method{order.scidb}{scidb}(..., na.last = TRUE, decreasing = FALSE)
\S4method{sort.list}{scidb}(x, partial = NULL, na.last = TRUE, decreasing = FALSE,
               method = c("shell", "quick", "radix"))
}
\arguments{
  \item{x}{A \code{scidb} vector object.}
  \item{...}{A \code{scidb} vector object.}
  \item{decreasing}{A logical value: TRUE means sort in decreasing order.}
  \item{na.last}{NOT USED--for compatability with native R functions only.}
  \item{partial}{NOT USED--for compatability with native R functions only.}
  \item{method}{NOT USED--for compatability with native R functions only.}
}
\value{A \code{scidb} reference object.}
\note{
Due to the technical difficulty of dispatching on ..., the \code{order.scidb}
method must be called by its full name.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
x <- scidb("build(<v:double>[i=1:10,10,0],random()%100)")

a <- order.scidb(x)
print(a)
}
}
