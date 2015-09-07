\name{order_scidb}
\title{Methods for Function \code{order} in Package \pkg{scidb}}
\description{
\code{order}
returns a permutation which rearranges its first argument
into ascending or descending order.
}
\usage{
order_scidb(x, attribute=1, decreasing=FALSE)
}
\arguments{
  \item{x}{A 1-d SciDB array object.}
  \item{attribute}{1-based numeric index of the SciDB array attribute to order on.}
  \item{decreasing}{Set to \code{TRUE} to return a permutation that orders entries in decreasing order.}
}
\value{
A 1-d SciDB array with integer permutation order entries.
}
\author{
B. W. Lewis <blewis@paradigm4.com>
}

\examples{
\dontrun{
x <- scidbeval(scidb("build(<v:double>[i=1:10,10,0],random()%100)"))
a <- order.scidb(x)
a[]
}
}
