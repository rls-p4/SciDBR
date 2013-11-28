\name{slice}
\alias{slice}
\title{
slice
}
\description{
Slice along an array coordinate value, returning a lower-dimensional array.
}
\usage{
slice(x, d, n, `eval`=FALSE)
}
\arguments{
  \item{x}{A scidb object}
  \item{d}{The coordinate axis to slice along (character).}
  \item{n}{The coordinate axis value to slice at (integer).}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation. By default, \code{eval} is set to defer evaluation only when called from functions in the \pkg{scidb} package.}
}
\value{
A \code{scidb} object of one lower dimension than the original object.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
## XXX
}}
