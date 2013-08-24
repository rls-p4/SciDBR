\name{bind}
\alias{bind}
\title{
bind
}
\description{
The \code{bind} function is a wrapper to the SciDB `apply` operator.
We chose the name `bind` as this function most closely matches the
concept of the R \code{cbind} function.

Similarly to \code{cbind}, the \code{bind} function adds a new
variable (a.k.a. SciDB `attribute`) to a SciDB array.
}
\usage{
bind(X, name, FUN, eval)
}
\arguments{
  \item{X}{A scidb, scidbdf, or scidbexpr object}
  \item{name}{A character name for the new attribute}
  \item{FUN}{A SciDB function or expression that defines the values for the new attribute (character)}
  \item{eval}{A logical value: if TRUE then evaluate the \code{bind} expression and return a \code{scidb} object, otherwise return a \code{scidbexpr} object that contains a character string representation of the un-evaluated SciDB query. By default, eval is set to FALSE if \code{bind} is called from another function in the scidb package.}
}
\value{
Either a \code{scidb} object or a \code{scidbexpr} object depending on the value of \code{eval}.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency. Many functions in the SciDB package accept un-evaluated expresions represented as \code{scidbexpr} objects. One can compose these objects to form complex query expressions that can be evaluated in one transaction.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(iris)
y <- bind(x, "prod", "Petal_Length * Petal_Width")

# The new scidbdf object y contains a new variable named 'prod':
print(head(y))
}}
