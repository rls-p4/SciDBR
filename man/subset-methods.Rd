\name{subset-methods}
\docType{methods}
\alias{subset}
\alias{subset,scidb-method}
\alias{subset,scidbdf-method}
\title{Methods for Function \code{subset} in Package \pkg{scidb}}
\description{
SciDB \code{filter} operations.
}
\usage{
\S4method{subset}{scidb}(X,subset,eval)
\S4method{subset}{scidbdf}(X,subset,eval)
}
\arguments{
  \item{X}{A \code{scidb} or \code{scidbdf} object.}
  \item{subset}{A character string representing a SciDB filter expression or an R expression.}
  \item{eval}{(Optional) If true, execute the query. Otherwise defer evaluation.}
}
\details{
Perform a SciDB \code{filter} operation on a SciDB array.  The \code{subset}
argument can be an R expression or a character string representing an explicit
SciDB filter operation.  The R expression form can include R scalar values and
can generate more efficient SciDB queries in some cases as shown in the
examples.
}
\note{
When \code{subset} is an R expression, conditions involving array dimensions
will be translated to SciDB \code{between} statements when possible.  The R
expression it must use valid R syntax, although no distinction are made between
scalar and vector forms of logical operators.  For instance, \code{|} and
\code{||} are both translated to SciDB \code{or}.

Simple R scalars and constants may be used in R expressions and they will
be translated appropriately in the generated SciDB query. More complex
R objects like functions can't be used, however, because the logical
expressions are ultimately evaluated by SciDB. Dimension values are
treated as integer values.

Explicit grouping by parenthesis may be required to generate most
optimal queries when attribute and dimension conditions are mixed together
in an expression.
}
\value{A \code{scidb} or \code{scidbdf} reference object.}
\author{
B. W. Lewis <blewis@paradigm4.com>
}
\examples{
\dontrun{
# Create a copy of the iris data frame in a 1-d SciDB array named "iris."
# Note that SciDB attribute names will be changed to conform to SciDB
# naming convention.
x <- as.scidb(iris)
# Filter the array explicitly using SciDB filter syntax
y <- subset(x,"Species = 'setosa'")
# Using an R expression form is equivalent in this example
z <- subset(x, Species = "setosa")

# The R expression form can generate better-optimized SciDB
# expressions than the explicit form. Consider the stucture
# of the SciDB array x:
str(x)

# Attributes:
#      attribute   type nullable
# 1 Sepal_Length double     TRUE
# 2  Sepal_Width double     TRUE
# 3 Petal_Length double     TRUE
# 4  Petal_Width double     TRUE
# 5      Species string     TRUE
# Dimension: 
#   dimension start end chunk
# 1       row     1 150   150

# Now compare a filter involving the 'row' dimension and
# an attribute. Note the difference in the generated queries:

y <- subset(x, "Species = 'setosa' and row > 40")
y@name
# [1] "filter(R_array5494563bc4e1101849601199,Species = 'setosa' and row > 40)"

z <- subset(x, Species == 'setosa' & row > 40)
z@name
# [1] "filter(between(R_array5494563bc4e1101849601199,41,null),Species = 'setosa' )"

# Important things to note:
# 1. The R expression form uses R syntax.
# 2. The R expression form generates a SciDB query using between on
#    the dimensions when possible.

}
}
