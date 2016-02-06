# A general SciDB array class for R. It's a hybrid S4 class with some S3
# methods. The class can represent SciDB arrays and array promises.
#
# A scidb object is fully defined by:
# name = any SciDB expression that can produce an array 
# schema = the corresponding SciDB array schema for 'name' above
# logical_plan = the corresponding array query plan
# gc = environment
#      If gc$remove = TRUE, remove SciDB array when R gc is run on object.
#      The gc environment also stores dependencies required by array promises.
#
# Objects includes a few additional convenience slots:
# dimensions = a character vector of SciDB dimension names
# attributes = character vector of array attribute names  (derived from the schema)

setClass("scidb",
         representation(name="character",
                        schema="character",
                        attributes="character",
                        dimensions="character",
                        logical_plan="character",
                        gc="environment"),
         S3methods=TRUE)
