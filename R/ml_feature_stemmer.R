#' Feature Transformation -- Stemmer (Transformer)
#'
#' A stemmer that converts input tokens to their word stem.
#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param input_col The name of the input column.
#' @param output_col The name of the output column.
#' @param language A character string used to specify the language of a stemmed text.
#' @param uid A character string used to uniquely identify the feature transformer.
#' @param ... Optional arguments; currently unused.
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns a \code{ml_transformer},
#'   a \code{ml_estimator}, or one of their subclasses. The object contains a pointer to
#'   a Spark \code{Transformer} or \code{Estimator} object and can be used to compose
#'   \code{Pipeline} objects.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the transformer or estimator appended to the pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a transformer is constructed then
#'   immediately applied to the input \code{tbl_spark}, returning a \code{tbl_spark}
#' }
#'
#' @examples
#'\dontrun{
#'library(dplyr)
#' library(sparklyr)
#' sc <- spark_connect(master = "local")
#' data.frame(text = "Mary had a little lamb") %>%
#' copy_to(sc, ., "test") %>%
#' sdf_mutate(tokens = ft_tokenizer(text),
#'            stoppedTokens = ft_stop_words_remover(tokens),
#'            stemmedTokens = ft_stemmer(stoppedTokens))}
#'
#' @seealso
#' \url{https://github.com/master/spark-stemming}
#'
#' \url{http://snowballstem.org/}
#'

#' @importFrom utils head
#' @importFrom dplyr %>%
#' @importFrom rlang %||%

#' @export
ft_stemmer <- function(
  x, input_col, output_col, language = "English",
  uid = sparklyr::random_string("stemmer_"), ...) {
  UseMethod("ft_stemmer")
}

#' @export
ft_stemmer.spark_connection <- function(
  x, input_col, output_col, language = "English",
  uid = sparklyr::random_string("stemmer_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.mllib.feature.Stemmer",
                             input_col, output_col, uid) %>%
    sparklyr::invoke("setLanguage", language)

  new_ml_stemmer(jobj)
}

#' @export
ft_stemmer.ml_pipeline <- function(
  x, input_col, output_col, language = "English",
  uid = sparklyr::random_string("stemmer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_stemmer.tbl_spark <- function(
  x, input_col, output_col, language = "English",
  uid = sparklyr::random_string("stemmer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  sparklyr::ml_transform(transformer, x)
}

new_ml_stemmer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_stemmer")
}
