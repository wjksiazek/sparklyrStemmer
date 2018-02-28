#' @export
ft_stemmer <- function(
  x, input_col, output_col, language = "English",
  uid = random_string("stemmer_"), ...) {
  UseMethod("ft_stemmer")
}

#' @export
ft_stemmer.spark_connection <- function(
  x, input_col, output_col, language = "English",
  uid = random_string("stemmer_"), ...) {

  ml_ratify_args()
  jobj <- ml_new_transformer(x, "org.apache.spark.mllib.feature.Stemmer",
                             input_col, output_col, uid) %>%
    sparklyr::invoke("setLanguage", language)

  new_ml_stemmer(jobj)
}

#' @export
ft_stemmer.ml_pipeline <- function(
  x, input_col, output_col, language = "English",
  uid = random_string("stemmer_"), ...) {

  transformer <- ml_new_stage_modified_args()
  ml_add_stage(x, transformer)
}

#' @export
ft_stemmer.tbl_spark <- function(
  x, input_col, output_col, language = "English",
  uid = random_string("stemmer_"), ...) {
  transformer <- ml_new_stage_modified_args()
  ml_transform(transformer, x)
}

new_ml_stemmer <- function(jobj) {
  new_ml_transformer(jobj, subclass = "ml_stemmer")
}
