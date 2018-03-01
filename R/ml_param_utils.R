#' @importFrom utils head
#' @importFrom dplyr %>%
#' @importFrom rlang %||%
#' @import sparklyr

ml_set_param <- function(x, param, value, ...) {
  setter <- param %>%
    ml_map_param_names(direction = "rs") %>%
    {paste0("set",
            toupper(substr(., 1, 1)),
            substr(., 2, nchar(.)))}
  sparklyr::spark_jobj(x) %>%
    sparklyr::invoke(setter, value) %>%
    ml_constructor_dispatch()
}

ml_get_param_map <- function(jobj) {
  sc <- sparklyr::spark_connection(jobj)
  object <- if (sparklyr::spark_version(sc) < "2.0.0")
    "sparklyr.MLUtils"
  else
    "sparklyr.MLUtils2"

  sparklyr::invoke_static(sc,
                object,
                "getParamMap",
                jobj) %>%
    ml_map_param_list_names()
}

ml_map_param_list_names <- function(x, direction = c("sr", "rs"), ...) {

  mapping_tables <- ml_create_mapping_tables()
  direction <- rlang::arg_match(direction)
  mapping <- if (identical(direction, "sr"))
    mapping_tables$param_mapping_s_to_r
  else
    mapping_tables$param_mapping_r_to_s

  rlang::set_names(x, unname(sapply(names(x), function(nm) mapping[[nm]] %||% nm)))
}

ml_map_param_names <- function(x, direction = c("sr", "rs"), ...) {
  mapping_tables <- ml_create_mapping_tables()
  direction <- rlang::arg_match(direction)
  mapping <- if (identical(direction, "sr"))
    mapping_tables$param_mapping_s_to_r
  else
    mapping_tables$param_mapping_r_to_s

  unname(sapply(x, function(nm) mapping[[nm]] %||% nm))
}
