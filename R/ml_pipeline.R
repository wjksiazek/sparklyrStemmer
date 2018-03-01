#' @importFrom utils head
#' @importFrom dplyr %>%
#' @importFrom rlang %||%
#' @import sparklyr

new_ml_transformer <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_transformer"))
}

new_ml_pipeline_stage <- function(jobj, ..., subclass = NULL) {
  structure(
    list(
      uid = sparklyr::invoke(jobj, "uid"),
      param_map = ml_get_param_map(jobj),
      ...,
      .jobj = jobj
    ),
    class = c(subclass, "ml_pipeline_stage")
  )
}

ml_add_stage <- function(x, transformer) {
  sc <- sparklyr::spark_connection(x)
  stages <- if (rlang::is_null(sparklyr::ml_stages(x))) list(sparklyr::spark_jobj(transformer)) else {
    tryCatch(sparklyr::spark_jobj(x) %>%
               sparklyr::invoke("getStages") %>%
               c(sparklyr::spark_jobj(transformer)),
             error = function(e) sparklyr::spark_jobj(x) %>%
               sparklyr::invoke("stages") %>%
               c(sparklyr::spark_jobj(transformer))
    )
  }

  jobj <- sparklyr::invoke_static(sc, "sparklyr.MLUtils",
                        "createPipelineFromStages",
                        sparklyr::ml_uid(x),
                        stages)
  new_ml_pipeline(jobj)
}

ml_new_transformer <- function(sc, class, input_col, output_col, uid) {
  sparklyr::ensure_scalar_character(input_col)
  sparklyr::ensure_scalar_character(output_col)
  sparklyr::ensure_scalar_character(uid)
  sparklyr::invoke_new(sc, class, uid) %>%
    sparklyr::invoke("setInputCol", input_col) %>%
    sparklyr::invoke("setOutputCol", output_col)
}

new_ml_pipeline <- function(jobj, ..., subclass = NULL) {
  stages <- tryCatch({
    jobj %>%
      sparklyr::invoke("getStages") %>%
      lapply(ml_constructor_dispatch)
  },
  error = function(e) {
    NULL
  })
  new_ml_estimator(
    jobj,
    stages = stages,
    stage_uids = if (rlang::is_null(stages))
      NULL
    else
      sapply(stages, function(x)
        x$uid),
    ...,
    subclass = c(subclass, "ml_pipeline")
  )
}

ml_new_stage_modified_args <- function(envir = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  modified_args <- caller_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args() %>%
    lapply(rlang::new_quosure, env = envir) %>%
    rlang::modify(
      x = rlang::new_quosure(rlang::parse_expr("spark_connection(x)"), env = caller_frame$env)
    ) %>%
    # filter `features` so it doesn't get partial matched to `features_col`
    (function(x) x[setdiff(names(x), "features")])

  stage_constructor <- sub("\\..*$", ".spark_connection", rlang::lang_name(caller_frame))
  rlang::lang(stage_constructor, !!!modified_args) %>%
    rlang::eval_tidy()
}

ml_ratify_args <- function(env = rlang::caller_env(2)) {
  caller_frame <- rlang::caller_frame()
  caller <- caller_frame$fn_name %>%
    strsplit("\\.") %>%
    unlist() %>%
    head(1)

  if (grepl("^ml_", caller)) {
    # if caller is a ml_ function (as opposed to ft_),
    #   get calls to function in the stack
    calls <- sys.calls()
    calls <- calls %>%
      sapply(`[[`, 1) %>%
      sapply(deparse) %>%
      grep(caller, ., value = TRUE)

    # if formula is specified and we didn't dispatch to ml_*.tbl_spark,
    #   throw error
    if (!any(grepl("tbl_spark", calls)) &&
        !rlang::is_null(caller_frame$env[["formula"]]))
      stop(paste0("formula should only be specified when calling ",
                  caller, " on a tbl_spark"))
  }

  validator_fn <- caller_frame$fn_name %>%
    gsub("^(ml_|ft_)", "ml_validator_", .) %>%
    gsub("\\..*$", "", .)
  args <- caller_frame %>%
    rlang::lang_standardise() %>%
    rlang::lang_args()

  default_args <- Filter(Negate(rlang::is_symbol),
                         rlang::fn_fmls(caller_frame$fn)) %>%
    lapply(rlang::new_quosure, env = caller_frame$env)

  args_to_validate <- ml_args_to_validate(args, default_args) %>%
    lapply(rlang::eval_tidy, env = env)

  validated_args <- rlang::invoke(
    validator_fn, args = args_to_validate, nms = names(args)
  )

  invisible(
    lapply(names(validated_args),
           function(x) assign(x, validated_args[[x]], caller_frame$env))
  )
}

ml_args_to_validate <- function(args, current_args, default_args = current_args) {
  # creates a list of arguments to validate
  # precedence: user input, then, current pipeline params,
  #   then default args from formals
  input_arg_names <- names(args)
  current_arg_names <- names(current_args)
  default_arg_names <- names(default_args)

  args %>%
    c(current_args[setdiff(current_arg_names, input_arg_names)]) %>%
    c(default_args[setdiff(default_arg_names,
                           union(input_arg_names, current_arg_names)
    )]
    )
}


ml_get_constructor <- function(jobj) {
  jobj %>%
    sparklyr::jobj_class() %>%
    lapply(ml_map_class) %>%
    Filter(length, .) %>%
    lapply(function(x) paste0("new_ml_", x)) %>%
    Filter(function(fn) exists(fn, where = asNamespace("sparklyr"),
                               mode = "function"), .) %>%
    rlang::flatten_chr() %>%
    head(1)
}

ml_constructor_dispatch <- function(jobj) {
  do.call(ml_get_constructor(jobj), list(jobj = jobj))
}

ml_map_class <- function(x) {
  mapping_tables <- ml_create_mapping_tables()
  mapping_tables$ml_class_mapping[[x]]
}

ml_validator_stemmer <- function(args, nms){
  args %>%
    ml_validate_args(
      {
        language <- sparklyr::ensure_scalar_character(language)
      }
    ) %>%
    ml_extract_args(nms)
}

ml_extract_args <- function(
  args, nms, mapping_list = list(
    input.col = "input_col",
    output.col = "output_col"
  )) {
  args[mapply(`%||%`, mapping_list[nms], nms)]
}

ml_validate_args <- function(
  args, expr = NULL,
  mapping_list = list(
    input.col = "input_col",
    output.col = "output_col"
  )) {
  validations <- rlang::enexpr(expr)

  data <- names(args) %>%
    setdiff(mapping_list[intersect(names(mapping_list), .)]) %>%
    args[.] %>%
    rlang::set_names(
      mapply(`%||%`, mapping_list[names(.)], names(.))
    )

  rlang::invoke(within,
                data = data,
                expr = validations,
                .bury = NULL)
}

new_ml_estimator <- function(jobj, ..., subclass = NULL) {
  new_ml_pipeline_stage(jobj,
                        ...,
                        subclass = c(subclass, "ml_estimator"))
}
