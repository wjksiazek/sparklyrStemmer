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
