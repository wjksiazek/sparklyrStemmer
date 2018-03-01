#' @importFrom utils head
#' @importFrom dplyr %>%
#' @importFrom rlang %||%
#' @import sparklyr
#'
ml_create_mapping_tables <- function() { # nocov start
  param_mapping_list <-
    list("input_col" = "inputCol",
         "output_col" = "outputCol")

  param_mapping_r_to_s <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))
  param_mapping_s_to_r <- new.env(parent = emptyenv(),
                                  size = length(param_mapping_list))

  invisible(lapply(names(param_mapping_list),
                   function(x) {
                     param_mapping_r_to_s[[x]] <- param_mapping_list[[x]]
                     param_mapping_s_to_r[[param_mapping_list[[x]]]] <- x
                   }))

  ml_class_mapping_list <- list(
    # feature (transformers)
    "PolynomialExpansion" = "polynomial_expansion",
    "Normalizer" = "normalizer",
    "Interaction" = "interaction",
    "HashingTF" = "hashing_tf",
    "Binarizer" = "binarizer",
    "Bucketizer" = "bucketizer",
    "DCT" = "dct",
    "ElementwiseProduct" = "elementwise_product",
    "IndexToString" = "index_to_string",
    "OneHotEncoder" = "one_hot_encoder",
    "RegexTokenizer" = "regex_tokenizer",
    "SQLTransformer" = "sql_transformer",
    "StopWordsRemover" = "stop_words_remover",
    "Tokenizer" = "tokenizer",
    "VectorAssembler" = "vector_assembler",
    "NGram" = "ngram",
    "VectorSlicer" = "vector_slicer",
    # feature (estimators)
    "VectorIndexer" = "vector_indexer",
    "VectorIndexerModel" = "VectorIndexerModel",
    "StandardScaler" = "standard_scaler",
    "StandardScalerModel" = "standard_scaler_model",
    "MinMaxScaler" = "min_max_scaler",
    "MinMaxScalerModel" = "min_max_scaler_model",
    "MaxAbsScaler" = "max_abs_scaler",
    "MaxAbsScalerModel" = "max_abs_scaler_model",
    "Imputer" = "imputer",
    "ImputerModel" = "imputer_model",
    "ChiSqSelector" = "chisq_selector",
    "ChiSqSelectorModel" = "chisq_selector_model",
    "Word2Vec" = "word2vec",
    "Word2VecModel" = "word2vec_model",
    "IDF" = "idf",
    "IDFModel" = "idf_model",
    "VectorIndexer" = "vector_indexer",
    "VectorIndexerModel" = "vector_indexer_model",
    "QuantileDiscretizer" = "quantile_discretizer",
    "RFormula" = "r_formula",
    "RFormulaModel" = "r_formula_model",
    "StringIndexer" = "string_indexer",
    "StringIndexerModel" = "string_indexer_model",
    "CountVectorizer" = "count_vectorizer",
    "CountVectorizerModel" = "count_vectorizer_model",
    "PCA" = "pca",
    "PCAModel" = "pca_model",
    "BucketedRandomProjectionLSH" = "bucketed_random_projection_lsh",
    "BucketedRandomProjectionLSHModel" = "bucketed_random_projection_lsh_model",
    "MinHashLSH" = "minhash_lsh",
    "MinHashLSHModel" = "minhash_lsh_model",
    # regression
    "LogisticRegression" = "logistic_regression",
    "LogisticRegressionModel" = "logistic_regression_model",
    "LinearRegression" = "linear_regression",
    "LinearRegressionModel" = "linear_regression_model",
    "GeneralizedLinearRegression" = "generalized_linear_regression",
    "GeneralizedLinearRegressionModel" = "generalized_linear_regression_model",
    "DecisionTreeRegressor" = "decision_tree_regressor",
    "DecisionTreeRegressionModel" = "decision_tree_regression_model",
    "GBTRegressor" = "gbt_regressor",
    "GBTRegressionModel" = "gbt_regression_model",
    "RandomForestRegressor" = "random_forest_regressor",
    "RandomForestRegressionModel" = "random_forest_regression_model",
    "AFTSurvivalRegression" = "aft_survival_regression",
    "AFTSurvivalRegressionModel" = "aft_survival_regression_model",
    "IsotonicRegression" = "isotonic_regression",
    "IsotonicRegressionModel" = "isotonic_regression_model",
    # classification
    "GBTClassifier" = "gbt_classifier",
    "GBTClassificationModel" = "gbt_classification_model",
    "RandomForestClassifier" = "random_forest_classifier",
    "RandomForestClassificationModel" = "random_forest_classification_model",
    "NaiveBayes" = "naive_bayes",
    "NaiveBayesModel" = "naive_bayes_model",
    "MultilayerPerceptronClassifier" = "multilayer_perceptron_classifier",
    "MultilayerPerceptronClassificationModel" = "multilayer_perceptron_classification_model",
    "OneVsRest" = "one_vs_rest",
    "OneVsRestModel" = "one_vs_rest_model",
    "LinearSVC" = "linear_svc",
    "LinearSVCModel" = "linear_svc_model",
    # recommendation
    "ALS" = "als",
    "ALSModel" = "als_model",
    # clustering
    "LDA" = "lda",
    "LDAModel" = "lda_model",
    "KMeans" = "kmeans",
    "KMeansModel" = "kmeans_model",
    "BisectingKMeans" = "bisecting_kmeans",
    "BisectingKMeansModel" = "bisecting_kmeans_model",
    "GaussianMixture" = "gaussian_mixture",
    "GaussianMixtureModel" = "gaussian_mixture_model",
    # fpm
    "FPGrowth" = "fpgrowth",
    "FPGrowthModel" = "fpgrowth_model",
    # tuning
    "CrossValidator" = "cross_validator",
    "CrossValidatorModel" = "cross_validator_model",
    "TrainValidationSplit" = "train_validation_split",
    "TrainValidationSplitModel" = "train_validation_split_model",
    # evaluation
    "BinaryClassificationEvaluator" = "binary_classification_evaluator",
    "MulticlassClassificationEvaluator" = "multiclass_classification_evaluator",
    "RegressionEvaluator" = "regression_evaluator",
    # pipeline
    "Pipeline" = "pipeline",
    "PipelineModel" = "pipeline_model",
    "Transformer" = "transformer",
    "Estimator" = "estimator",
    "PipelineStage" = "pipeline_stage"
  )

  ml_class_mapping <- new.env(parent = emptyenv(),
                              size = length(ml_class_mapping_list))

  invisible(lapply(names(ml_class_mapping_list),
                   function(x) {
                     ml_class_mapping[[x]] <- ml_class_mapping_list[[x]]
                   }))

  rlang::ll(param_mapping_r_to_s = param_mapping_r_to_s,
            param_mapping_s_to_r = param_mapping_s_to_r,
            ml_class_mapping = ml_class_mapping)
} # nocov end
