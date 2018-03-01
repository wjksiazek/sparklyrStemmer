library(dplyr)
library(sparklyr)
library(sparklyrStemmer)

sc <- spark_connect(master = "local")
test_df <- data.frame(text = "Mary had a little lamb") %>%
  copy_to(sc, ., "test", overwrite = TRUE)

test_that("stemmer works", {
  stemmingResult <-
    test_df %>%
    ft_tokenizer("text", "tokens") %>%
    ft_stemmer("tokens", "stemmedTokens") %>%
    select(stemmedTokens) %>%
    collect %>%
    unlist(use.names = F)

  expect_equal(stemmingResult, c("mari", "had", "a", "littl", "lamb"))

})

test_that("stemmer object is created", {
  stemmer <- ft_stemmer(sc, "tokens", "stemmedTokens", "English")
  expect_is(stemmer, "ml_stemmer")
  expect_is(stemmer, "ml_transformer")
  expect_is(stemmer, "ml_pipeline_stage")
})

test_that("stemmer can be added to a pipeline", {

  skip("Later...")

  pipeline <- ml_pipeline(sc)
  pipeline1 <- ft_stemmer(pipeline, "tokens", "stemmedTokens", "English")

  skip("Later...")
  expect_is(pipeline1$stages[[1]], "ml_stemmer")

  pipeline2 <- ft_tokenizer(pipeline, "text", "tokens") %>%
    ft_stemmer("tokens", "stemmedTokens", "English")

  skip("Later...")
  expect_equal(length(pipeline2$param_map$stages), 2)
  expect_equal(length(pipeline2$stages), 2)
  expect_is(pipeline2$stages[[1]], "ml_tokenizer")
  expect_is(pipeline2$stages[[1]], "ml_stemmer")
})

spark_disconnect(sc)
detach("package:dplyr", character.only = T)
# detach("package:sparklyr", character.only = T)
