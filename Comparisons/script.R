# Author: Adib Alwani

library(ggplot2)
library(rmarkdown)
library(knitr)

# Read benchmark file
df <- read.csv("benchmark.csv")

# Plot the Graph
p <- ggplot(df, aes(MODE, TIME)) + geom_point()
p + facet_grid(. ~ PLATFORM)

# Report
render("report.Rmd")
