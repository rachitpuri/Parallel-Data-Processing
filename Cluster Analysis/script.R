library(ggplot2)
library(rmarkdown)
library(knitr)

# Read output of Reducer
df <- read.table("finaloutput")

# V1 --> Carrier Code
# V2 --> Month in 2015
# V3 --> Total flights in 2015
# V4 --> Average ticket price in 2015

# Order by V3
o1 <- df[order(df$V3),]

# Get top 10 flights
top10 <- tail(o1, n = 10L)
colnames(top10) <- c("carrier_code", "month", "total_flights", "average_price")

# Convert numeric month to letter abbrevation
top10$month <- month.abb[top10$month]

# Plot
p <- ggplot(top10, aes(carrier_code, average_price)) + geom_point()
p + facet_grid(. ~ month)

# Save
ggsave("plot.png")

# Report
render("report.Rmd")
