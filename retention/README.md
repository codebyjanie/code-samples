## About the Project
### Background
Content creators are changing how consumers interact with brands, media, and purchasing channels. 
A research by Deloitte show that three out of five consumers surveyed are likely to positively engage with a brand with the right creator’s recommendation.
As brands look to raise their share of voice with content creators and invest strategically to scale their collaborations, it's important to understand whether the brands are acquiring new influencers, and whether the existing influencers retained or churned.
"Creator retention" impacts the success and ROI of influencer programs and what brands can do to maximize their performance. 
### Objective
Brands can analyze their ROI on influencers marketing and budget investments at a program level
## Getting Started
### Data
Posts that are mentioning the brands across social media platforms including Instagram, YouTube, TikTok, etc.
### Usage
You can always run `python retention.py -h` to see a list of parameters and what they are used for:
* `-p, --posts` Single post file
* `-f, --folder` Folder of multiple post files
* `-g, --groupby` Filters to partition the data (by brand, post category, platform, etc.)
* `-t, --timeframe` Set the timeframe for the aggregation. You can choose from the following: month|quarter|half-year|year
* `--brand-list` CSV file of a subset of brands
* `--brand-group` CSV file of taxonomy of brands to define the division of the brands
* `--out-all` Output the overall retention rates
* `--out-groupby` Output the retention rates for groupby items
* `--out-plm` Output the influencer list with performance metrics for groupby items
### Sample commands
```
./retention.py -p [POST_FILE.csv] -g category,group,beauty_group -t quarter --brand-group [BRAND_TAXONOMY.csv] --out-all [OUTPUT_RETENTION_OVERALL.csv] --out-groupby [OUTPUT_RETENTION_BY_GROUP] --out-plm [OUTPUT_PERFORMANCE_METRICS.csv]
```
