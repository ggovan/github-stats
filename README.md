# Github Stats

Spark project to process Event and Commit data from GitHub.

# Build

```
sbt assembly
```

# Useage

Processes data stored in json files in directories for events and commits.

```
spark-submit --class githubstats.GithubStats github-stats-assembly-0.0.1.jar path/to/events/dir path/to/commits/dir
```
