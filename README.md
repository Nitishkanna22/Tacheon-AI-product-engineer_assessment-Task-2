# Tacheon-AI-product-engineer_assessment-Task-2

A production-minded Python pipeline that fetches hourly weather data for
multiple Indian cities from the Open-Meteo API, transforms it into an
analytics-ready format, and loads it into Google BigQuery.

## Why did I select Open- Meteo
* No API key requiredZero setup friction; anyone can clone and run
* Reliable SLA for a free APINot likely to be down during assessment review
* Multi-city supportLets us demonstrate parameterisation and batch fetching

## Pipeline Explained

![image_alt](https://github.com/Nitishkanna22/Tacheon-AI-product-engineer_assessment-Task-2/blob/ce47f5fe64fb7b6b474474ff6255e22a32fc9c45/Pipeline%20Explained.png)

## Project Structure 

![image_alt](https://github.com/Nitishkanna22/Tacheon-AI-product-engineer_assessment-Task-2/blob/5a4ae1aa2b251ae184dc0ac5ba4b96e034a7bd95/Project%20Structure.png)

## Output 

![image_alt](https://github.com/Nitishkanna22/Tacheon-AI-product-engineer_assessment-Task-2/blob/0a08ead767d8613b4524d182be0a127d2ecae668/Output%20Screenshoot%201.png)


## Decisions Made and What I'd Revisit

* Chose WRITE_APPEND over WRITE_TRUNCATE:
Simple and safe. The tradeoff is duplicates on re-runs.
With more time, I'd add an incremental load pattern using a watermark table.

* Chose load jobs over streaming inserts:
Streaming inserts aren't available in the BigQuery Sandbox, but load jobs are.
In production on a billed project, streaming inserts would give near-real-time latency.

* Single-threaded city fetching:
Deliberate for simplicity and readability. With 5 cities it's fast enough.
First thing I'd parallelise at scale.

* Geocoding on every run:
Lat/lon for a city doesn't change. A real production pipeline would cache this
in a small reference table and skip geocoding for known cities.

* No unit tests in this submission:
Given the 4-day constraint, I prioritised working, documented code over test coverage.
In a real codebase, fetcher.py and transformer.py are both pure-function-ish and
straightforward to test with pytest and unittest.mock for the API calls.
