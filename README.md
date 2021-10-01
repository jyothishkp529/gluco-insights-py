# gluco-insights-py
A simple PySpark based analytical tool to find insights from Blood Glucose data.

## Build project
This project is build with [Poetry](https://python-poetry.org/docs/) . Please follow the link to setup Poetry locally, if not available.
```
poetry build
```
## Run Pipeline
- unzip dist/gluco-insights-1.0.0.tar.gz
- configure the pipeline input and output paths

```
cd dist/gluco-insights-1.0.0/src
spark-submit --deploy-mode client --master local[*]  GlucoApp.py -p glucose_measure
```