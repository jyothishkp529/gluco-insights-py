# gluco-insights-py
A simple PySpark based analytical tool to find insights from Blood Glucose data.

# Run Pipeline
```
spark-submit --deploy-mode client --master local[*]  pipeline.GlucoApp.py -p glucose_measure
```