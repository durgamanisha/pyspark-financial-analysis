# Financial Data Analysis using PySpark

## Overview
Analyze historical cryptocurrency/stock data using PySpark. Calculate moving averages, daily returns, and volatility. Store processed data in parquet files (data lake) and perform trend analysis.

## Technologies
- Python
- PySpark
- SQL
- CSV Dataset


### Sample Output

+------+----------+-----+-------+--------------------+--------------------+
|symbol| date|close| SMA_2| daily_return| volatility_2|
+------+----------+-----+-------+--------------------+--------------------+
| BTC|2025-01-01|30500|30500.0| 0.0| 0.0|
| BTC|2025-01-02|31500|31000.0| 0.03278688524590164| 0.0|
| BTC|2025-01-03|32000|31750.0|0.015873015873015872|0.011959911729670985|
| ETH|2025-01-01| 2050| 2050.0| 0.0| 0.0|
| ETH|2025-01-02| 2100| 2075.0|0.024390243902439025| 0.0|
| ETH|2025-01-03| 2150| 2125.0|0.023809523809523808|4.106311156716319...|
+------+----------+-----+-------+--------------------+--------------------+
