# DTU-CTFDS-02807-PROJ

This project contains a Jupyter notebook for each technique described in the report.

- ALS model: collaborative/als_model.ipynb
- content_based_filtering: content_based/content_based_filtering.ipynb
- KMeans model: kmeans_model.ipynb
- Simple exploratory statistics: exploratory.ipynb
- Preprocessing of CSV to Parquet: preprocessing.ipynb

All dependencies are listed in the pyproject.toml, which can be installed using.
```bash
pip3 install -e .
```

Each notebook can be run independently, so long as the datasets reside at:
- Reviews parquet: file:////work/ds/steam_reviews_parquet
- Games parquet: file:////work/ds/steam_games_parquet

The parquet files can be downloaded at:
- https://1drv.ms/f/s!Av3RfVE8_irIg_Y5rz4zF16iNjDuZw?e=JWD0zS