conda deactivate
conda activate \Users\ab\Miniconda3\envs\stocks

# To run the data aggregation
uv run python main.py

uv run dg dev --working-directory \Users\ab\OneDrive\projects\stocks\src\dagster
