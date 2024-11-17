import kagglehub

# Download latest version
steam_reviews_path = kagglehub.dataset_download("najzeko/steam-reviews-2021")
steam_games_path = kagglehub.dataset_download("artermiloff/steam-games-dataset")

print("Path to dataset files:", steam_reviews_path)
print("Path to dataset files:", steam_games_path)
