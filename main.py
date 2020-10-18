import logging
from utils import ETLHelper

# We want release date, title, genres, revenue, production_companies, budget (profit = revenue - cost), popularity
# however, we want to basically "dim" production_companies and genres as the dimensions in this case
# This ETL will create "dimensions" from genre and prod and associate them with "fact" data to perform aggregrate calculations
# "movie_id" (id in data/movies_metadata.csv) will be the primary key to link the data

# data_model.png = ERP-style diagram for this data model
# Final "reports" to hydrate the data model are delivered into 'reports/xxx.csv'

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data_loader = ETLHelper()
    data_loader.create_title_dim('data/movies_metadata.csv')
    data_loader.create_production_model()
    data_loader.create_genre_model()
    data_loader.create_facts()
    data_loader.create_genre_report_data()
    data_loader.create_production_report_data()