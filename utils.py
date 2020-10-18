import pandas as pd
import sqlite3 
from ast import literal_eval
import logging
pd.options.mode.chained_assignment = None


class ETLHelper:

    def __init__(self):
        self.conn = sqlite3.connect('movies_model.db')
        self.cur = self.conn.cursor()
        self.title_dim = None
    
    def df_to_db(self, data, tablename, prefix='dim'):
        data.to_sql(name=f'{prefix}_{tablename}', if_exists='replace', con=self.conn, index=False)
        res = self.cur.execute(f"""SELECT count(*) FROM {prefix}_{tablename};""").fetchall()
        logging.info(f"Created {prefix}_{tablename} with {res[0][0]} records")

    def create_title_dim(self, data):
        metadata = pd.read_csv(data)
        slim_df = metadata[['id', 'production_companies', 'budget', 'revenue', 'title', 'popularity', 'genres', 'release_date']]
        # remove likely bad/incomplete/missing data
        slim_df = slim_df.dropna()
        # cast
        slim_df['revenue'] = slim_df['revenue'].astype('int')
        slim_df['budget'] = slim_df['budget'].astype('int')
        slim_df = slim_df[slim_df['revenue'] > 0] 
        slim_df = slim_df[slim_df['budget'] > 0] 
        slim_df['release_date'] = pd.to_datetime(slim_df['release_date'], format='%Y-%m-%d').dt.date
        # prepare data for splitting into quasi-dim/facts
        title_dim = slim_df.rename(columns={"id": "movie_id"})
        self.title_dim = title_dim

    def create_production_model(self):
        # iterate to create a model
        prod_list = []
        for ind in self.title_dim.index:
            # eval string into dict (they are NOT json's, but nested python dicts)
            try:
                production_list_dict = literal_eval(self.title_dim['production_companies'][ind])
            except SyntaxError: # catch syntax EOF for bad nested data
                logging.warn(f"Couldn't unnest python dict: {self.title_dim['production_companies'][ind]}")
                pass
            # flatten
            prod_df = pd.DataFrame(production_list_dict)
            if not prod_df.empty:
                prod_list.append(prod_df)
                prod_df.columns = ['prod_company_name', 'prod_company_id']
                prod_df['movie_id'] = self.title_dim['movie_id'][ind]
        prod_df_out = pd.concat(prod_list)
        self.df_to_db(prod_df_out.drop_duplicates(), 'production_companies')
            
    def create_genre_model(self):
        genre_list = []
        for ind in self.title_dim.index:
            # eval string into dict (they are NOT json's)
            try:
                genre_list_dict = literal_eval(self.title_dim['genres'][ind])
            except SyntaxError: # catch syntax EOF for bad nested data
                logging.warn(f"Couldn't unnest python dict: {self.title_dim['genres'][ind]}")
                pass
            # flatten
            genre_df = pd.DataFrame(genre_list_dict)
            if not genre_df.empty:
                genre_list.append(genre_df)
                genre_df.columns = ['genre_id', 'genre']
                genre_df['movie_id'] = self.title_dim['movie_id'][ind]
        genre_df_out = pd.concat(genre_list)
        self.df_to_db(genre_df_out.drop_duplicates(), 'genres')

    def create_facts(self):
        fact_df = self.title_dim[['movie_id', 'budget', 'revenue', 'title', 'popularity', 'release_date']]
        self.df_to_db(fact_df, 'movie_details', 'fact')

    def create_genre_report_data(self):
        sql = """ 
        SELECT 
        strftime('%Y', RELEASE_DATE) AS RELEASE_YEAR, 
        GENRE AS GENRE, 
        SUM(BUDGET) AS BUDGET, 
        SUM(REVENUE) AS REVENUE, 
        SUM(REVENUE - BUDGET) AS PROFIT,
        SUM(POPULARITY) AS POPULARITY

        FROM FACT_MOVIE_DETAILS
        JOIN DIM_GENRES
        ON DIM_GENRES.MOVIE_ID = FACT_MOVIE_DETAILS.MOVIE_ID

        GROUP BY RELEASE_YEAR, GENRE
        """
        report = pd.read_sql(sql, self.conn)
        report.to_csv('report/genre_data.csv', index=False)
        logging.info("Wrote genre data to report/genre_data.csv")

    def create_production_report_data(self):
        sql = """ 
                WITH PROD_REL_GENRE_BY_YEAR AS (
                    SELECT 
                        strftime('%Y', RELEASE_DATE) AS RELEASE_YEAR,
                        PROD_COMPANY_NAME,
                        SUM( CASE WHEN UPPER(GENRE) = 'ANIMATION' THEN 1 ELSE 0 END) AS N_ANIMATION ,
                        SUM( CASE WHEN UPPER(GENRE) = 'COMEDY' THEN 1 ELSE 0 END) AS N_COMEDY ,
                        SUM( CASE WHEN UPPER(GENRE) = 'FAMILY' THEN 1 ELSE 0 END) AS N_FAMILY ,
                        SUM( CASE WHEN UPPER(GENRE) = 'ADVENTURE' THEN 1 ELSE 0 END) AS N_ADVENTURE ,
                        SUM( CASE WHEN UPPER(GENRE) = 'FANTASY' THEN 1 ELSE 0 END) AS N_FANTASY ,
                        SUM( CASE WHEN UPPER(GENRE) = 'DRAMA' THEN 1 ELSE 0 END) AS N_DRAMA ,
                        SUM( CASE WHEN UPPER(GENRE) = 'ROMANCE' THEN 1 ELSE 0 END) AS N_ROMANCE ,
                        SUM( CASE WHEN UPPER(GENRE) = 'ACTION' THEN 1 ELSE 0 END) AS N_ACTION ,
                        SUM( CASE WHEN UPPER(GENRE) = 'CRIME' THEN 1 ELSE 0 END) AS N_CRIME ,
                        SUM( CASE WHEN UPPER(GENRE) = 'THRILLER' THEN 1 ELSE 0 END) AS N_THRILLER ,
                        SUM( CASE WHEN UPPER(GENRE) = 'HISTORY' THEN 1 ELSE 0 END) AS N_HISTORY ,
                        SUM( CASE WHEN UPPER(GENRE) = 'SCIENCE FICTION' THEN 1 ELSE 0 END) AS N_SCIENCE_FICTION ,
                        SUM( CASE WHEN UPPER(GENRE) = 'MYSTERY' THEN 1 ELSE 0 END) AS N_MYSTERY ,
                        SUM( CASE WHEN UPPER(GENRE) = 'HORROR' THEN 1 ELSE 0 END) AS N_HORROR ,
                        SUM( CASE WHEN UPPER(GENRE) = 'WAR' THEN 1 ELSE 0 END) AS N_WAR ,
                        SUM( CASE WHEN UPPER(GENRE) = 'FOREIGN' THEN 1 ELSE 0 END) AS N_FOREIGN ,
                        SUM( CASE WHEN UPPER(GENRE) = 'DOCUMENTARY' THEN 1 ELSE 0 END) AS N_DOCUMENTARY ,
                        SUM( CASE WHEN UPPER(GENRE) = 'WESTERN' THEN 1 ELSE 0 END) AS N_WESTERN ,
                        SUM( CASE WHEN UPPER(GENRE) = 'MUSIC' THEN 1 ELSE 0 END) AS N_MUSIC ,
                        SUM( CASE WHEN UPPER(GENRE) = 'TV MOVIE' THEN 1 ELSE 0 END) AS N_TV_MOVIE 
                        FROM FACT_MOVIE_DETAILS
                        JOIN DIM_GENRES
                            ON DIM_GENRES.MOVIE_ID = FACT_MOVIE_DETAILS.MOVIE_ID
                        JOIN DIM_PRODUCTION_COMPANIES
                            ON DIM_PRODUCTION_COMPANIES.MOVIE_ID = FACT_MOVIE_DETAILS.MOVIE_ID 
                        GROUP BY 
                        RELEASE_YEAR, 
                        PROD_COMPANY_NAME
                ),
                AGGREGATES AS (
                SELECT 
                    strftime('%Y', RELEASE_DATE) AS RELEASE_YEAR, 
                    PROD_COMPANY_NAME AS PROD_COMPANY_NAME, 
                    SUM(BUDGET) AS BUDGET, 
                    SUM(REVENUE) AS REVENUE, 
                    SUM(REVENUE - BUDGET) AS PROFIT, 
                    AVG(POPULARITY) AS POPULARITY
                FROM FACT_MOVIE_DETAILS
                JOIN DIM_GENRES
                    ON DIM_GENRES.MOVIE_ID = FACT_MOVIE_DETAILS.MOVIE_ID
                JOIN DIM_PRODUCTION_COMPANIES
                    ON DIM_PRODUCTION_COMPANIES.MOVIE_ID = FACT_MOVIE_DETAILS.MOVIE_ID
                GROUP BY RELEASE_YEAR, PROD_COMPANY_NAME
                )

                SELECT *
                FROM AGGREGATES
                LEFT JOIN PROD_REL_GENRE_BY_YEAR USING (RELEASE_YEAR, PROD_COMPANY_NAME)
        """
        report = pd.read_sql(sql, self.conn)
        report.to_csv('report/production_data.csv', index=False)
        logging.info("Wrote production data to report/production_data.csv")