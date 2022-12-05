import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
import logging


class FilmManger:
    COLUMNS = ["id", "title", "director", "airing_date", "country", "adult", "american", "streaming_service", "duration"]
    
    def __init__(self, output_file: str, engine_path: str):
        self.output_file = output_file
        self.engine_path = engine_path

    def reset_file(self):
        # Reset file
        logging.info(f'Resetting output file: {self.output_file}')
        pd.DataFrame(columns=self.COLUMNS).to_parquet(self.output_file)

    def save_to_db(self):
        # Overwrite film table with file data
        logging.info(f'Saving data from {self.output_file} to database')
        df = pd.read_parquet(self.output_file)
        engine = create_engine(self.engine_path)
        df.to_sql('film', con=engine, if_exists='replace')

    def convert_date(self, date: str) -> str:
        if isinstance(date, str):
            return datetime.strptime(date, "%B %d, %Y").strftime("%Y-%m-%d")
        else:
            return ""

    def is_adult(self, rating) -> bool:
        if rating == "R" or rating == "TV-MA" or rating == "18+":
            return True
        # Otherwise including None
        else:
            return False

    def is_american(self, country) -> bool:
        # True if country is only US
        if country == "United States":
            return True
        # Otherwise including None
        else:
            return False

    def get_streaming_service(self, file_name: str) -> str:
        # Get the service from file name
        file_name = file_name.split(os.sep)[-1]
        return file_name.split("_")[0].capitalize()

    def import_csv(self, file_name: str):
        # Load dataframe from csv
        logging.info(f'Loading data from {file_name} into dataframe')
        df = pd.read_csv(file_name)

        logging.info('Processing dataframe')

        # Filter for films only
        df = df[(df["type"] == "Movie")]

        # Change the date format
        df.loc[:, "date_added"] = df.loc[:, "date_added"].apply(
            lambda x: self.convert_date(x)
        )
        df = df.rename(columns={"date_added": "airing_date"})

        # Change director values so that all values are strings
        df.loc[:, "director"] = df.loc[:, "director"].apply(
            lambda x: x if isinstance(x, str) else ""
        )

        # Insert new columns
        df.insert(0, "streaming_service", self.get_streaming_service(file_name))

        df.insert(0, "adult", False)
        df.loc[:, "adult"] = df.loc[:, "rating"].apply(
            lambda x: self.is_adult(x)
        )

        df.insert(0, "american", False)
        df.loc[:, "american"] = df.loc[:, "country"].apply(
            lambda x: self.is_american(x)
        )

        # Create unique id column based on the hash value of combined columns
        df.insert(0, "id", 0)
        df.loc[:, "id"] = df.loc[:, ["title", "director", "airing_date", "streaming_service"]].sum(axis=1).map(hash)

        # Load output file
        logging.info(f'Loading data from {self.output_file} into dataframe')
        output = pd.read_parquet(self.output_file)

        # Keep necessary columns and append to output
        logging.info(f'Appending and saving data to {self.output_file}')
        pd.concat([output, df[self.COLUMNS]], ignore_index=True).drop_duplicates(
            subset=["id"], ignore_index=True
        ).to_parquet(self.output_file)

    def run(self, input_files: list[str]):
        for file in input_files:
            self.import_csv(file)
