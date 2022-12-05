from pytest import mark
from datetime import datetime
import pandas as pd
from model.film import Film
from manager.film_manager import FilmManger
import os


@mark.manager
class FilmManagerTest:
    def reset_file_test(self, config_obj, parquet_output_file):
        fm = FilmManger(config_obj.output_file, config_obj.engine_path)
        fm.reset_file()
        output = pd.read_parquet(parquet_output_file)
        assert len(output.index) == 0

    @mark.parametrize('date', [
        'September 21, 2000',
        'January 01, 1999',
        'December 31, 2021',
        None
    ])
    def convert_date_test(self, file_manager, date):
        if isinstance(date, str):
            assert file_manager.convert_date(date) == datetime.strptime(date, "%B %d, %Y").strftime("%Y-%m-%d")
        else:
            assert file_manager.convert_date(date) == ""

    @mark.parametrize('rating', [
        'R',
        'TV-MA',
        '18+'
    ])
    def is_adult_true_test(self, file_manager, rating):
        assert file_manager.is_adult(rating) is True

    @mark.parametrize('rating', [
        'PG-13',
        'PG',
        'G',
        None
    ])
    def is_adult_false_test(self, file_manager, rating):
        assert file_manager.is_adult(rating) is False

    @mark.parametrize('country', [
        'United States'
    ])
    def is_american_true_test(self, file_manager, country):
        assert file_manager.is_american(country) is True

    @mark.parametrize('country', [
        'United Kingdom',
        'Canada',
        'France, United States, United Kingdom',
        None
    ])
    def is_american_false_test(self, file_manager, country):
        assert file_manager.is_american(country) is False

    def get_streaming_service_test(self, file_manager, csv_file):
        assert file_manager.get_streaming_service(csv_file) == csv_file.split(os.sep)[-1].split("_")[0].capitalize()

    def import_csv_test(self, file_manager, csv_file, parquet_output_file):
        file_manager.import_csv(csv_file)
        output = pd.read_parquet(parquet_output_file)
        assert len(output.index) == 3

    def import_csv_duplicate_test(self, file_manager, csv_file, parquet_output_file):
        file_manager.import_csv(csv_file)
        file_manager.import_csv(csv_file)
        output = pd.read_parquet(parquet_output_file)
        assert len(output.index) == 3

    def import_csv_addition_test(self, file_manager, extra_csv_files, parquet_output_file):
        file_manager.import_csv(extra_csv_files[0])
        file_manager.import_csv(extra_csv_files[1])
        output = pd.read_parquet(parquet_output_file)
        assert len(output.index) == 5

    def run_test(self, file_manager, all_csv_files, parquet_output_file):
        file_manager.run(all_csv_files)
        output = pd.read_parquet(parquet_output_file)
        assert len(output.index) == 9

    @mark.integration
    @mark.database
    def save_to_db_test(self, file_manager, all_csv_files, db_session, mock_db):
        file_manager.run(all_csv_files)
        file_manager.save_to_db()
        rows = db_session.query(Film).count()
        db_session.close()
        assert rows == 9
