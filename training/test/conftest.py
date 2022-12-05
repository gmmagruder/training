import json
from pytest import fixture
from manager.film_manager import FilmManger
from config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database.mock_database import MockDB

config = Config("test")


@fixture(scope="function")
def file_manager():
    fm = FilmManger(config.output_file, config.engine_path)
    fm.reset_file()
    return fm


def load_csv_files(path):
    with open(path) as file:
        data = json.load(file)
        return data


@fixture(params=load_csv_files(config.test_file_path))
def csv_file(request):
    csv = request.param
    return csv


@fixture(params=load_csv_files(config.test_extra_file_path))
def extra_csv_files(request):
    csvs = request.param
    return csvs


@fixture(scope="session")
def all_csv_files():
    files = load_csv_files(config.test_file_path)
    return files


@fixture(scope="session")
def parquet_output_file():
    return config.output_file


@fixture(scope="function")
def db_session():
    engine = create_engine(config.engine_path)
    Session = sessionmaker(bind=engine)
    return Session()


@fixture(scope="session")
def mock_db():
    db = MockDB(config.host, config.user, config.password, config.port)
    db.create_db(config.db_name)

    yield db

    # Drop table after tests
    db.drop_db(config.db_name)


@fixture(scope="session")
def config_obj():
    return config
