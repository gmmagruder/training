import luigi
from config import Config
from manager.film_manager import FilmManger
from task.populate_database_task import PopulateDatabaseTask

if __name__ == '__main__':
    config = Config()
    fm = FilmManger(config.output_file, config.engine_path)
    fm.reset_file()

    # fm.import_csv("files/netflix_titles.csv")
    # fm.run(["files/netflix_titles.csv", "files/disney_plus_titles.csv", "files/amazon_prime_titles.csv"])
    # fm.save_to_db()

    luigi.build([PopulateDatabaseTask(csv_list=["files/netflix_titles.csv", "files/disney_plus_titles.csv", "files/amazon_prime_titles.csv"],
                                      output_file=config.output_file,
                                      engine_path=config.engine_path
                                      )], workers=1, local_scheduler=True)
