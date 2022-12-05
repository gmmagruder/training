import luigi
from luigi.mock import MockTarget
from task.film_manager_task import FilmManagerTask
from manager.film_manager import FilmManger


class PopulateDatabaseTask(luigi.Task):
    csv_list = luigi.ListParameter(default=[])
    output_file = luigi.Parameter()
    engine_path = luigi.Parameter()

    def output(self):
        return MockTarget(str(self))

    def requires(self):
        pre_tasks = []
        for csv in self.csv_list:
            pre_tasks.append(FilmManagerTask(input_csv=csv, output_file=self.output_file, engine_path=self.engine_path))

        return pre_tasks

    def run(self):
        fm = FilmManger(self.output_file, self.engine_path)
        fm.save_to_db()
