import luigi
from manager.film_manager import FilmManger
from luigi.mock import MockTarget


class FilmManagerTask(luigi.Task):
    input_csv = luigi.Parameter()
    output_file = luigi.Parameter()
    engine_path = luigi.Parameter()

    def output(self):
        return MockTarget(str(self))

    def run(self):
        fm = FilmManger(self.output_file, self.engine_path)
        fm.import_csv(self.input_csv)

        with self.output().open("w") as f:
            f.write(f"Finished task")

