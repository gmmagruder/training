class Config:
    def __init__(self, mode="dev"):
        self.output_file = {
            "dev": "files/cleaned_films.parquet",
            "test": "../files/cleaned_films.parquet"
        }[mode]

        self.test_file_path = {
            "dev": "test/test_files/test_data.json",
            "test": "test_files/test_data.json"
        }[mode]

        self.test_extra_file_path = {
            "dev": "test/test_files/test_extra_data.json",
            "test": "test_files/test_extra_data.json"
        }[mode]

        self.host = {
            "dev": "127.0.0.1",
            "test": "127.0.0.1"
        }[mode]

        self.port = {
            "dev": "3306",
            "test": "3306"
        }[mode]

        self.user = {
            "dev": "USERNAME",
            "test": "USERNAME"
        }[mode]

        self.password = {
            "dev": "PASSWORD",
            "test": "PASSWORD"
        }[mode]

        self.db_name = {
            "dev": "film",
            "test": "test"
        }[mode]

        self.table_name = {
            "dev": "film",
            "test": "test"
        }[mode]

        self.engine_path = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
