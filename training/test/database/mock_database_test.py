from pytest import mark
from database.mock_database import MockDB
from sqlalchemy_utils.functions import database_exists


@mark.database
class MockDBTest:
    def create_db_test(self, config_obj):
        db = MockDB(config_obj.host, config_obj.user, config_obj.password, config_obj.port)
        db.create_db(config_obj.db_name)

        if database_exists(config_obj.engine_path):
            assert True
        else:
            assert False

    def create_twice_db_test(self, config_obj):
        db = MockDB(config_obj.host, config_obj.user, config_obj.password, config_obj.port)
        db.create_db(config_obj.db_name)
        db.create_db(config_obj.db_name)

        if database_exists(config_obj.engine_path):
            assert True
        else:
            assert False

    def drop_db_test(self, config_obj):
        db = MockDB(config_obj.host, config_obj.user, config_obj.password, config_obj.port)
        db.create_db(config_obj.db_name)
        db.drop_db(config_obj.db_name)

        if not database_exists(config_obj.engine_path):
            assert True
        else:
            assert False
