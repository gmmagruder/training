from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from config import Config


config = Config()
app = Flask(__name__, template_folder='files/template')
app.config['SQLALCHEMY_DATABASE_URI'] = config.engine_path
db = SQLAlchemy(app)
db.init_app(app)


class Film(db.Model):
    index = db.Column('index', db.Integer, primary_key=True)
    title = db.Column(db.String)
    director = db.Column(db.String)
    airing_date = db.Column(db.Date)
    country = db.Column(db.String)
    adult = db.Column(db.Boolean)
    american = db.Column(db.Boolean)
    streaming_service = db.Column(db.String)
    duration = db.Column(db.String)

    def __int__(self, title, director, airing_date, country, adult, american, streaming_service, duration):
        self.title = title
        self.director = director
        self.airing_date = airing_date
        self.country = country
        self.adult = adult
        self.streaming_service = streaming_service
        self.duration = duration


with app.app_context():
    # Create table
    db.create_all()


@app.route('/')
def show_all():
    return render_template('show_all.html', films=Film.query.limit(10).all(), show_adult=True)


@app.route('/no-adult')
def no_adult():
    return render_template('show_all.html', films=Film.query.filter(text('adult = 0')).limit(10).all(), show_adult=False)


app.run()
