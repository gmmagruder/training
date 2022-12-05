from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Date

Base = declarative_base()


class Film(Base):
    __tablename__ = "film"

    index = Column('index', Integer, primary_key=True)
    title = Column(String)
    director = Column(String)
    airing_date = Column(Date)
    country = Column(String)
    adult = Column(Boolean)
    american = Column(Boolean)
    streaming_service = Column(String)
    duration = Column(String)

    def __int__(self, title, director, airing_date, country, adult, american, streaming_service, duration):
        self.title = title
        self.director = director
        self.airing_date = airing_date
        self.country = country
        self.adult = adult
        self.streaming_service = streaming_service
        self.duration = duration
