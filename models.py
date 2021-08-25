from sqlalchemy import Table, Column, Integer, String, ForeignKey, DateTime, Float
from sqlalchemy import MetaData

meta = MetaData()

movie = Table(
    'movie', meta,
    Column('id', Integer, primary_key=True),
    Column('title', String, index=True),
    Column('country', String, index=True),
)
vendor = Table(
    'vendor', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String, index=True),
)
language = Table(
    'language', meta,
    Column('id', Integer, primary_key=True),
    Column('code', String),
    Column('name', String, index=True),
)
project = Table(
    'project', meta,
    Column('id', Integer, primary_key=True),
    Column('movie_id', Integer, ForeignKey('movie.id')),
    Column('vendor_id', Integer, ForeignKey('vendor.id')),
    Column('language_id', Integer, ForeignKey('language.id')),
)
project_state = Table(
    'project_state', meta,
    Column('id', Integer, primary_key=True),
    Column('name', String, index=True)
)
project_state_log = Table(
    'project_state_log', meta,
    Column('id', Integer, primary_key=True),
    Column('project_id', Integer, ForeignKey('project.id')),
    Column('state_id', Integer, ForeignKey('project_state.id')),
    Column('timestamp', DateTime),
)
project_milestone = Table(
    'project_milestone', meta,
    Column('id', Integer, primary_key=True),
    Column('project_id', Integer, ForeignKey('project.id')),
    Column('state_id', Integer, ForeignKey('project_state.id')),
    Column('due_date', DateTime),
)
country = Table(
    'country', meta,
    Column('id', Integer, primary_key=True),
    Column('iso_alpha', String),
    Column('latitude', Float(10, 6)),
    Column('longitude', Float(10, 6)),
    Column('name', String, index=True)
)