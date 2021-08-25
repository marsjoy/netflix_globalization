# -*- coding: utf-8 -*-

import csv
import json
import os
from datetime import datetime, timedelta
from random import randrange, seed

import pandas as pd
from sqlalchemy import create_engine

from models import movie, language, project, project_state_log, project_state, vendor, meta, country, project_milestone

if os.path.exists('netflix_demo.db'):
    os.remove('netflix_demo.db')
if os.path.exists('data/project_milestone.csv'):
    os.remove('data/project_milestone.csv')
if os.path.exists('data/project_state_log.csv'):
    os.remove('data/project_state_log.csv')

netflix_df = pd.read_csv('data/netflix_titles.csv')
movies = netflix_df[['title', 'country']]
with open('data/iso_codes.json') as iso_codes:
    languages = pd.DataFrame.from_dict([json.loads(iso_codes.read())]).T.reset_index()
    languages.columns = ['code', 'name']
vendors = pd.read_csv('data/vendors.csv')
countries = pd.read_csv('data/countries.csv')

engine = create_engine('sqlite:///netflix_demo.db')
meta.create_all(engine)

seed_movie_query = movie.insert().values(movies.to_dict('records'))
engine.execute(seed_movie_query)
seed_language_query = language.insert().values(languages.to_dict('records'))
engine.execute(seed_language_query)
seed_vendor_query = vendor.insert().values(vendors.to_dict('records'))
engine.execute(seed_vendor_query)

vendors = pd.read_sql_table('vendor', engine)
movies = pd.read_sql_table('movie', engine)
languages = pd.read_sql_table('language', engine)
projects = pd.concat([movies[['id']],
                      vendors[['id']].sample(n=7787, replace=True, ignore_index=True),
                      languages[['id']].sample(n=7787, replace=True, ignore_index=True)], axis=1)
projects.columns = ['movie_id', 'vendor_id', 'language_id']
seed_project_query = project.insert().values(projects.to_dict('records'))
engine.execute(seed_project_query)
seed_country_query = country.insert().values(countries.to_dict('records'))
engine.execute(seed_country_query)

state_ids = tuple(range(6))
seed(0)


def state_logs():
    today = datetime.today()
    first = datetime(year=2021, month=1, day=1)
    day_count = (today - first).days
    date = first + timedelta(days=randrange(day_count))
    state_id = state_ids[0]
    while date < today and state_id < state_ids[-1]:
        yield state_id, date
        state_id += 1
        date += timedelta(days=7 + randrange(-4, 6))


projects = pd.read_sql_table('project', engine)
project_ids = projects['id'].to_list()

with open(r'data/project_state_log.csv', 'a') as logs:
    project_state_log_writer = csv.writer(logs)
    project_state_log_writer.writerow(('project_id', 'state_id', 'timestamp'))
    with open(r'data/project_milestone.csv', 'a') as milestones:
        project_milestone_writer = csv.writer(milestones)
        project_milestone_writer.writerow(('project_id', 'state_id', 'due_date'))
        for project_id in project_ids:
            for log_state_id, log_date in state_logs():
                project_state_log_writer.writerow((project_id, log_state_id, log_date))
                if log_state_id != state_ids[-1]:
                    milestone_date = log_date + timedelta(days=10)
                    milestone_state_id = log_state_id + 1
                    project_milestone_writer.writerow((project_id, milestone_state_id, milestone_date))

seed_project_states_query = project_state.insert().values([(0, 'Requirement Gathering',),
                                                           (1, 'Asset Creation',),
                                                           (2, 'Translation',),
                                                           (3, 'Subtitle Engineering',),
                                                           (4, 'QA',),
                                                           (5, 'Completion',)])
engine.execute(seed_project_states_query)

project_state_logs = pd.read_csv('data/project_state_log.csv',
                                 parse_dates=['timestamp'], infer_datetime_format=True,
                                 dtype={'project_id': 'int', 'state_id': 'int', 'timestamp': 'str'})
project_milestones = pd.read_csv('data/project_milestone.csv', parse_dates=['due_date'], infer_datetime_format=True,
                                 dtype={'project_id': 'int', 'state_id': 'int', 'due_date': 'str'})

seed_project_state_log_query = project_state_log.insert().values(project_state_logs.to_dict('records'))
engine.execute(seed_project_state_log_query)

seed_project_milestone_query = project_milestone.insert().values(project_milestones.to_dict('records'))
engine.execute(seed_project_milestone_query)
