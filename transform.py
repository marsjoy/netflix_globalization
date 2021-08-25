#!/usr/bin/env python
# coding: utf-8

from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///netflix_demo.db')

movies = pd.read_sql_table('movie', engine)
movies.columns = ['movie_id', 'movie_title', 'movie_country']
movies.set_index('movie_id', inplace=True)

project = pd.read_sql_table('project', engine)
project.set_index('movie_id', inplace=True)
project.columns = ['project_id', 'vendor_id', 'language_id']

project_movie = project.join(movies, how='inner', on='movie_id')
project_movie.reset_index(inplace=True)
project_movie.set_index('vendor_id', inplace=True)

vendors = pd.read_sql_table('vendor', engine)
vendors.columns = ['vendor_id', 'vendor_name']
vendors.set_index('vendor_id', inplace=True)

project_movie_vendor = project_movie.join(vendors, how='inner', on='vendor_id')
project_movie_vendor.reset_index(inplace=True)
project_movie_vendor.set_index('language_id', inplace=True)

languages = pd.read_sql_table('language', engine)
languages.columns = ['language_id', 'language_code', 'language_name']
languages.set_index('language_id', inplace=True)

project_movie_vendor_language = project_movie_vendor.join(languages, how='inner', on='language_id')
project_movie_vendor_language.reset_index(inplace=True)

project_movie_vendor_language.to_csv('data/project_movie_vendor_language.csv', index=False)

project_movie_vendor_language.rename(columns={'movie_country': 'country'}, inplace=True)

by_country = project_movie_vendor_language.groupby(['country']).aggregate('count')[['project_id']]
by_country.rename(columns={'project_id': 'total_projects'}, inplace=True)

countries = pd.read_sql_table('country', engine)
countries.drop(['id', 'latitude', 'longitude'], axis=1, inplace=True)
countries.rename(columns={'name': 'country'}, inplace=True)
countries.set_index('country', inplace=True)

project_country = by_country.join(countries, how='inner', on='country')
project_country.reset_index(inplace=True)
project_country.to_csv('data/project_country.csv', index=False)

project_state_logs = pd.read_sql_table('project_state_log', engine,
                                       parse_dates={'timestamp': pd._libs.tslibs.timestamps.Timestamp})
project_state_logs.drop('id', axis=1, inplace=True)
project_state_logs.set_index(['project_id', 'state_id'], inplace=True)
project_state_logs.columns = ['state_entered_timestamp']

project_milestones = pd.read_sql_table('project_milestone', engine, parse_dates=['due_date'])
project_milestones.drop('id', axis=1, inplace=True)
project_milestones.set_index(['project_id', 'state_id'], inplace=True)

project_state_milestones = project_milestones.join(project_state_logs, how='left', on=['project_id', 'state_id'])

project_state_milestones_grouped = project_state_milestones.groupby(['project_id'])

project_state_milestones['state_completed_timestamp'] = project_state_milestones_grouped.state_entered_timestamp.shift(
    -1)
project_state_milestones['days_to_complete'] = project_state_milestones.apply(
    lambda x: pd.Int64Dtype().na_value if pd.isna(x.state_completed_timestamp) else int(
        (x.state_completed_timestamp - x.state_entered_timestamp).days),
    axis=1,
)
project_state_milestones['late'] = project_state_milestones.apply(
    lambda x: True if not pd.isna(x.state_completed_timestamp) and x.due_date < x.state_completed_timestamp
    else True if pd.isna(x.state_completed_timestamp) and x.due_date < datetime.today()
    else False,
    axis=1,
)
project_state_milestones['days_late'] = project_state_milestones.apply(
    lambda x: int((datetime.today() - x.due_date).days) if pd.isna(x.state_completed_timestamp) and x.late == 1
    else int((x.state_completed_timestamp - x.due_date).days) if x.late == 1
    else pd.Int64Dtype().na_value,
    axis=1,
)
project_state_milestones['achieved'] = project_state_milestones.apply(
    lambda x: False if pd.isna(x.state_completed_timestamp) else True,
    axis=1,
)

project_state_milestones.reset_index(inplace=True)
project_state_milestones.to_csv('data/project_state_milestones.csv', index=False)
project_state_milestones.set_index('project_id', inplace=True)

project_movie_vendor_language.set_index('project_id', inplace=True)

project_state_milestones_with_metadata = project_state_milestones.join(project_movie_vendor_language, how='inner',
                                                                       on=['project_id'])
project_state_milestones_with_metadata.reset_index(inplace=True)
project_state_milestones_with_metadata.to_csv('data/project_state_milestones_with_metadata.csv', index=False)
