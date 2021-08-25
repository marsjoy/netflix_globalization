#!/usr/bin/env python
# coding: utf-8

# In[378]:


import pandas as pd
import sqlite3 as sql
from sqlalchemy.types import String, Date, Text
from sqlalchemy import Table, Column, Integer, String, MetaData, Date, Index, ForeignKey, Time, create_engine
import json
import plotly.express as px
from datetime import datetime
import plotly.graph_objects as go


# In[316]:


pn.extension('tabulator')


# In[317]:


project_country = pd.read_csv('data/project_country.csv')


# In[318]:


project_state_milestones = pd.read_csv('data/project_state_milestones.csv',
                                       parse_dates=['due_date','state_entered_timestamp', 'state_completed_timestamp'],
                                       infer_datetime_format=True)


# In[377]:


project_state_milestones_meta = pd.read_csv('data/project_state_milestones_with_metadata.csv',
                                       parse_dates=['due_date','state_entered_timestamp', 'state_completed_timestamp'],
                                       infer_datetime_format=True)


# In[320]:


blocking = project_state_milestones[(project_state_milestones['late']==1) & (project_state_milestones['achieved']==0)].groupby('project_id').min()


# In[321]:


missed_deadlines = project_state_milestones[(project_state_milestones['late']==1) & (project_state_milestones['achieved']==1)].groupby('project_id').min()


# In[322]:


hit_deadlines = project_state_milestones[(project_state_milestones['late']==0) & (project_state_milestones['achieved']==1)].groupby('project_id').min()


# In[379]:


project_state_milestones_avg_days_to_complete = project_state_milestones[project_state_milestones['achieved']==1].groupby('state_id').days_to_complete.mean().to_frame().head()
project_state_milestones_avg_days_to_complete.rename(columns={'days_to_complete': 'Average Days to Complete'}, inplace=True)                                                           


# In[381]:


project_state_milestones_avg_days_late = project_state_milestones[project_state_milestones['late']==1].groupby('state_id').days_late.mean().to_frame().head()
project_state_milestones_avg_days_late.rename(columns={'days_late': 'Days'}, inplace=True)


# In[367]:


late_by_vendor = project_state_milestones_meta[project_state_milestones_meta['late']==True].groupby('vendor_name')[['state_id']].count()
late_by_vendor.rename(columns={'state_id': 'Total Missed Deadlines'}, inplace=True)


# In[368]:


hit_by_vendor = project_state_milestones_meta[project_state_milestones_meta['late']==False].groupby('vendor_name')[['state_id']].count()
hit_by_vendor.rename(columns={'state_id': 'Total Hit Deadlines'}, inplace=True)


# In[382]:


late_by_language = project_state_milestones_meta[project_state_milestones_meta['late']==False].groupby('language_name')[['state_id']].count()


# In[396]:


pn.config.sizing_mode='stretch_width'
map_fig = px.choropleth(project_country, locations="iso_alpha", color='total_projects',
                    hover_name="country", 
                    hover_data={"total_projects": True, "iso_alpha": False},
                    color_continuous_scale=px.colors.sequential.Aggrnyl,
                    labels={'total_projects': 'Total Projects'})
project_state_milestones_avg_days_to_complete_tab = pn.widgets.Tabulator(project_state_milestones_avg_days_to_complete,
                                                                         theme='bulma')
project_state_milestones_avg_days_late_tab = pn.widgets.Tabulator(project_state_milestones_avg_days_late,
                                                                  theme='bulma')
late_by_vendor_tab = pn.widgets.Tabulator(late_by_vendor,
                                          theme='bulma')
hit_by_vendor_tab = pn.widgets.Tabulator(hit_by_vendor,
                                          theme='bulma')
blocking_num = go.Figure(go.Indicator(
    mode = "number+delta",
    value = blocking.state_id.count()/project_state_milestones.state_id.count(),
    number = {'valueformat':'.0%'},
    delta = {'position': "top", 'reference': .1},
))
hit_num = go.Figure(go.Indicator(
    mode = "number+delta",
    value = hit_deadlines.state_id.count()/project_state_milestones.state_id.count(),
    number = {'valueformat':'.0%'},
    delta = {'position': "top", 'reference': 1}))
missed_num = go.Figure(go.Indicator(
    mode = "number+delta",
    value = missed_deadlines.state_id.count()/project_state_milestones.state_id.count(),
    number = {'valueformat':'.0%'},
    delta = {'position': "top", 'reference': 0}))

link = pn.pane.Markdown("""
###[See the code that generates the data and visualizations on Github](https://github.com/marsjoy/netflix_globalization){:target="_blank"}

------------
""", align='center',width=500)

golden = pn.template.GoldenTemplate(title='Localization Projects')
golden.main.append(
    pn.Column(
        pn.Row(
            link, align='center'
        ),
        pn.Row(
            pn.Card(map_fig, title='Projects by Movie County of Origin', align='center',background='WhiteSmoke')
        ),
        pn.Row(
            pn.Card(blocking_num, title='Percentage Blocking States', align='center',background='WhiteSmoke'),
            pn.Card(hit_num, title='Percentage Hit Deadlines', align='center',background='WhiteSmoke'),
            pn.Card(missed_num, title='Percentage Missed Deadlines', align='center',background='WhiteSmoke'),
        ),
        pn.Row(
            pn.Column(
                pn.Card(project_state_milestones_avg_days_to_complete_tab,
                        title='Avg Days to Complete by State',
                        align='center',background='WhiteSmoke')
            ),
            pn.Column(
                pn.Card(project_state_milestones_avg_days_late_tab,
                        title='Avg Days Late by State',
                        align='center',background='WhiteSmoke')),
            pn.Column(
                pn.Card(late_by_vendor_tab,
                        title='Missed Deadlines by Vendor',
                        align='center',background='WhiteSmoke')),
            pn.Column(
                pn.Card(hit_by_vendor_tab,
                        title='Hit Deadlines by Vendor',
                        align='center',background='WhiteSmoke')),
           sizing_mode='stretch_both')
    )
)


# In[398]:


golden.save('index.html')


# In[ ]:




