#!/usr/bin/env python
# coding: utf-8

# In[493]:


# coding: utf-8
import pandas as pd
import panel as pn
import plotly.express as px
import plotly.graph_objects as go

pn.extension('tabulator')

engine = create_engine('sqlite:///netflix_demo.db')

states = pd.read_sql_table('project_state', engine)

states.columns = ['state_id', 'name']
states.set_index('state_id', inplace=True)

project_country = pd.read_csv('data/project_country.csv')
project_state_milestones = pd.read_csv('data/project_state_milestones.csv',
                                       parse_dates=['due_date', 'state_entered_timestamp', 'state_completed_timestamp'],
                                       infer_datetime_format=True)
project_state_milestones_meta = pd.read_csv('data/project_state_milestones_with_metadata.csv',
                                            parse_dates=['due_date', 'state_entered_timestamp',
                                                         'state_completed_timestamp'],
                                            infer_datetime_format=True)

blocking = project_state_milestones[
    (project_state_milestones['late'] == 1) & (project_state_milestones['achieved'] == 0)].groupby('project_id').min()

missed_deadlines = project_state_milestones[
    (project_state_milestones['late'] == 1) & (project_state_milestones['achieved'] == 1)].groupby('project_id').min()

hit_deadlines = project_state_milestones[
    (project_state_milestones['late'] == 0) & (project_state_milestones['achieved'] == 1)].groupby('project_id').min()

project_state_milestones_avg_days_to_complete = project_state_milestones[
    project_state_milestones['achieved'] == 1].groupby('state_id').days_to_complete.mean().to_frame().join(states, on='state_id', how='inner')
project_state_milestones_avg_days_to_complete.days_to_complete = project_state_milestones_avg_days_to_complete.days_to_complete.apply(lambda x: round(x))
project_state_milestones_avg_days_to_complete.reset_index(inplace=True)
project_state_milestones_avg_days_to_complete.drop(['state_id'], inplace=True, axis=1)
project_state_milestones_avg_days_to_complete = project_state_milestones_avg_days_to_complete[['name', 'days_to_complete']]
project_state_milestones_avg_days_to_complete.columns = ['State', 'Days']

project_state_milestones_avg_days_late = project_state_milestones[project_state_milestones['late'] == 1].groupby(
    'state_id').days_late.mean().to_frame().join(states, on='state_id', how='inner')
project_state_milestones_avg_days_late.rename(columns={'days_late': 'Days'}, inplace=True)
project_state_milestones_avg_days_late.Days = project_state_milestones_avg_days_late.Days.apply(lambda x: round(x))
project_state_milestones_avg_days_late.reset_index(inplace=True)
project_state_milestones_avg_days_late.drop(['state_id'], inplace=True, axis=1)
project_state_milestones_avg_days_late = project_state_milestones_avg_days_late[['name', 'Days']]
project_state_milestones_avg_days_late.columns = ['State', 'Days']

project_state_milestones_meta.reset_index(inplace=True)
project_state_milestones_meta_grouped = project_state_milestones_meta[project_state_milestones_meta['late'] == True].groupby('vendor_name')['state_id'].count().to_frame()
project_state_milestones_meta_grouped.reset_index(inplace=True)
project_state_milestones_meta_grouped.set_index('state_id',inplace=True)
project_state_milestones_grouped_state = project_state_milestones_meta_grouped.join(states, on='state_id', how='inner')
late_by_vendor = project_state_milestones_meta[project_state_milestones_meta['late'] == True].groupby('vendor_name').state_id.count().to_frame()
late_by_vendor.reset_index(inplace=True)
late_by_vendor.rename(columns={'state_id': 'Total Missed Deadlines', 'vendor_name': 'Vendor'}, inplace=True)

hit_by_vendor = project_state_milestones_meta[project_state_milestones_meta['late'] == False].groupby('vendor_name')[
    ['state_id']].count()
hit_by_vendor.reset_index(inplace=True)
hit_by_vendor.rename(columns={'state_id': 'Total Hit Deadlines', 'vendor_name': 'Vendor'}, inplace=True)

pn.config.sizing_mode = 'stretch_width'
map_fig = px.choropleth(project_country,
                        locations="iso_alpha",
                        color='total_projects',
                        hover_name="country",
                        hover_data={"total_projects": True, "iso_alpha": False},
                        color_continuous_scale=px.colors.sequential.Aggrnyl,
                        labels={'total_projects': 'Total Projects'})
project_state_milestones_avg_days_to_complete_tab = pn.widgets.Tabulator(project_state_milestones_avg_days_to_complete,
                                                                         theme='bulma',
                                                                         show_index=False)
project_state_milestones_avg_days_late_tab = pn.widgets.Tabulator(project_state_milestones_avg_days_late,
                                                                  theme='bulma',
                                                                  show_index=False)
late_by_vendor_tab = pn.widgets.Tabulator(late_by_vendor,
                                          theme='bulma',
                                          show_index=False)
hit_by_vendor_tab = pn.widgets.Tabulator(hit_by_vendor,
                                         theme='bulma',
                                         show_index=False)
blocking_num = go.Figure(go.Indicator(
    mode="number+delta",
    value=blocking.state_id.count() / project_state_milestones.state_id.count(),
    number={'valueformat': '.0%'},
    delta={'position': "top", 'reference': .1},
))
hit_num = go.Figure(go.Indicator(
    mode="number+delta",
    value=hit_deadlines.state_id.count() / project_state_milestones.state_id.count(),
    number={'valueformat': '.0%'},
    delta={'position': "top", 'reference': 1}))
missed_num = go.Figure(go.Indicator(
    mode="number+delta",
    value=missed_deadlines.state_id.count() / project_state_milestones.state_id.count(),
    number={'valueformat': '.0%'},
    delta={'position': "top", 'reference': 0}))

link = pn.pane.Markdown(
    """
    ###[See the code that generates the data and visualizations on Github](https://github.com/marsjoy/netflix_globalization){:target="_blank"}
    
    ------------
    """,
    align='center',
    width=500)

golden = pn.template.GoldenTemplate(title='Localization Projects')
golden.main.append(
    pn.Column(
        pn.Row(
            link, align='center'
        ),
        pn.Row(
            pn.Card(map_fig, title='Projects by Movie Country of Origin', align='center', background='WhiteSmoke')
        ),
        pn.Row(
            pn.Card(blocking_num, title='Percentage Blocking States', align='center', background='WhiteSmoke'),
            pn.Card(hit_num, title='Percentage Hit Deadlines', align='center', background='WhiteSmoke'),
            pn.Card(missed_num, title='Percentage Missed Deadlines', align='center', background='WhiteSmoke'),
        ),
        pn.Row(
            pn.Column(
                pn.Card(project_state_milestones_avg_days_to_complete_tab,
                        title='Avg Days to Complete by State',
                        align='center', background='WhiteSmoke')
            ),
            pn.Column(
                pn.Card(project_state_milestones_avg_days_late_tab,
                        title='Avg Days Late by State',
                        align='center', background='WhiteSmoke')),
            pn.Column(
                pn.Card(late_by_vendor_tab,
                        title='Missed Deadlines by Vendor',
                        align='center', background='WhiteSmoke')),
            pn.Column(
                pn.Card(hit_by_vendor_tab,
                        title='Hit Deadlines by Vendor',
                        align='center', background='WhiteSmoke')),
            sizing_mode='stretch_both')
    )
)

golden.save('index.html')

