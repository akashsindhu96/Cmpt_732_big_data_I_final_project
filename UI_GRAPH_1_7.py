import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

df = pd.read_csv('/Users/bilalhussain/Downloads/Total_States.csv/accidents_total_for_all_states.csv')
lth= pd.read_csv('/Users/bilalhussain/Downloads/Accidents_by_severity.csv/accidents_by_severity.csv')
year_df = pd.read_csv('/Users/bilalhussain/Downloads/Count_by_Year.csv/year_count.csv')
month_df = pd.read_csv('/Users/bilalhussain/Downloads/Count_by_month.csv/accidents_month.csv')
weekday_df = pd.read_csv('/Users/bilalhussain/Downloads/Count_by_weekday.csv/week_days_count.csv')
hour_df = pd.read_csv('/Users/bilalhussain/Downloads/Count_by_hour.csv/count_hour.csv')
weather_df = pd.read_csv('/Users/bilalhussain/Downloads/by_month_weather.csv/weather_month.csv')


external_stylesheets = [dbc.themes.LUX]
nav_item = dbc.NavItem(dbc.NavLink('GITHUB REPO',href='https://github.com/Akashsindhu/Cmpt_732_big_data_I_final_project'))

navbar = dbc.Navbar(
    dbc.Container(
    [
        html.A(
            dbc.Row(
                [
                    dbc.Col(dbc.NavbarBrand("CMPT 732: Programming for Big Data Lab 1 - Group ABS", className="ml-2")),
                ],
                align="center",
                no_gutters=True,
            ),
            href="#",
        ),
        dbc.NavbarToggler(id="navbar-toggler"),
        dbc.Collapse(dbc.Nav([nav_item],className='ml-auto',navbar=True), id="navbar-collapse", navbar=True),
    ],
    ),
    color="dark",
    dark=True,
    className='mb-5'
)

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# GRAPH 1
# TOTAL ACCIDENT REPORTS (2016 - 2020)
fig = go.Figure(data=go.Choropleth(
    locations=df['State'],
    z=df['Total accidents in each state'].astype(float),
    locationmode='USA-states',
    colorscale='Viridis',
    autocolorscale=False,
    text=df['Total accidents in each state'],
    marker_line_color='black',
    colorbar_title="COUNT"
))
fig.update_layout(
    title_text='Total Accident Reports (2016 - 2020)',
    title_x= 0.5,
    geo = dict(
        scope='usa',
        projection=go.layout.geo.Projection(type = 'albers usa'),
        showlakes=True,
        lakecolor='rgb(255, 255, 255)'),
)

# GRAPH 2
# ACCIDENT BY SEVERITIES (LOW TO HIGH)
severity_count = lth[lth.Severity==1]
severities = pd.DataFrame({'4': 'Very High Severity',
    '3': 'High Severity',
    '2': 'Low Severity',
    '1': 'Very Low Severity'}, index=[0])
rows = 2
cols = 2
fig2 = make_subplots(
    rows=rows, cols=cols,
    specs = [[{'type': 'choropleth'} for c in np.arange(cols)] for r in np.arange(rows)],
    subplot_titles = list(severities.loc[0,:]))

for i, severity in enumerate(severities):
    result = lth.loc[lth['Severity']==int(severity)]
    fig2.add_trace(go.Choropleth(
        locations=result.State,
        z = result['Total Severity'],
        locationmode = 'USA-states',
        colorscale='Viridis',
        marker_line_color='black',
        zmin = 4,
        zmax = 75000,
        colorbar_title = "Number of Accidents"
    ), row = i//cols+1, col = i%cols+1)

fig2.update_layout(
    title_text = 'Accident by Severities (Low to High)', title_x = 0.5,
    **{'geo' + str(i) + '_scope': 'usa' for i in [''] + np.arange(2,rows*cols+1).tolist()},
    )
for index, trace in enumerate(fig.data):
    fig.data[index].hovertemplate = 'State: %{location}<br>Total Accidents: %{z:.2f}<extra></extra>'

# GRAPH 3
# ACCIDENT COUNT BY YEAR
fig3 = px.bar(year_df, y='ACCIDENTS', x='YEAR', text='ACCIDENTS',title='Accident Count per Year',height=600,color_discrete_sequence =['#692A99']*len(year_df),)
fig3.update_traces(texttemplate='%{text:.2s}', textposition='outside')
fig3.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5)

# GRAPH 4
# ACCIDENT COUNT BY MONTH
fig4 = px.bar(month_df, y='ACCIDENTS', x='MONTH', text='ACCIDENTS',title='Accident Count per Month',height=600,color_discrete_sequence =['#B1339E']*len(month_df),)
fig4.update_traces(texttemplate='%{text:.2s}', textposition='outside')
fig4.update_layout(uniformtext_minsize=10, uniformtext_mode='hide',title_x=0.5)

# GRAPH 5
# ACCIDENT COUNT BY DAY OF THE WEEK
fig5 = px.bar(weekday_df, y='total_accidents', x='week_day', text='total_accidents',labels={'week_day':'DAY','total_accidents':'ACCIDENTS'},title='Accident Count per Day',height=600,color_discrete_sequence =['#EA4F88']*len(weekday_df),)
fig5.update_traces(texttemplate='%{text:.2s}', textposition='outside')
fig5.update_layout(uniformtext_minsize=10, uniformtext_mode='hide',title_x=0.5)

# GRAPH 6
# ACCIDENT COUNT BY HOUR OF THE DAY
fig6 = px.bar(hour_df, y='total_accidents', x='hour_df',labels={'hour_df':'HOUR','total_accidents':'ACCIDENTS'},title='Accident Count per Hour',height=600,color_discrete_sequence =['#F98477']*len(hour_df),)
fig6.update_traces(texttemplate='%{text:.2s}', textposition='outside')
fig6.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5)

# GRAPH 7
# TOP 10 WEATHER CONDITIONS CAUSING ACCIDENTS
weather_df.mon_year = pd.to_datetime(weather_df.mon_year)
weather_df.mon_year = pd.to_datetime(weather_df.mon_year,format='%y/%d/%m')
fig7 = go.Figure()

for col in np.unique(weather_df.Weather_Condition):
    new_we= weather_df.loc[weather_df['Weather_Condition']==col]
    fig7.add_trace(go.Scatter(x=new_we.mon_year , y=new_we['weather_acc'],
                              name=col,
                              mode='markers+lines',
                              marker={"color": list(range(-3,10)), "cmid": 0},
                              ),
                   )

fig7.update_layout(yaxis_title='NUMBER OF ACCIDENTS',
                   paper_bgcolor='rgba(0,0,0,0)',
                   plot_bgcolor='rgba(0,0,0,0)',
                   template="seaborn",
                   margin=dict(t=0))



app.layout = html.Div([navbar,
    dbc.Container([
        dbc.Row([
            dbc.Col(html.H1("USA Accidents Analysis"), className="mb-2")
        ]),
        dbc.Row([
            dbc.Col(html.H6(children='Visualising accident trends across the United States'), className="mb-4")
        ]),

        dbc.Row([
            dbc.Col(dbc.Card(html.H3(children='GENERAL ACCIDENT TRENDS',
                                     className="text-center text-light bg-dark"), body=True, color="dark")
                    , className="mb-4")
        ]),
    dcc.Graph(figure=fig),
    dcc.Graph(figure=fig2),

        dbc.Row([
            dbc.Col(dbc.Card(html.H3(children='ACCIDENT COUNT (2016 - 2020)',
                                     className="text-center text-light bg-dark"), body=True, color="dark")
                    , className="mt-4 mb-5")
        ]),

        dcc.Graph(figure=fig3),
        dcc.Graph(figure=fig4),
        dcc.Graph(figure=fig5),
        dcc.Graph(figure=fig6),

    dbc.Row([
            dbc.Col(dbc.Card(html.H3(children='ACCIDENT TIMELINES',
                                     className="text-center text-light bg-dark"), body=True, color="dark")
                    , className="mt-4 mb-5")
        ]),
        dbc.Row([
            dbc.Col(html.H5(children='Accidents due to Top Ten Weather Conditions', className="text-center"),
                    className="mt-4")
        ]),
    dcc.Graph(figure=fig7),


    ]
    )
])

if __name__ == '__main__':
    app.run_server(host='127.0.0.1',port=4050 ,debug=True)