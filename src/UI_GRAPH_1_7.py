import pandas as pd
import plotly.express as px
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np


df = pd.read_csv('CSV_GRAPH_1_7/accidents_total_for_all_states.csv')
lth= pd.read_csv('CSV_GRAPH_1_7/accidents_by_severity.csv')
year_df = pd.read_csv('CSV_GRAPH_1_7/year_count.csv')
month_df = pd.read_csv('CSV_GRAPH_1_7/accidents_month.csv')
weekday_df = pd.read_csv('CSV_GRAPH_1_7/week_days_count.csv')
hour_df = pd.read_csv('CSV_GRAPH_1_7/count_hour.csv')
weather_df = pd.read_csv('CSV_GRAPH_1_7/weather_month.csv')
cities_df = pd.read_csv('CSV_GRAPH_1_7/top_cities.csv')
hl_df = pd.read_csv('CSV_GRAPH_1_7/l_h_visibility.csv')
day_df = pd.read_csv('CSV_GRAPH_1_7/day_time.csv')
houston_df = pd.read_csv('CSV_GRAPH_1_7/houston_sev.csv')
houston_streets= pd.read_csv('CSV_GRAPH_1_7/houston_streets.csv')
houston_features = pd.read_csv('CSV_GRAPH_1_7/houston_features.csv')

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


# GRAPH 8
# ACCIDENTS IN TOP 20 CITIES
fig8 = go.Figure()
fig8.add_trace(go.Heatmap(
z=cities_df['total_acc'],
x=cities_df['month'],
y=cities_df['City'],
colorscale='Viridis',
))
fig8.update_layout(
    title='Aggregated Monthly Accidents in Top 20 Cities',
    title_x = 0.5,
xaxis_nticks=36,
    height=600,)

# GRAPH 9
# LOW SEVERITITY ACCIDENTS DUE TO LOW VISIBILITY
low_df = hl_df.copy()
low_df= low_df.sort_values('Low')
fig9 = px.bar(low_df, y='States', x='Low',labels={'Low':'NUMBER OF ACCIDENTS','States':'STATES'},
                    color_discrete_sequence =['#ea4f88']*len(low_df),
              orientation='h')
fig9.update_traces(textposition='outside')
fig9.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5,height=600)

# GRAPH 10
# HIGH SEVERITY ACCIDENTS DUE TO LOW VISIBILITY
high_df = hl_df.copy()
high_df= high_df.sort_values('High')
fig10 = px.bar(high_df, y='States', x='High',labels={'High':'NUMBER OF ACCIDENTS','States':'STATES'},
               color_discrete_sequence =['#952ea0']*len(high_df),
               orientation='h')
fig10.update_traces(textposition='outside')
fig10.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5,height=600)

# GRAPH 11
# ACCIDENT TRENDS IN HOUSTON FOR TIME OF THE DAY
fig11 = px.bar(day_df, x="Weather_Condition", y="num_accidents",labels={'Weather_Condition':'WEATHER CONDITIONS',"num_accidents":"NUMBER OF ACCIDENTS",
                                                                        'hour':'TIME OF THE DAY'} ,color_discrete_sequence=px.colors.sequential.Magma,
               color='hour',title="Number of Accidents in Houston at Different Times of the Day due to various Weather Conditons")
fig11.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5,height=600)

#GRAPH 12
#NUMBERS OF ACCIDENTS IN HOUSTON AT DIFFERENT TIMES OF THE DAY WITH DIFFERENT SEVERITIES
fig12 = px.bar(houston_df, x="hour", y="total_acc_per_Sev",labels={'Severity':'Severity',"total_acc_per_Sev":"NUMBER OF ACCIDENTS",
                                                                        'hour':'TIME OF THE DAY'} ,color_discrete_sequence=px.colors.sequential.Magma,
               color='Severity',title="Number of Accidents in Houston at Different Times of the Day with Different Severities")
fig12.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5,height=600)

#GRAPH 13
#Top 20 Houston Streets for Accidents

fig13 = px.bar(houston_streets, y='Street', x='total_acc_in_streets',labels={'Street':'STREET','total_acc_in_streets': 'NUMBER OF ACCIDENTS'},
                    color_discrete_sequence =['#ea4f88']*len(houston_streets),title='Top 20 Houston Streets for Accidents',
              orientation='h')
fig13.update_traces(textposition='outside')
fig13.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5,height=600)

#GRAPH 14
houston_features=houston_features.sort_values('count',ascending=False)
fig14 = px.bar(houston_features, y='type', x='count',labels={'type':'NEARYBY ROAD FEATURES','count': 'NUMBER OF ACCIDENTS'},
                    color_discrete_sequence =['#952ea0']*len(houston_features),title='Common Road Features for Accidents in Houston',
              orientation='h')
fig14.update_traces(textposition='outside')
fig14.update_layout(uniformtext_minsize=8, uniformtext_mode='hide',title_x=0.5,height=500)



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

        dcc.Graph(figure=fig8),


    dbc.Row([
        dbc.Col(dbc.Card(html.H3(children='ACCIDENT TRENDS W.R.T VISIBILITY', className="text-center text-light bg-dark"), body=True, color="dark"),
                className="mt-4 mb-5")
        ]),
        dbc.Row([
            dbc.Col(html.H5(children='Low Severity Accidents due to Low Visibility', className="text-center"), className="mt-4")
        ]),
        dcc.Graph(figure=fig9),
        dbc.Row([
            dbc.Col(html.H5(children='High Severity Accidents due to Low Visibility', className="text-center"), className="mt-4")
        ]),
        dcc.Graph(figure=fig10),
dbc.Row([
        dbc.Col(dbc.Card(html.H3(children='ACCIDENT TRENDS IN THE MOST ACCIDENT PRONE CITY', className="text-center text-light bg-dark"), body=True, color="dark"),
                className="mt-4 mb-5")
        ]),
        dcc.Graph(figure=fig11),
        dcc.Graph(figure=fig12),
        dcc.Graph(figure=fig13),
        dcc.Graph(figure=fig14),

    ]
    )
])

if __name__ == '__main__':
    app.run_server(host='127.0.0.1',port=4050 ,debug=True)