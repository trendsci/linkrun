#! /usr/bin/python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html

import psycopg2 as pg

conn = pg.connect(host=r"linkrundb.caf9edw1merh.us-west-2.rds.amazonaws.com",
                dbname="linkrundb", user="postgres", password="turtles21")

cur = conn.cursor()
cur.execute("""SELECT (_1,_2) from linkrun.mainstats
where _1 = 'www.dsw.com'
OR _1 = 'www.macys.com';""")
for i in cur.fetchall():
    print(i)

a="""

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
"""
