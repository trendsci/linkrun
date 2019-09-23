#! /usr/bin/python3
# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State

import psycopg2 as pg

def printall(cur):
    for i in cur.fetchall():
        print(i)

conn = pg.connect(host=r"linkrundb.caf9edw1merh.us-west-2.rds.amazonaws.com",
                dbname="linkrundb", user="postgres", password="turtles21")

cur = conn.cursor()
cur.execute("""SELECT _1,_2 from linkrun.mainstats3
where _1 = 'www.dsw.com'
OR _1 = 'www.macys.com'
OR _1 = 'www.google.com'
OR _1 = 'google.com'
;""")
printall(cur)

cur.execute("""SELECT COUNT(*) from linkrun.mainstats3
;""")
printall(cur)

cur.execute("""SELECT * from linkrun.mainstats3
ORDER BY _2 DESC
LIMIT 20;""")

top_links = cur.fetchall()

#print([i for i in top100])


#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__)#, external_stylesheets=external_stylesheets)

app.layout = html.Div(id = "maindiv",children=[
    html.H1(children='LinkRun'),

    html.Div(children='''
        Website link-back popularity ranking.
    '''),

    dcc.Input(id='user_link', value='20', type='text'),

    html.Button("Submit (number or website)",id="search"),

    dash_table.DataTable(
        id='link_pupolarity',
        columns=[{"name":'Link',"id":"1"},{"name":'Number of linking pages',"id":"2"}],
        data=[{"1":a,"2":b} for a,b in top_links],
        editable=True
    ),

    # dcc.Graph(
    #     id='example-graph',
    #     figure={
    #         'data': [
    #             {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
    #             {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
    #         ],
    #         'layout': {
    #             'title': 'Dash Data Visualization'
    #         }
    #     }
    # )
])

@app.callback(
    Output(component_id='link_pupolarity', component_property='data'),
    [Input(component_id='search', component_property='n_clicks')],
    [State('user_link','value')]
)
def update_table(clicks,input_value):
    try:
        try:
            number = int(input_value)
            cur.execute("""SELECT * from linkrun.mainstats3
            ORDER BY _2 DESC
            LIMIT {};""".format(number))
            top_links = cur.fetchall()
            return [{"1":a,"2":b} for a,b in top_links]
        except:
            pass

        #print(input_value, input_value[:4])

        cur.execute("""SELECT * from linkrun.mainstats3
        WHERE _1 = '{}'
        OR _1 = '{}'
        ORDER BY _2 DESC
        LIMIT 5;""".format(input_value, input_value[4:])
        )

        top_links = cur.fetchall()
    except Exception as e:
        pass
        #print("exception: ",e)
    return [{"1":a,"2":b} for a,b in top_links]

if __name__ == '__main__':
    app.run_server()#debug=True)
