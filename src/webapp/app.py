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
cur.execute("""
SELECT * FROM linkrun.temp500copy
ORDER BY _3 DESC
LIMIT 20;
""")
top_links = cur.fetchall()

#print([i for i in top100])


#external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__)#, external_stylesheets=external_stylesheets)

app.layout = \
    html.Div(id = "centering_div",
    style={'text-align':'center'},
    children=[
        html.Div(id = "maindiv",
        style={'width': '70%','display': 'inline-block'},
        children=[
        html.H1(children='LinkRun'),

        html.Div(children='''
            Website link-back popularity ranking
        ''',style={'font-size':'18px'}),
        #html.Br(),

        html.Pre(children='''
            Enter a number to view top sites, or a comma separated lists of sites.
            E.g. "20", "facebook.com,google.com", "target.com,walmart.com"
        '''),
        dcc.Input(
            id='user_link',
            value='20',
            type='text',
            style={'width':'100%',
                    'font-size':'18px'}
            ),

        html.Button("Submit (number or website)",
            id="search",
            style={'font-size':'14px'}
            ),

        html.Br(),html.Br(),

        dash_table.DataTable(
            id='link_pupolarity',
            columns=[{"name":'Link subdomain',"id":"1"},
            {"name":'Link domain.tld',"id":"2"},
            {"name":'Number of linking pages',"id":"3"}],
            data=[{"1":a,"2":b,"3":c} for a,b,c in top_links],
            editable=True
        )

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
]
)

@app.callback(
    [Output(component_id='link_pupolarity', component_property='data'),
    Output(component_id='link_pupolarity', component_property='columns')],
    [Input(component_id='search', component_property='n_clicks')],
    [State('user_link','value')]
)
def update_table(clicks,input_value):
    print("User input:\n",input_value,"\n")
    input_value_list = input_value.split(",")
    print("User input list:\n",input_value_list,"\n")
    try:
        try:
            number = int(input_value)
            cur.execute("""SELECT * from linkrun.temp500copy
            ORDER BY _3 DESC
            LIMIT {};""".format(number))
            top_links = cur.fetchall()
            return [{"1":a,"2":b,"3":c} for a,b,c in top_links], [{"name":'Link subdomain',"id":"1"},{"name":'Link domain.tld',"id":"2"},{"name":'Number of linking pages',"id":"3"}]
        except:
            pass

        #print(input_value, input_value[:4])

        # generate SQL statment:
        # WORKING ON THIS!
        sql_where_clause = "WHERE "
        for entry in input_value_list:
            sql_where_clause += "_2 = '{}' OR ".format(str(entry))
        else:
            sql_where_clause = sql_where_clause[:-3]
        # sql_command = """SELECT * from linkrun.temp500copy
        # {}
        # AND _3 > 1
        # ORDER BY _3 DESC
        # LIMIT 1000;""".format(sql_where_clause)
        sql_command = """SELECT _2, sum(_3) as sum_3 from linkrun.temp500copy
        {}
        --AND _3 > 1
        GROUP BY _2
        ORDER BY sum_3 DESC
        LIMIT 1000;""".format(sql_where_clause)
        print(sql_command)
        cur.execute(sql_command)

        top_links = cur.fetchall()
        return [{"2":a,"3":b} for a,b in top_links], [{"name":'Link domain.tld',"id":"2"},
        {"name":'Number of linking pages',"id":"3"}],
    except Exception as e:
        pass
        #print("exception: ",e)


if __name__ == '__main__':
    app.run_server()#debug=True)
