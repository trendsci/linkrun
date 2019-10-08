#! /usr/bin/python3
# -*- coding: utf-8 -*-

import os
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from dash.dependencies import Input, Output, State

import psycopg2 as pg

# Get passwords from environment variables
jdbc_password = os.environ['POSTGRES_PASSWORD']
jdbc_user = os.environ['POSTGRES_USER']
jdbc_host = os.environ['POSTGRES_LOCATION']

column_names = ['_1','_2','sum_3']
col1, col2, col3 = column_names
table_name = r"linkrunstatic.ccjuly2019_full"#'linkrunprod1.linkrundb_30_7_2019_first_last_16001_24000'#'linkrun.temp500copy'
table_name_grouped = r"linkrunstatic.ccjuly2019_full_grouped_ranked"

def printall(cur):
    for i in cur.fetchall():
        print(i)

def pretty_rank(rank_value):
    #pretty_rank_text = r"▮▮▮▯▯▯▯▯▯▯ 30%"
    rank_1_to_10 = round(rank_value*10)
    pretty_rank_text = u'\u25FC'*rank_1_to_10 + u'\u25FB'*(10-rank_1_to_10)
    pretty_rank_number = "("+str(rank_1_to_10*10)+"%)"
    pretty_rank_text += " {:>6}".format(pretty_rank_number)
    return pretty_rank_text
    #return "hello {:.2f}".format(rank_value)

conn = pg.connect(host= jdbc_host,
                dbname="linkrundb", user=jdbc_user, password=jdbc_password)

cur = conn.cursor()
# Preload data on page with top domains
cur.execute("""
            SELECT * FROM {table_name}
            ORDER BY {col3} DESC
            LIMIT 20;
            """.format(col3=col3,table_name=table_name) )
top_links = cur.fetchall()

external_stylesheets = ['https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = \
    html.Div(id = "centering_div",
    style={'text-align':'center'},
    children=[

        html.Div(id = "maindiv",
            className="mt-2",
            style={'width': '80%','max-width': '600px','display': 'inline-block'},
            children=[

            html.H1(children='LinkRun'),

            html.Div(children='''
                Website popularity ranking
                ''',
                style={'font-size':'18px'}
                ),

            html.Br(),

            html.Details([
                html.Summary('Not sure what to do? Click here.'),
            dcc.Markdown(children=[
            '''
            LinkRun ranks website popularity based on the number of pages linking to them.

            To search the LinkRun database: enter a number to view top sites (e.g. 20),
            a domain name (e.g. "facebook.com"), or a comma separated lists of domains (e.g. "facebook.com,google.com", "target.com,walmart.com")
            '''],
            style={'text-align':'left'})
            ])

            ,
            dcc.Input(
                id='user_link',
                className="form-group form-control",
                value='20',
                type='text',
                style={'width':'100%',
                        'font-size':'18px'}
                ),
            html.Div(id='submit', children=[
            html.Button("Submit (number or website)",
                id="search",
                className="btn btn-primary",
                style={'font-size':'14px','display': 'inline-block'}
                ),

            dcc.Checklist(id="group_by_domain",
                options=[
                {'label': 'Group all subdomains & display ratings', 'value': 'group'}
                ],
                value=[],
                )],
            ),

            #html.Br(),

            html.Div(
            dash_table.DataTable(
                id='link_pupolarity',
                columns=[{"name":'Link subdomain',"id":"1"},
                {"name":'Link domain.tld',"id":"2"},
                {"name":'Number of linking pages',"id":"3"}],
                data=[{"1":a,"2":b,"3":c} for a,b,c in top_links],

                style_header={'fontWeight': 'normal', 'textAlign': 'center'},

                style_table={'max-height': '500px',
                             'overflowY': 'auto'},

                style_cell={'height': 'auto',
                            'minWidth': '180px', 'width': '180px', 'maxWidth': '180px',
                            'whiteSpace': 'normal',
                            'textAlign': 'left'},

                editable=False,
                #filter_action="native",
                sort_action="native",
                sort_mode="multi",
                page_action='native',
                page_current= 0,
                page_size= 200,
                fixed_rows={'headers': True, 'data':0},
            ),
            style={"max-width":"100%"}
            )


])
]
)

@app.callback(
    [Output(component_id='link_pupolarity', component_property='data'),
    Output(component_id='link_pupolarity', component_property='columns'),
    Output(component_id='link_pupolarity', component_property='page_current')],
    [Input(component_id='search', component_property='n_clicks')],
    [State('user_link','value'),
    State('group_by_domain','value')]
)
def update_table(clicks,input_value,group_by_domain):

    columns_3 = [{"name":'Link subdomain',"id":"1"},
    {"name":'Link domain.tld',"id":"2"},
    {"name":'Number of linking pages',"id":"3"}]

    columns_3_rank = [{"name":'Link domain.tld',"id":"2"},
    {"name":'Number of linking pages',"id":"3"},
    {"name":'LinkRun Rank',"id":"4"}]

    columns_2 = [{"name":'Link domain.tld',"id":"2"},
    {"name":'Number of linking pages',"id":"3"}]

    print("\nUser clicked submit.")
    print("Group by domain:",group_by_domain)
    print("User input:",input_value)
    print("==============\n")

    input_value_list = input_value.split(",")
    print("User input list:\n",input_value_list,"\n")


    #top_links = [(r'<a href="www.google.com">Google Link</a>','here',2)]
    #return [{"1":a,"2":b,"3":c} for a,b,c in top_links], columns_3

    # Check if user entered a number
    try:
        limit_number = int(input_value)

        # if number > 10 million  or < 1, give user an error
        if limit_number > 1000:
            return [{"1":"Error","2":"Try a smaller number","3":""}],columns_3,0
        elif limit_number < 1:
            return [{"1":"Error","2":"Try a bigger number","3":""}],columns_3,0

        cur.execute("""SELECT * from {table_name}
        ORDER BY {col3} DESC
        LIMIT {limit_number};""".format(table_name=table_name,
                                        limit_number=limit_number,
                                        col3=col3)
                    )

        top_links = cur.fetchall()

        return [{"1":a,"2":b,"3":c} for a,b,c in top_links], columns_3,0

    except:
        # User input was not a number
        pass

    # If user input is not a number:
    try:
        # lowercase user input
        input_value_list = [val.lower().strip() for val in input_value_list]

        # If user wants to group all entries by domain
        if group_by_domain:
            # generate sql GROUP BY statement
            group_by = "GROUP BY {col2}".format(col2=col2)
            # generate sql WHERE statement
            sql_where_clause = "WHERE "
            for entry in input_value_list:
                sql_where_clause += "{col2} = $${entry}$$ OR ".format(
                                col2=col2, entry=str(entry))
            else:
                sql_where_clause = sql_where_clause[:-3]

            # sql_command = """
            # SELECT {col2}, sum({col3}) as sum_3 from {table_name}
            # {sql_where_clause}
            # {group_by}
            # ORDER BY sum_3 DESC
            # LIMIT 1000;""".format(
            #                     col2=col2,
            #                     col3=col3,
            #                     table_name=table_name_group,
            #                     sql_where_clause=sql_where_clause,
            #                     group_by=group_by
            #                     )

            #testing with ranking using grouped table
            sql_command = """
            SELECT {col2}, {col3}, linkrunrank from {table_name}
            {sql_where_clause}
            ORDER BY sum_3 DESC
            LIMIT 1000;""".format(
                                col2=col2,
                                col3=col3,
                                table_name=table_name_grouped,
                                sql_where_clause=sql_where_clause,
                                group_by=group_by
                                )
            print("SQL COMMAND:\n",sql_command)
            cur.execute(sql_command)

            top_links = cur.fetchall()
            print(top_links)
            #returning 3 columns
            return [{"2":a,"3":b,"4":pretty_rank(c)} for a,b,c in top_links], columns_3_rank, 0
            #return [{"2":a,"3":b} for a,b in top_links], columns_2, 0

        # if user does not want to group by domain:
        else:
            sql_where_clause = "WHERE "
            for entry in input_value_list:
                sql_where_clause += "{col2} = $${entry}$$ OR ".format(
                                col2=col2, entry=str(entry))
            else:
                sql_where_clause = sql_where_clause[:-3]
            print(sql_where_clause)
            sql_command = """
            SELECT {col1}, {col2}, {col3} from {table_name}
            {sql_where_clause}
            ORDER BY {col3} DESC
            LIMIT 1000;""".format(
                                col1=col1,
                                col2=col2,
                                col3=col3,
                                table_name=table_name,
                                sql_where_clause=sql_where_clause,
                                )
            print("SQL COMMAND:\n",sql_command)
            cur.execute(sql_command)

            top_links = cur.fetchall()
            return [{"1":a,"2":b,"3":c} for a,b,c in top_links], columns_3,0

    except Exception as e:
        pass

# from dash forum
#app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/bWLwgP.css"})
#app.css.append_css({"external_url": "https://codepen.io/chriddyp/pen/brPBPO.css"})


if __name__ == '__main__':
    app.run_server(host='0.0.0.0',port="8050",debug=True)
    #app.run_server(debug=True)
