"""

Graphing library for CORE-WESM

Copyright (C) 2025 Leonhard Hofbauer, licensed under a MIT license

"""


import logging


import pandas as pd
#import geopandas as gpd
#import numpy as np
import plotly.io as pio
#from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
from plotly.offline import init_notebook_mode


init_notebook_mode()

#pio.renderers.default='browser'

pio.templates["fdes"] = go.layout.Template(
    layout=dict(font={"size": 10,
                      "family":"zplLF",
                      "color":"black"}),
    layout_colorway=px.colors.qualitative.T10
)

pio.templates.default = "plotly_white+fdes"



logger = logging.getLogger(__name__)


 

def plot_tech_sector(results, parameter, scenario,
                     naming = None,
                     mapping_tech_sector=None,
                     sector = None,
                     geography = None,
                     str_filter = None,
                     agg_years=None, xscale=None):
  
    ### get data
    
    data = results[scenario][parameter].copy()
    # ind = data.index.names
    # data = data.reset_index().fillna("NA")
    # data = data.set_index(ind)
    
    ### filter
    if sector is not None:
        # load lookups
        techlu = pd.read_excel(mapping_tech_sector,
                               index_col="sector")
        
        data = data.loc[data.index.get_level_values("TECHNOLOGY").str.endswith(tuple(techlu.xs(sector)["technology"].to_list()))]

    if geography is not None:
        
        data = data.loc[data.index.get_level_values("REGION").str.contains(geography)]
    
    if str_filter is not None:
        for k,f in str_filter.items():
            data = data.loc[data.index.get_level_values(k).str.contains('|'.join(f))]
    
    ### rename
    if naming is not None:
        data = data.rename(index=naming)
    
    ### groupby core technology and regions
    if "TECHNOLOGY" in data.index.names:
        ind = data.index.names
        data = data.reset_index()
        #data.loc[:,"TECHNOLOGY"] = data.loc[:,"TECHNOLOGY"].str[0:6]
        data = data.drop("REGION",axis=1)
        data = data.groupby([i for i in ind if i !="REGION"]).sum()
    
    
    if xscale is not None:
        ysa = pd.read_csv(agg_years,
                          index_col="VALUE")["AGG"]
        xscale=1/ysa.value_counts()
        xscale.index.name="YEAR"
        xscale.name="VALUE"
        data.loc[:,"VALUE"] = data["VALUE"].multiply(xscale,axis=0)
        
    ### define techs to plot

    techs = data.index.get_level_values("TECHNOLOGY").unique()

    fig = go.Figure()

    for i,t in enumerate(techs):
        fig.add_trace(go.Bar(x=data.loc[(t,slice(None))].index.get_level_values("YEAR").astype(str),
                                 y=data.loc[(t,slice(None)),"VALUE"],
                                 name=t,
                                 showlegend=True,
                                 )
                      )

    fig.update_layout(
        barmode="stack",
        xaxis=dict(
            title="Year",
        ),
      
        legend=dict(x=0.029, y=1.5, font_size=10,orientation="h",
                    yref="paper",
                    traceorder="reversed"),
        margin=dict(l=0, r=0, t=50, b=0),
    )

    # fig.write_image((pcfg["run"]["results_dir"]+pcfg["run"]["run_id"]
    #                  +"/"+scenario+"/figures/")+parameter+"_"+scenario+".pdf", engine="kaleido")
    
    fig.show()
