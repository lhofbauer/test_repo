"""

Graphing library for a clean cooking analysis using CORE-WESM

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
from plotly.subplots import make_subplots

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



def plot_national_overview(results,
                           parameter,
                           scenarios,
                           dcfg,
                           naming = None,
                           col = None,
                           agg_years=None,
                           xscale=None
                          ):
    


    
    ### get data
    data = pd.concat([results[s][parameter]
                      for s in scenarios],keys=scenarios, names=["SCENARIO"])
    # ind = data.index.names
    # data = data.reset_index().fillna("NA")
    # data = data.set_index(ind)
    
    ### filter years
    
    data = data.loc[data.index.get_level_values("YEAR")>=2020]
    
    ### filter for total, rural, and urban
    data_al = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK")]
    data_ru = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK2")]
    data_ur = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK1")]
    
    data = pd.concat([data_al,
                      data_ru,
                      data_ur],keys=["Total","Rural","Urban"],
                     names=["AREA"])
    
    ### groupby core technology and regions
    ind = data.index.names
    data = data.reset_index()
    data.loc[:,"TECHNOLOGY"] = data.loc[:,"TECHNOLOGY"].str[3:]
    data = data.drop("REGION",axis=1)
    data = data.groupby([i for i in ind if i !="REGION"]).sum()
    
    ### rename
    if naming is not None:
        data = data.rename(index=naming)
    

    xscale=True
    if xscale is not None:
        ysa = pd.read_csv(agg_years,
                          index_col="VALUE")["AGG"]
        xscale=1/ysa.value_counts()
        xscale.index.name="YEAR"
        xscale.name="VALUE"
        data.loc[:,"VALUE"] = data["VALUE"].multiply(xscale,axis=0)
        
        
    ### define techs to plot

    techs = data.index.get_level_values("TECHNOLOGY").unique()
    techs = ['Electric coil','Electric induction', 'Electric pressure',
             'LPG',
             'Biofuel','Biogas', 'Firewood', 'Imp. firewood',
             'Metallic charcoal', 'Imp. charcoal', 
           'Kerosene' ]
    
    areas = ["Total","Urban","Rural"]
    fig = make_subplots(rows=len(areas), cols=len(scenarios),
                        shared_yaxes="all",
                        shared_xaxes="columns",
                        row_titles=areas,
                        column_titles=scenarios,
                        horizontal_spacing=0.05,
                        vertical_spacing=0.05)

    for r,a in enumerate(areas):
        for c,s in enumerate(scenarios):
            for i,t in enumerate(techs):
                fig.add_trace(go.Bar(x=data.loc[(a,s,t,slice(None))].index.get_level_values("YEAR").astype(str),
                                         y=data.loc[(a,s,t,slice(None)),"VALUE"],
                                         name=t,
                                         marker_color = col[t],
                                         showlegend=True if c==0 and r==0 else False,
                                         ),
                              row=r+1,col=c+1
                              )
    fig.update_xaxes(title="Year", row=len(areas))
    fig.update_yaxes(title="Production (PJ)", col=1)
    
    fig.update_layout(
        barmode="stack",
        #xaxis=dict(
        #    title="Year",
        #),
        #title=scenario if geography is None else scenario+": "+geography,      
        #legend=dict(x=0.029, y=1.5, font_size=10,orientation="h",
        #            yref="paper",
        #            traceorder="reversed"),
        margin=dict(l=0, r=0, t=25, b=0),
        width=800,
    )

    #fig.write_image("figures/national.png", engine="kaleido")
    fig.show()


def plot_counties(results,
                  parameter,
                 scenarios,
                 counties,
                 list_counties,
                 dcfg,
                 naming = None,
                 col = None,
                 agg_years=None,
                 xscale=None
                ):


    
    ### get data
    data = pd.concat([results[s][parameter]
                      for s in scenarios],keys=scenarios, names=["SCENARIO"])

    
    ### filter years
    years = [2020,2025,2030,2035,2040,2045,2050]
    data = data.loc[data.index.get_level_values("YEAR").isin(years)]
    
    ### filter for total, rural, and urban
    data_al = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK")]
    data_ru = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK2")]
    data_ur = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK1")]
    
    data = pd.concat([data_al,
                      data_ru,
                      data_ur],keys=["Total","Rural","Urban"],
                     names=["AREA"])
    
    ### groupby core technology and regions
    ind = data.index.names
    data = data.reset_index()
    data.loc[:,"TECHNOLOGY"] = data.loc[:,"TECHNOLOGY"].str[3:]
    data = data.groupby([i for i in ind]).sum()
    
    ### rename
    # load county list
    list_counties = pd.read_csv(list_counties,
                           keep_default_na=False)
    counties_ren = list_counties.loc[:,("ID","NAME")].set_index("ID")["NAME"]
    
    if naming is not None:
        data = data.rename(index=naming)
        data = data.rename(index=counties_ren)
    
    ### calculate relative
    relative = "TECHNOLOGY"
    data = data/data.groupby([i for i in data.index.names
                              if i not in relative]).sum()
    
    ### aggregate techs
    agg = {"Electric coil" : "Electric",
               "Electric induction" : "Electric",
               "Electric pressure" : "Electric"}
    cgroupby = {"TECHNOLOGY":agg}
    # groupby content of level based on function or dict
    if cgroupby is not None:
        for k,v in cgroupby.items():
            idx = data.index.to_frame()
            if isinstance(v, dict):
                agg = data.index.get_level_values(k).to_series().replace(v).to_list()
            if callable(v):
                agg = [v(i) for i in data.index.get_level_values(k)]
            
            idx = idx.rename(columns={k:k+"_"})
            idx.insert(list(idx.columns).index(k+"_"), k, agg)
            data.index = pd.MultiIndex.from_frame(idx)
    
        data = data.groupby([l for l in data.index.names if l[:-1] not in list(cgroupby.keys())]).sum()
        

    # xscale=True
    # if xscale is not None:
    #     ysa = pd.read_csv(agg_years,
    #                       index_col="VALUE")["AGG"]
    #     xscale=1/ysa.value_counts()
    #     xscale.index.name="YEAR"
    #     xscale.name="VALUE"
    #     data.loc[:,"VALUE"] = data["VALUE"].multiply(xscale,axis=0)
        
        
    ### define techs to plot

    techs = data.index.get_level_values("TECHNOLOGY").unique()
    techs = ['Electric', 
             'LPG',
             'Biofuel',
             'Firewood', 
             'Imp. firewood']
    fig = make_subplots(rows=len(techs), cols=len(scenarios),
                        shared_yaxes="all",
                        shared_xaxes="columns",
                        row_titles=techs,
                        column_titles=scenarios,
                        horizontal_spacing=0.05,
                        vertical_spacing=0.05)
    
    scs = ['#AAAA00','#EE8866','#77AADD','#99DDFF',
           '#EEDD88','#44BB99','#FFAABB']
    ms = ["square","triangle-up","pentagon","diamond"]
    
    # filter for existing counties and rename if required, cap at 4
    counties = ([c for c in counties if c in counties_ren.values]
                +[counties_ren.loc[c] for c in counties if c in counties_ren.index])
    counties = counties[:4]
    
    for i,s in enumerate(scenarios):
        for ii,t in enumerate(techs):
            fig.add_trace(go.Box(
                y=data.loc[("Total",s,slice(None),t),"VALUE"],
                x=data.loc[("Total",s,slice(None),t)].index.get_level_values("YEAR").astype(str),
                #name=t,
                #boxpoints="all",
                #boxpoints=False,
                #fillcolor=col[t],
                line=dict(color="DarkSlateGrey"),
                #offsetgroup=t,
                # legendgroup="scenarios",
                # legendgrouptitle=dict(text="Scenario",
                #                       font=dict(size=12)),
                #marker_color='#3D9970'
                showlegend=False if i==0 else False
            ),
                row=ii+1,col=i+1)
            
            for iii,c in enumerate(counties):
                    fig.add_trace(go.Scatter(
                                y=data.loc[("Total",s,c,t),"VALUE"],
                                x=data.loc[("Total",s,c,t)].index.get_level_values("YEAR").astype(str),
                                mode='markers+lines',
                                name = c,
                                marker=dict(size=8,
                                            symbol=ms[iii],
                                            color=scs[iii],
                                            line=dict(width=2,
                                            color='DarkSlateGrey'),
                                            ),
                                line = dict(color='black',
                                            width=1,
                                            dash='dot'),
                                showlegend=True if i==0 and ii==0 else False
                    ),
                        row=ii+1,col=i+1
                        )
            
    fig.update_yaxes(range=[0,1])        
    fig.update_layout(
        #boxmode="group",
        #xaxis=dict(
        #    title="Year",
        #),
        #title=scenario if geography is None else scenario+": "+geography,      
        #legend=dict(x=0.029, y=1.5, font_size=10,orientation="h",
        #            yref="paper",
        #            traceorder="reversed"),
        margin=dict(l=0, r=25, t=25, b=0),
        width=800,
    )
    #fig.write_image("figures/counties.png", engine="kaleido")
    fig.show()  
    
def plot_county_impacts(results,
                  parameter,
                 scenarios,
                 counties,
                 list_counties,
                 dcfg,
                 naming = None,
                 col = None,
                 agg_years=None,
                 xscale=None
                ):

    
    ### get data
    data = pd.concat([results[s][parameter]
                      for s in scenarios],keys=scenarios, names=["SCENARIO"])
    # ind = data.index.names
    # data = data.reset_index().fillna("NA")
    # data = data.set_index(ind)
    # convert to TWh 
    data = data/3.6
    
    ### filter years
    years = [2020,2025,2030,2035,2040,2045,2050]
    data = data.loc[data.index.get_level_values("YEAR").isin(years)]
    
    ### filter for total, rural, and urban
    data_al = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK")]
    data_ru = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK2")]
    data_ur = data.loc[data.index.get_level_values("TECHNOLOGY").str.startswith("RK1")]
    
    data = pd.concat([data_al,
                      data_ru,
                      data_ur],keys=["Total","Rural","Urban"],
                     names=["AREA"])
    
    ### filter for electricity
    data = data.loc[data.index.get_level_values("FUEL").str.startswith("RE19ELC003")]
    
    ### groupby regions
    data = data.groupby([i for i in data.index.names if (i != "TECHNOLOGY"
                                                         and i != "FUEL")]).sum()
    
    ### rename
    # load county list
    list_counties = pd.read_csv(list_counties,
                           keep_default_na=False)
    counties_ren = list_counties.loc[:,("ID","NAME")].set_index("ID")["NAME"]
    
    if naming is not None:
        data = data.rename(index=naming)
        data = data.rename(index=counties_ren)
    
    ### calculate relative
    # relative = "TECHNOLOGY"
    # data = data/data.groupby([i for i in data.index.names
    #                           if i not in relative]).sum()
    

    xscale=True
    if xscale is not None:
        ysa = pd.read_csv(agg_years,
                          index_col="VALUE")["AGG"]
        xscale=1/ysa.value_counts()
        xscale.index.name="YEAR"
        xscale.name="VALUE"
        data.loc[:,"VALUE"] = data["VALUE"].multiply(xscale,axis=0)
        
    
    ### arrange demand peak data 
    # get data
    ys = pd.concat([results[s]["YearSplit"]
                     for s in scenarios],keys=scenarios, names=["SCENARIO"])
    ys.loc[:,"VALUE"] = ys["VALUE"].multiply(xscale,axis=0)
    
    roa = pd.concat([results[s]["RateOfActivity"]
                     for s in scenarios],keys=scenarios, names=["SCENARIO"])
    iar = pd.concat([results[s]["InputActivityRatio"]
                     for s in scenarios],keys=scenarios, names=["SCENARIO"])
    pdata = roa*iar
    
    ### filter years
    years = [2020,2025,2030,2035,2040,2045,2050]
    pdata = pdata.loc[pdata.index.get_level_values("YEAR").isin(years)]
    ys = ys.loc[ys.index.get_level_values("YEAR").isin(years)]
    ### filter for total, rural, and urban
    pdata_al = pdata.loc[pdata.index.get_level_values("TECHNOLOGY").str.startswith("RK")]
    pdata_ru = pdata.loc[pdata.index.get_level_values("TECHNOLOGY").str.startswith("RK2")]
    pdata_ur = pdata.loc[pdata.index.get_level_values("TECHNOLOGY").str.startswith("RK1")]
    
    pdata = pd.concat([pdata_al,
                      pdata_ru,
                      pdata_ur],keys=["Total","Rural","Urban"],
                     names=["AREA"])
    
    ### filter for electricity
    pdata = pdata.loc[pdata.index.get_level_values("FUEL").str.startswith("RE19ELC003")]
    
    ### groupby regions
    pdata = pdata.groupby([i for i in pdata.index.names if (i not in ["TECHNOLOGY",
                                                                   "FUEL",
                                                                   "MODE_OF_OPERATION"])]).sum()
    # calculate as GW of demand
    pdata = pdata/3.6*1000
    pdata = pdata/(ys*8760)
    pdata = pdata.groupby([i for i in pdata.index.names if i != "TIMESLICE"]).max()
    
    if naming is not None:
        pdata = pdata.rename(index=naming)
        pdata = pdata.rename(index=counties_ren)
    
    ### define techs to plot

    imp = ["el-an","el-pk"]
    fig = make_subplots(rows=len(imp), cols=len(scenarios),
                        shared_yaxes="all",
                        shared_xaxes="columns",
                        row_titles=imp,
                        column_titles=scenarios,
                        horizontal_spacing=0.05,
                        vertical_spacing=0.05)
    
    scs = ['#AAAA00','#EE8866','#77AADD','#99DDFF',
           '#EEDD88','#44BB99','#FFAABB']
    ms = ["square","triangle-up","pentagon","diamond"]
    
    # filter for existing counties and rename if required, cap at 4
    counties = ([c for c in counties if c in counties_ren.values]
                +[counties_ren.loc[c] for c in counties if c in counties_ren.index])
    counties = counties[:4]
    
    for i,s in enumerate(scenarios):
        for ii,p in enumerate(imp):
            if p == "el-an":
                df = data
            else:
                df = pdata
            fig.add_trace(go.Box(
                y=df.loc[("Total",s,slice(None)),"VALUE"],
                x=df.loc[("Total",s,slice(None))].index.get_level_values("YEAR").astype(str),
                #name=t,
                #boxpoints="all",
                #boxpoints=False,
                #fillcolor=col[t],
                line=dict(color="DarkSlateGrey"),
                #offsetgroup=t,
                # legendgroup="scenarios",
                # legendgrouptitle=dict(text="Scenario",
                #                       font=dict(size=12)),
                #marker_color='#3D9970'
                showlegend=False if i==0 else False
            ),
                row=ii+1,col=i+1)
            
            for iii,c in enumerate(counties):
                    fig.add_trace(go.Scatter(
                                y=df.loc[("Total",s,c),"VALUE"],
                                x=df.loc[("Total",s,c)].index.get_level_values("YEAR").astype(str),
                                mode='markers+lines',
                                name = c,
                                marker=dict(size=8,
                                            symbol=ms[iii],
                                            color=scs[iii],
                                            line=dict(width=2,
                                            color='DarkSlateGrey'),
                                            ),
                                line = dict(color='black',
                                            width=1,
                                            dash='dot'),
                                showlegend=True if i==0 and ii==0 else False
                    ),
                        row=ii+1,col=i+1
                        )
            
    fig.update_yaxes(title="Electricity consumption for cooking (TWh)",
                     row=1,
                     col=1)
    fig.update_yaxes(title="Electricity peak for cooking (GW)",
                     row=2,
                     col=1)  
    #fig.update_yaxes(range=[0,1])        
    fig.update_layout(
        #boxmode="group",
        #xaxis=dict(
        #    title="Year",
        #),
        #title=scenario if geography is None else scenario+": "+geography,      
        #legend=dict(x=0.029, y=1.5, font_size=10,orientation="h",
        #            yref="paper",
        #            traceorder="reversed"),
        margin=dict(l=0, r=25, t=25, b=0),
        width=800,
    )
    fig.show()     
    # old version
    # for i,s in enumerate(scenarios):
    #     for ii,t in enumerate(techs):
    #         fig.add_trace(go.Box(
    #             y=data.loc[("Total",s,slice(None),t)]["VALUE"],
    #             x=data.loc[("Total",s,slice(None),t)].index.get_level_values("YEAR").astype(str),
    #             name=t,
    #             #boxpoints="all",
    #             #boxpoints=False,
    #             fillcolor=col[t],
    #             line=dict(color="DarkSlateGrey"),
    #             offsetgroup=t,
    #             # legendgroup="scenarios",
    #             # legendgrouptitle=dict(text="Scenario",
    #             #                       font=dict(size=12)),
    #             #marker_color='#3D9970'
    #             showlegend=True if i==0 else False
    #         ),
    #             row=1,col=i+1)
            
    # fig.update_layout(
    #     boxmode="group",
    #     #xaxis=dict(
    #     #    title="Year",
    #     #),
    #     #title=scenario if geography is None else scenario+": "+geography,      
    #     #legend=dict(x=0.029, y=1.5, font_size=10,orientation="h",
    #     #            yref="paper",
    #     #            traceorder="reversed"),
    #     margin=dict(l=0, r=0, t=25, b=0),
    #     #width=800,
    # )
    # fig.show()      


    #
