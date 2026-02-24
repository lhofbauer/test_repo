#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Introduces the main fratoo model class.

Copyright (C) 2025 Leonhard Hofbauer, licensed under a MIT license
"""

import logging
import sys
import importlib.util
import os
import multiprocessing as mp
import shutil
import gc
import math
import zipfile
import re
import datetime
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


import json

import itertools




try:
    import geopandas as gpd
except ImportError:
    gpd = None 
try:
    from shapely.geometry import Polygon
except ImportError:
    Polygon = None
try:
    from pyomo.opt import SolverFactory
    from pyomo.contrib import appsi
    import pyomo.environ as pyo
    from pyomo.common.tempfiles import TempfileManager
except ImportError:
    pyo = None
try:
    import pandas_datapackage_reader as pdr
except ImportError:
    pdr = None     
try:
    import frictionless as fl
except ImportError:
    fl = None
    
logger = logging.getLogger(__name__)

# use plotly as backend for pandas-based plotting
pd.options.plotting.backend = "plotly"




def set_verbosity(level="INFO"):
    """Set log verbosity for the package.


    Parameters
    ----------
    level : str, optional
        Level of logging for the entire package. One of Python's standard
        levels, i.e., 'DEBUG','INFO','WARNING', 'ERROR', and 'CRITICAL'.
        The default is 'INFO'.

    Returns
    -------
    None.

    """
    # FIXME: include possibility to write log in file (?)
    
    root_logger = logging.getLogger()
    
    # remove existing handlers
    if root_logger.hasHandlers():
        for h in root_logger.handlers:
            root_logger.removeHandler(h)
    
    
    console = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
    )
    console.setFormatter(formatter)
    root_logger.addHandler(console)
    root_logger.setLevel(level)


class Model(object):
    """ The main fratoo model class.
    
        
    """
    
    # Class attributes:
    # input_data  
    # ms_struct

    # FIXME: Format/blacken again at some point
    # FIXME: Add function to save (processed) input data set, or run data set
    # FIXME: consider handling of dtypes, make sure dtypes are processed properly
    # FIXME: think abo checks/exceptions in general and review code 
    # accordingly, potentially including a number of of checks/exceptions/errors
    # FIXME: add docstring(s) where missing
    # FIXME: add check/consider if regions given to run are part of model
    # (at the moment they are deleted from params but included in sets)
    # FIXME: reconsider use of pandas multiindex and check if all use of it
    # (especially indexing) makes sense and is not dependant on region being
    # first column of index etc.
    # FIXME: rename region(s)/etc. to entities
    # FIXME: consider where does all of this assume a specific structure of the
    # Pyomo/OSeMOSYS model (e.g., plotting capacity assumes specific variable
    # is existing, etc.) and should it be like this, should it be articulated
    # somewhere
    def __init__(self, data=None, model=None, process=False, tempdir=None,
                 *args, **kwargs):
        """Return an empty or initialised model.
        
        
        Parameters
        ----------
        data : str, optional
            Path to datapackage file. The default is None.
        model : str, optional
            Path to OSeMOSYS file. The default is None.
        process : bool, optional
            Specifies whether to process input data (e.g., abbreviations) or
            not. The default is False.
        tempdir : str, optional
            Specifies temporary directory for pyomo. If None is given, no
            directory will explicitly be provided to pyomo.
            The default is None.
        *args : TYPE
            DESCRIPTION.
        **kwargs : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        # FIXME: *args/**kwargs arguments necessary? Remove ?
        
        logger.info("Initializing model")

        if model is not None:
            self.init_osemosys(model)

        if data is not None:
            self.init_from_datapackage(data, process=process)
        
        if tempdir is not None:
            TempfileManager.tempdir = tempdir
            
        logger.info("Initialized model")
        
    def init_from_datapackage(self, data, process=False):
        """ Load model input data from data package.
        

        Parameters
        ----------
        data : str
            Path to datapackage file.
        process : bool, optional
            Specifies whether to process framework specific syntax
            (e.g., abbreviations) in the input data  or not.
            The default is False.

        Returns
        -------
        None.

        """
        # FIXME: call (yet to be developed) input data consistency check function
        
        # FIXME: consider what format/etc. is expected from data package (e.g.
        # specified types, columns -> otherwise a column not index (?), names,
        # etc.) and how this relates to code and what code should do/check/etc.
        
        # FIXME: implement using frictionless package (see usage for results
        # below) (?)
        
        if pdr is None:
            logger.warning("Loading model data from data package is not"
                           " possible as the required"
                           " dependency(pandas_datapackage_reader)"
                           " is not available.")
            return
        
        logger.info("Loading model data package")

        dat = pdr.read_datapackage(data)

        self.input_data = {k: v for k, v in dat.items()
                           if not k.startswith("ft_")}
        
        if ("ft_affiliation" in dat.keys() and
                "ft_param_agg" in dat.keys() and
                "ft_param_disagg" in dat.keys() and
                "ft_scale" in dat.keys()):
            
            self.ms_struct = {k: v for k, v in dat.items()
                          if k.startswith("ft_")}
        else:
            logger.warning("No or incomplete data on multi-scale structure \
                           found, loading as normal OSeMOSYS model")
            self.ms_struct = None
            
        logger.info("Loaded model data package")
        
        if process == True:
            self.process_input_data()
            
    def init_from_dictionary(self, data, config=None, process=False):
        """ Load model input data from dictionary.
        
    
        Parameters
        ----------
        data : dictionary
            Dictionary with all parameters, including multi-scale structure, (keys)
            and the respective data in DataFrames (values).
        config : dictionary
            otoole compatible config dictionary.
        process : bool, optional
            Specifies whether to process framework specific syntax
            (e.g., abbreviations) in the input data  or not.
            The default is False.
    
        Returns
        -------
        None.
    
        """
    
        logger.info("Loading model data from dictionary")
    
        self.input_data = {k: v for k, v in data.items()
                           if not k.startswith("ft_")}
        
        if ("ft_affiliation" in data.keys() and
                "ft_param_agg" in data.keys() and
                "ft_param_disagg" in data.keys() and
                "ft_scale" in data.keys()):
            
            self.ms_struct = {k: v for k, v in data.items()
                          if k.startswith("ft_")}
        else:
            logger.warning("No or incomplete data on multi-scale structure \
                           found, loading as normal OSeMOSYS model")
            self.ms_struct = None
        
        if config is not None:
            self.data_config = config
        logger.info("Loaded model data")
        
        if process == True:
            self.process_input_data()

    def init_osemosys(self, model):
        """ Load OSeMOSYS model file.
        

        Parameters
        ----------
        model : str
            Path to OSeMOSYS python file.

        Returns
        -------
        None.

        """
        
        # make variable global, necessary due to problems with multi-processing
        global OSeMOSYS
        logger.info("Loading OSeMOSYS framework")
        
        name = os.path.splitext(os.path.basename(model))[0]
        spec = importlib.util.spec_from_file_location(name, model)
        OSeMOSYS = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(OSeMOSYS)
        
        logger.info("Loaded OSeMOSYS framework")


    def process_input_data(self, sep=":"):
        """ Process potential fratoo abbreviation syntax in model input data.

        Returns
        -------
        None.

        """
        
        # FIXME: because of fratoo syntax some columns can't be integer
        # formatted any more automatically in dataframes, might need to be
        # manually arranged during processing
        # FIXME: potentially check format of syntax, see here
        # https://stackoverflow.com/questions/14966647/check-python-string-format
        
        # check if input data are loaded
        if not hasattr(self,"input_data"):
            raise AttributeError("Input data can not be processed as they are"+
                                 " not existing.")
        
        
        logger.info("Processing input data")
        
        for param, df_values in self.input_data.items():
            
            # if empty dataframe or a set, do nothing
            if df_values.empty or df_values.index.names[0] is None:
                continue
            
            # drop all columns with default value for parameter (to avoid
            # it being aggregate to another value in case entities are
            # aggregated for model runs) if default value available
            if hasattr(self,"data_config"):
                df_values = df_values.loc[df_values["VALUE"]
                                          !=self.data_config[param]["default"]]
            elif "OSeMOSYS" in globals():
                df_values = df_values.loc[df_values["VALUE"]
                                          !=getattr(OSeMOSYS.model,param)._default_val]
            

            # get dict with dataframe column headers as keys and dataframe
            # columns values (as list) as values
            values = df_values.reset_index().to_dict(orient="list")

            # update values as list of lists with abbreviations replaced
            for s, v in values.items():
                
                # FIXME: also test if int(v[2:]) is valid scale?
                # FIXME: potentially check if multi-scale model and raise
                # proper error if multi-scale abbreviation syntax is used 
                # (currently non ms syntax works in non ms models but if 
                # ms syntax used error is not very clear)
                
                
                values[s] = [
                    [r]
                    if not isinstance(r, str) else 
                    self.input_data[s]["VALUE"].tolist()
                    if r == ":*" else
                    self.ms_struct["ft_scale"][
                        self.ms_struct["ft_scale"]["VALUE"] == int(r[2:])
                    ].index.tolist()
                    if (r.startswith(":*") and r[2:].isdigit()) else
                    [r]
                    for r in v
                ]
                
            # update values by calculating all combinations for each row and
            # appending it to new list
            r = list()
            for i in range(len(df_values.index)):

                r.extend(itertools.product(*[values[s][i]
                                             for s in values.keys()]))

            for i, s in enumerate(values.keys()):

                values[s] = [item[i] for item in r]


         
            # process fratoo syntax for parent region's fuel, e.g., ":2:EL"
            if "FUEL" in values.keys() and "REGION" in values.keys():
                
                for i, (f, r) in enumerate(zip(values["FUEL"],
                                               values["REGION"])):
                    if f.count(":") == 2 and f.split(":")[1].isdigit():
                        parent = r
                        
                        # FIXME: delete this, or is the while loop worse 
                        # (e.g., as never ending loop)(?)
                        # for _ in range(
                        #     -int(f.split(":")[1])
                        #     + self.ms_struct["ft_scale"].loc[r, "VALUE"]
                        # ):
                        
                        # go recursively through parents until correct scale
                        # reached
                        while (self.ms_struct["ft_scale"].loc[parent,
                                                              "VALUE"] !=
                               int(f.split(":")[1])):

                            
                            parent = self.ms_struct["ft_affiliation"].loc[
                                parent, "VALUE"
                            ]
                            if (self.ms_struct["ft_scale"].loc[parent,"VALUE"]<
                               int(f.split(":")[1])):
                                raise ValueError("No parent region on \
                                                 specified scale found")
                                
                        values["FUEL"][i] = parent + sep + f.split(":")[2]
                    elif f.count(":") == 2:
                        values["FUEL"][i] = f.split(":")[1] + sep + f.split(":")[2]
                        
                        
            # process fratoo syntax for parent region's emissions, e.g., ":2:CD"
            if "EMISSION" in values.keys() and "REGION" in values.keys():
                
                for i, (f, r) in enumerate(zip(values["EMISSION"],
                                               values["REGION"])):
                    if f.count(":") == 2 and f.split(":")[1].isdigit():
                        parent = r
                        
                        # FIXME: delete this, or is the while loop worse 
                        # (e.g., as never ending loop)(?)
                        # for _ in range(
                        #     -int(f.split(":")[1])
                        #     + self.ms_struct["ft_scale"].loc[r, "VALUE"]
                        # ):
                        
                        # go recursively through parents until correct scale
                        # reached
                        while (self.ms_struct["ft_scale"].loc[parent,
                                                              "VALUE"] !=
                               int(f.split(":")[1])):

                            
                            parent = self.ms_struct["ft_affiliation"].loc[
                                parent, "VALUE"
                            ]
                            if (self.ms_struct["ft_scale"].loc[parent,"VALUE"]<
                               int(f.split(":")[1])):
                                raise ValueError("No parent region on \
                                                 specified scale found")
                                
                        values["EMISSION"][i] = parent + sep + f.split(":")[2]
                    elif f.count(":") == 2:
                        values["EMISSION"][i] = f.split(":")[1] + sep + f.split(":")[2]
                        
            
            # feed processed dict back into DataFrame and check dtypes
            df_values = pd.DataFrame.from_dict(values)
            
            # FIXME: maybe go through all levels and set dtypes  
            if "MODE_OF_OPERATION" in df_values.columns:
                df_values["MODE_OF_OPERATION"] = df_values[
                    "MODE_OF_OPERATION"].astype(int)
            
            if "YEAR" in df_values.columns:
                df_values["YEAR"] = df_values["YEAR"].astype(int)
                
            df_values = df_values.set_index(list(df_values.columns[0:-1]))
            
            
            # remove duplicates, keep last
            df_values = df_values[~df_values.index.duplicated(keep="last")]

            self.input_data[param] = df_values

        logger.info("Processed input data")


            
    
    def _create_regions_for_run(self, regions, autoinclude, weights, syn =["_","+"]):
        """ Create DataFrame of spatial entities for a model run.
        
        It requires the entities to be included explicitely in the model run
        and (optionally) adds necessary child and parent regions along with
        type, scale, fraction (for parents), and respective explicit parent 
        region for each region.
        
        Parameters
        ----------
        regions : list
            List of spatial entities (strings) to be explicitely included.
            Sublists can be used to indicate entities to be aggregated.
        autoinclude : bool
            If the entities for the run are to automatically also include
            (recursively) parent and child entities of explicitely listed
            spatial entities.
        weights : str
            Name of the parameter that is used to as weight when allocating
            calculating the fraction of parent regions. The parameter needs
            to be defined over the REGION set. If None is given equal weights
            are assumed.

        Returns
        -------
        df_entities : DataFrame
            DataFrame with all entities for the optimization run
            and relevant information if applicable, i.e., multi-scale.
        """
        # FIXME: potentially use better structure , i.e., not through a dict
        # format of r:[type,eparent] (?)    
        # FIXME: make possibility to aggregate nicer
        # 1) not under eparent - change this whole approach
        # 2) proper name for aggregated areas (child and others)
        # FIXME: if not multi-scale, it will expect no aggregation, i.e. flat
        # list
        
        # create dataframe of regions to be included, i.e., explicitely
        # mentioned as well as implicitely included ones, i.e., parent
        # or child regions (IDs are 0: explicitely included, 1: child/
        # aggregated regions, 2: parent)
        
        # if not multi-scale, just return excplicit regions in DataFrame index
        if self.ms_struct == None:
            # if this is to be done differently, i.e., without setting empty
            # columns for type, etc., one needs to change _create_pyomo_data()
            df_regions = pd.DataFrame(index=regions, columns=["type",
                                                              "scale",
                                                              "eparent"])
            df_regions.index.name = "REGION"
            return df_regions
        

        
        dregions = dict()
        df_aff = self.ms_struct["ft_affiliation"]
        
        
        # add explicitely included regions
        for r in regions:
            if isinstance(r,list):
                for rr in r:
                    dregions[rr] = [1,r[0]+syn[1]]              
            else:
                dregions[r] = [0,np.nan]
        
        # use flattened list
        oregions = regions
        regions = list(dregions.keys())
        
        # if autoinclude, add child and parent entities recursively
        if autoinclude:
            
            for r in regions:
    
                # add child regions recursively
                children = [r]
    
                while children:
                    new_children = list()
                    for c in children:
                        gchildren = [
                            r
                            for r in df_aff[df_aff["VALUE"] == c].index.tolist()
                            if r not in regions
                        ]
                        
                        new_children.extend(gchildren)
                        
                    dregions.update(
                        [
                            (a, [1, r])
                            for a in new_children
                            if not (a in dregions.keys()
                                    and dregions[a][0] == 0)
                        ]
                    )
    
                    children = new_children
    
                # add parent regions recursively    
                
                if any(r in sl for sl in oregions if isinstance(sl,list)):          
                    # add aggregated parent regions if aggregation of explicit
                    # regions happens across different parent regions
                    plist = [[a for a in i] for i in oregions 
                             if (isinstance(i,list) 
                                 and (r in i))][0]
                    while not pd.isna(df_aff.loc[plist, "VALUE"]).all():
                        nplist = list()
                        if len(df_aff.loc[plist, "VALUE"].unique())==1:
                            dregions[df_aff.loc[plist,
                                                "VALUE"].unique()[0]] = [2, np.nan]
                            plist = [df_aff.loc[plist, "VALUE"].unique()[0]]
                            continue
                        parents = df_aff.loc[plist, "VALUE"]
                        nplist.extend([p for p in parents if p not in nplist])
                        dregions.update({p:[2,nplist[0]+syn[1]] for p in parents
                                         if ((p not in regions)
                                             and p not in dregions.keys())})
                        # for rr in plist:
                        #     parent = df_aff.loc[rr, "VALUE"]
                        #     if parent not in nplist:
                        #         nplist.append(parent)
                        #     if ((parent not in regions) 
                        #         and (parent not in dregions.keys())): 
                        #         dregions[parent] = [2,nplist[1]+"+"]
                                
                        plist = nplist
                else:
                    parent = r
                    while not pd.isna(df_aff.loc[parent, "VALUE"]):
                        parent = df_aff.loc[parent, "VALUE"]
        
                        if not (
                            parent in dregions.keys()
                            and (dregions[parent][0] == 0
                                 or dregions[parent][0] == 1)
                        ):
                            dregions[parent] = [2, np.nan]

        # Create dataframe and add fraction for parent regions

        # FIXME: add dtypes in dataframe creation?
        # FIXME: Implement a better approach for frac calculation, considering:
        #   1. children can be on different scales
        #   2. children can be of different size, e.g., include different number
        #   of own children (-> use lowest scale? -> potentially creates problems
        #   if different scale for lowest children)
        #   3. .. 
        # FIXME: rename column names (e.g., eparent), also capitalize (?)
        
        df_regions = pd.DataFrame(
            data={
                "type": {k: v[0] for k, v in dregions.items()},
                "eparent": {k: v[1] for k, v in dregions.items()},
            }
        )
        df_regions.index.name = "REGION"

        df_regions["scale"] = self.ms_struct["ft_scale"]["VALUE"]
        
        if weights is not None:
           df_aff["WEIGHT"] = self.input_data[weights].groupby(["REGION"]).sum()["VALUE"]
           for s in sorted(self.ms_struct["ft_scale"]
                           ["VALUE"].unique(),reverse=True)[1:]:
               d = df_aff.groupby(["VALUE"]).sum()
               d.index.name="REGION"
               df_aff.loc[df_aff["WEIGHT"].isna()|
                          (df_aff["WEIGHT"]==0),"WEIGHT"] = d
           df_aff["WEIGHT"] =  df_aff["WEIGHT"].fillna(1)
        else:
           df_aff["WEIGHT"] = 1 
           
        df_regions["frac"] = 1
        # sorting based on scale important for calculation below
        df_regions.sort_values("scale", ascending=False, inplace=True)
        
        # FIXME: alternative in the comment should not work because one
        # actually relies on fracs calculated earlier in the loop but they are
        # not saved back in df_regions here, yet a list comprehension would be
        # much better than iterating through a DataFrame
  
        # df_regions["frac"] = [df_regions.loc[
        #         df_regions.index.intersection(df_aff[df_aff["VALUE"] == r].index),
        #         "frac",
        #     ].sum() / df_aff.loc[df_aff["VALUE"] == r,"WEIGHT"].sum() if t==2 else 1 
        #     for r, t in zip(df_regions.index,df_regions["type"])]
        

        for r in df_regions[(df_regions["type"] == 2)].index:
            df_regions.loc[r, "frac"] = df_regions.loc[
                df_regions.index.intersection(df_aff[df_aff["VALUE"] == r].index),
                "frac",
            ].multiply(df_aff["WEIGHT"]).dropna().sum() / df_aff.loc[df_aff["VALUE"] == r,"WEIGHT"].sum()
            

        return df_regions

    def _create_run_data(self, df_regions, func=None,sep = ":",syn =["_","+"],
                         redset=True, pyomo=True):
        """ Create data dict for a run to fill, e.g., a pyomo model.
        

        Parameters
        ----------
        df_regions : DataFrame
            DataFrame containing regions and relevant infos. This is usually
            derived by _create_regions_for_run() from a list of regions.
        func : function, optional
            Function to be applied to input data before creating pyomo data.
            The default is None.
        sep: str, optional
            Separator used when merging region in technology and other
            sets.
        syn: list of str, optional
            Syntax used if aggregated regions are represented. syn[0] is the
            separator between scale and region, syn[1] a letter symbolize
            an aggregate region.
        redset : boolean, optional
            If to create reduced sets.
        pyomo : boolean, optional
            If pyomo model or basic run data.
            
        Returns
        -------
        run_data : dict
            Dictionary containing the data for the model run.

        """
        # FIXME: optimize pyomo model/data, e.g., delete technologies with
        # Input- and OutputActivityRatio = 0, e.g. also transmission between
        # regions when one is not present in run (currently only IAR/OAR
        # would be deleted, not CapitalCost etc.)
        # FIXME: should be able to run as non multi-scale
        # FIXME: split steps in separate functions (?)
        # FIXME: aggregation of regions might lead to unexpected results if,
        # e.g. two transmission techs with same name have to be aggregated 
        # (should result in two different data rows with same tech but
        # different fuel, so probably just one considered)
        # FIXME: check if this function really also works if autoinclude
        # is false but regions are aggregated, i.e., mock child entities exist
        
        logger.info("Creating pyomo data dictionary")
        
        run_data = {
            param: df.copy(deep=True) for param, df in self.input_data.items()
        }
        
        # apply function, e.g., for creating different scenarios
        if func is not None:
            run_data = func(run_data)
        
        # process parameter dataframes based on the regions dataframe, i.e.,
        # if explicitely included -> take over
        # if parent -> take over and potentially apply fraction
        # if child -> aggregate with all other ones in the list and
        # on the same scale and with same explicit parent
        for param, df_values in run_data.items():
            
            # if not indexed over regions or if empty, do nothing
            if (
                not (
                    "REGION" in df_values.index.names
                    or "REGION" == df_values.index.name
                )
            ) or df_values.empty:
                continue
            
            # discard if region not included in run, i.e., not listed in
            # df_regions
            df_values = df_values.loc[
                df_values.index.get_level_values("REGION").intersection(
                    df_regions.index
                ),
                :,
            ]
            
            # FIXME: maybe this can be done nicer without adding type, etc. to
            # data, not having to concat everything in the end, etc. (-> maybe
            # using apply function as below in the end of the function)
            # FIXME: might fail if fratoo syntax  but model not multi-scale,
            # i.e., df_regions doesn't have type, etc. columns
            
            # process data for fuels set with explicit region in the input
            # data, i.e., delete if region not included, rename if aggregated
            # child region
            if ("FUEL" == df_values.index.name
                or "FUEL" in df_values.index.names):
                df_v_f = df_values.loc[
                    df_values.index.get_level_values("FUEL").str.contains(sep),
                    :].reset_index()
                if not df_v_f.empty:
                    df_v_f[["fuel_region",
                            "fuel"]] = df_v_f["FUEL"].str.split(sep,
                                                                expand=True)
                    # add type, scale, eparent to dataframe, also deletes rows
                    # if region not included ('inner' merge)
                    df_v_f = pd.merge(
                        df_v_f,
                        df_regions[["type", "scale", "eparent"]],
                        how="inner",
                        right_on="REGION",
                        left_on="fuel_region",
                    )
                    # if child or aggregated parent region rename fuel to 
                    # aggregated name
                    df_v_f_c = df_v_f[df_v_f["eparent"].notna()]
                    
                    # FIXME: change this to get rid of the pandas warning
                    df_v_f_c.loc[:,"FUEL"] = (
                        df_v_f_c.loc[:,"scale"].astype(str)
                        + syn[0]
                        + df_v_f_c.loc[:,"eparent"].astype(str)
                        + sep
                        + df_v_f_c.loc[:,"fuel"].astype(str)
                    )
                    
                    df_v_f.set_index(list(df_v_f.columns), inplace=True)
                    df_v_f.reset_index("VALUE", inplace=True)
                    df_v_f_c.set_index(list(df_v_f_c.columns), inplace=True)
                    df_v_f_c.reset_index("VALUE", inplace=True)
                    
                    # Concat (processed) DataFrames
                    df_values = pd.concat(
                        [
                            df_values.loc[
                                ~df_values.index.get_level_values("FUEL").str.contains(
                                    sep
                                ),
                                :,
                            ],
                            df_v_f[df_v_f.index.get_level_values("eparent").isna()],
                            df_v_f_c,
                        ],
                        axis=0,
                        join="inner",
                    )
                    
            # process data for emissions set with explicit region in the input
            # data, i.e., delete if region not included, rename if aggregated
            # child region
            if ("EMISSION" == df_values.index.name
                or "EMISSION" in df_values.index.names):
                df_v_f = df_values.loc[
                    df_values.index.get_level_values("EMISSION").str.contains(sep),
                    :].reset_index()
                if not df_v_f.empty:
                    df_v_f[["emission_region",
                            "emission"]] = df_v_f["EMISSION"].str.split(sep,
                                                                expand=True)
                    # add type, scale, eparent to dataframe, also deletes rows
                    # if region not included ('inner' merge)
                    df_v_f = pd.merge(
                        df_v_f,
                        df_regions[["type", "scale", "eparent"]],
                        how="inner",
                        right_on="REGION",
                        left_on="emission_region",
                    )
                    # if child region rename fuel to aggregated name
                    df_v_f_c = df_v_f[df_v_f["eparent"].notna()]
                    
                    # FIXME: change this to get rid of the pandas warning
                    df_v_f_c.loc[:,"EMISSION"] = (
                        df_v_f_c.loc[:,"scale"].astype(str)
                        + syn[0]
                        + df_v_f_c.loc[:,"eparent"].astype(str)
                        + sep
                        + df_v_f_c.loc[:,"emission"].astype(str)
                    )
                    
                    df_v_f.set_index(list(df_v_f.columns), inplace=True)
                    df_v_f.reset_index("VALUE", inplace=True)
                    df_v_f_c.set_index(list(df_v_f_c.columns), inplace=True)
                    df_v_f_c.reset_index("VALUE", inplace=True)
                    
                    # Concat (processed) DataFrames
                    df_values = pd.concat(
                        [
                            df_values.loc[
                                ~df_values.index.get_level_values("EMISSION").str.contains(
                                    sep
                                ),
                                :,
                            ],
                            df_v_f[df_v_f.index.get_level_values("eparent").isna()],
                            df_v_f_c,
                        ],
                        axis=0,
                        join="inner",
                    )
                    
            # if parameter is to be split for parents, apply the fraction
            # for parents
            
            if not df_values.loc[
                df_values.index.get_level_values("REGION").intersection(
                    df_regions[df_regions["type"] == 2].index
                )].empty:
                
                if (self.ms_struct["ft_param_disagg"].loc[param, "VALUE"] == "frac"):
                   
                    df_values.loc[
                        df_values.index.get_level_values("REGION").intersection(
                            df_regions[df_regions["type"] == 2].index
                        ).unique(),
                        "VALUE"
                    ] = (
                        df_values.loc[
                            df_values.index.get_level_values("REGION").intersection(
                                df_regions[df_regions["type"] == 2].index
                            ),
                            "VALUE"
                        ]
                        * df_regions[df_regions["type"] == 2]["frac"]
                    )
                      
                        
            # FIXME: could be done nicer (?), e.g., no concat, use of idx, ...
            
            # process for child regions, i.e., aggregate for all child regions
            # on same scale and with same explicit parent (same for aggregated
            # parents)
            if not df_values.loc[
                df_values.index.get_level_values("REGION").intersection(
                    df_regions[df_regions["eparent"].notna()].index
                )
            ].empty:
                # get child regions and add respective scale and eparent
                df_children = df_values.loc[
                    df_values.index.get_level_values("REGION").intersection(
                        df_regions[df_regions["eparent"].notna()].index
                    ),
                    :,
                ].join(df_regions[["scale", "eparent"]])
                
                # aggregate depending on ft_param_agg
                if self.ms_struct["ft_param_agg"].loc[param, "VALUE"] == "sum":
                    df_children = df_children.groupby(
                        ["scale", "eparent"]
                        + [i for i in df_children.index.names if i != "REGION"]
                    ).sum()
                elif self.ms_struct["ft_param_agg"].loc[param, "VALUE"] == "eq":
                    df_children = df_children.groupby(
                        ["scale", "eparent"]
                        + [i for i in df_children.index.names if i != "REGION"]
                    ).mean()
                else:
                    raise ValueError("The aggregation method for parameter"+
                                         " values specified in ft_param_agg"+
                                         " is not implemented in fratoo.")
                
                # add region column/set with values named like
                # "[scale]_[eparent]" to index
                idx = df_children.index.to_frame()
                idx.insert(
                    0,
                    "REGION",
                    [
                        str(a) + syn[0] + str(b)
                        for a, b in zip(
                            df_children.index.get_level_values(0),
                            df_children.index.get_level_values(1),
                        )
                    ],
                )

                df_children.index = pd.MultiIndex.from_frame(idx)
                # FIXME: check if one can use better concat/merge/join, now
                # with different multi-index that doesn't seem to be very well
                # defined here (and for the other concats) and might break
                # with other changes introduced.
                
                df_children.reset_index(
                    level=["eparent", "scale"], drop=True, inplace=True
                )
                
                # Concat DataFrames
                df_values = pd.concat(
                    [
                        df_values.loc[
                            df_values.index.get_level_values("REGION").intersection(
                                df_regions[df_regions["eparent"].isna()].index
                            ),
                            :,
                        ],
                        df_children,
                    ],
                    axis=0,
                    join="inner",
                )

            run_data[param] = df_values


        # process sets and parameters to 'one-region' OSeMOSYS model
        
        # FIXME: implement this more generally, also for storage etc.
        # FIXME: this will likely create problems if some technology params
        # are defined, e.g., because abbreviation ' all regions, all techs' is
        # used, and which then will probably throw an pyomo error if this tech
        # if that tech is removed but param is still set
        
        # create list of technologies and fuel for which at least one activity
        # ratio is given (others will later be deleted from the sets)
        
        def_techs = set([r+sep+t for r,t in zip(
            run_data["InputActivityRatio"].index.get_level_values("REGION"),
            run_data["InputActivityRatio"].index.get_level_values("TECHNOLOGY")
            )]+
            [r+sep+t for r,t in zip(
            run_data["OutputActivityRatio"].index.get_level_values("REGION"),
            run_data["OutputActivityRatio"].index.get_level_values("TECHNOLOGY")
            )]
            )
  
        def_fuels = set([r+sep+f if sep not in f else f for r,f in zip(
            run_data["InputActivityRatio"].index.get_level_values("REGION"),
            run_data["InputActivityRatio"].index.get_level_values("FUEL")
            )]+
            [r+sep+f if sep not in f else f for r,f in zip(
            run_data["OutputActivityRatio"].index.get_level_values("REGION"),
            run_data["OutputActivityRatio"].index.get_level_values("FUEL")
            )]
            )
        

        for param, df_values in run_data.items():
            
            
            # create list of actual model regions including aggregation of
            # child regions
            run_regions = list(
                set(
                    [
                        r if pd.isna(e) else str(s) + syn[0] + str(e)
                        for r, t, s, e in zip(
                            df_regions.index,
                            df_regions["type"],
                            df_regions["scale"],
                            df_regions["eparent"],
                        )
                    ]
                )
            )
            
            if param == "REGION":
                df_values = pd.DataFrame(["run_region"], columns=["VALUE"])

            # FIXME: put in one condition (?) -> differentiation maybe later
            # necessary when optimizing sets?
            # FIXME: potential to make all of this nicer?
            # FIXME: optimize sets, e.g., don't add techs actually
            # non-existent in some regions
            
            

            # process sets
            elif param == "FUEL":
                df_values = pd.DataFrame(
                    [r + sep + f for r in run_regions for f in df_values["VALUE"]
                     if r + sep + f in def_fuels],
                    columns=["VALUE"],
                )

            elif param == "EMISSION":
                df_values = pd.DataFrame(
                    [r + sep + e for r in run_regions for e in df_values["VALUE"]],
                    columns=["VALUE"],
                )

            elif param == "TECHNOLOGY":
                df_values = pd.DataFrame(
                    [r + sep + t for r in run_regions for t in df_values["VALUE"]
                     if r + sep + t in def_techs],
                    columns=["VALUE"],
                )

            elif param == "STORAGE":
                df_values = pd.DataFrame(
                    [r + sep + s for r in run_regions for s in df_values["VALUE"]],
                    columns=["VALUE"],
                )
            
            # process parameters if indexed over region set, i.e., push region
            # into other sets (fuel, technology, etc.) if not explicitely given
            # (only in the case of fuel or emission)
            elif (
                "REGION" in df_values.index.names or "REGION" == df_values.index.name
            ) and not df_values.empty:

                idx = df_values.index.to_frame()
                
                # FIXME: could be also done with list comprehension (?) as done
                # above, maybe make all this consistent
                if "EMISSION" in idx.columns:
                    idx["EMISSION"] = idx.apply(
                        lambda r: r["REGION"] + sep + r["EMISSION"]
                        if sep not in r["EMISSION"]
                        else r["EMISSION"],
                        axis=1,
                    )
                if "FUEL" in idx.columns:
                    idx["FUEL"] = idx.apply(
                        lambda r: r["REGION"] + sep + r["FUEL"]
                        if sep not in r["FUEL"]
                        else r["FUEL"],
                        axis=1,
                    )
                if "TECHNOLOGY" in idx.columns:
                    idx["TECHNOLOGY"] = idx["REGION"] + sep + idx["TECHNOLOGY"]
                if "STORAGE" in idx.columns:
                    idx["STORAGE"] = idx["REGION"] + sep + idx["STORAGE"]

                idx["REGION"] = "run_region"
                df_values.index = pd.MultiIndex.from_frame(idx)
            
            # check if defined over technology set and delete rows if set for
            # technologies that are not part of the technology set
            # FIXME: needs to be checked if this makes sense/can be done better
            # and needs to applied consistently across sets
            if (
                "TECHNOLOGY" in df_values.index.names 
                or "TECHNOLOGY" == df_values.index.name
            ) and not df_values.empty:            
                df_values = df_values[df_values.index.get_level_values("TECHNOLOGY").isin(def_techs)]
                
            run_data[param] = df_values
            
        if redset:    
        # create trimmed down sets for more efficient model implementation
            mode_tech_in = set([(f,m,t) for f,t,m,v in zip(
                run_data["InputActivityRatio"].index.get_level_values("FUEL"),
                run_data["InputActivityRatio"].index.get_level_values("TECHNOLOGY"),
                run_data["InputActivityRatio"].index.get_level_values("MODE_OF_OPERATION"),
                run_data["InputActivityRatio"]["VALUE"]
                ) if v != 0 ]
                )
            mode_tech_out = set([(f,m,t) for f,t,m,v in zip(
                run_data["OutputActivityRatio"].index.get_level_values("FUEL"),
                run_data["OutputActivityRatio"].index.get_level_values("TECHNOLOGY"),
                run_data["OutputActivityRatio"].index.get_level_values("MODE_OF_OPERATION"),
                run_data["OutputActivityRatio"]["VALUE"]
                ) if v != 0 ]
                )
            mode_tech_em = set([(e,m,t) for e,t,m,v in zip(
                run_data["EmissionActivityRatio"].index.get_level_values("EMISSION"),
                run_data["EmissionActivityRatio"].index.get_level_values("TECHNOLOGY"),
                run_data["EmissionActivityRatio"].index.get_level_values("MODE_OF_OPERATION"),
                run_data["EmissionActivityRatio"]["VALUE"]
                ) if v != 0 ]
                )
            mode_tech = set([(t,m) for t,m,v in zip(
                run_data["InputActivityRatio"].index.get_level_values("TECHNOLOGY"),
                run_data["InputActivityRatio"].index.get_level_values("MODE_OF_OPERATION"),
                run_data["InputActivityRatio"]["VALUE"]
                ) if v != 0 ]
                +
                [(t,m) for t,m,v in zip(
                    run_data["OutputActivityRatio"].index.get_level_values("TECHNOLOGY"),
                    run_data["OutputActivityRatio"].index.get_level_values("MODE_OF_OPERATION"),
                    run_data["OutputActivityRatio"]["VALUE"]
                    ) if v != 0 ])
    
    
            
            run_data["MODETECHNOLOGYFUELIN"] = pd.DataFrame([],columns=["VALUE"])
            run_data["MODETECHNOLOGYFUELIN"]["VALUE"] = list(mode_tech_in)
            run_data["MODETECHNOLOGYFUELOUT"] = pd.DataFrame([],columns=["VALUE"])
            run_data["MODETECHNOLOGYFUELOUT"]["VALUE"] = list(mode_tech_out)
            run_data["MODETECHNOLOGYEMISSION"] = pd.DataFrame([],columns=["VALUE"])
            run_data["MODETECHNOLOGYEMISSION"]["VALUE"] = list(mode_tech_em)
            run_data["MODETECHNOLOGY"] = pd.DataFrame([],columns=["VALUE"])
            run_data["MODETECHNOLOGY"]["VALUE"] = list(mode_tech)        
        # run_data["MODETECHNOLOGYsparse"] = pd.DataFrame(mode_tech,columns=["INDEX","VALUE"])
        # run_data["MODETECHNOLOGYsparse"] = run_data["MODETECHNOLOGYsparse"].groupby("INDEX").sum()
        # run_data["MODETECHNOLOGYsparse"]["VALUE"] = run_data["MODETECHNOLOGYsparse"]["VALUE"].apply(lambda x: list(set(x))) 
        
        if pyomo:
        # fill pyomo dict for model run with data from DataFrames
            run_data = {
                None: {
                    a: {None: run_data[a]["VALUE"].tolist()}
                    if a.isupper()
                    else run_data[a].to_dict()["VALUE"]
                    for a in run_data.keys()
                }
            }
        
        logger.info("Created run data dictionary")
        
        return run_data
 
    
    def perform_runs(self, names, entities, func=None, autoinclude=True,
                     weights=None,
                     processes=1, join_results=False, overwrite=False,
                     solver="cbc", duals=None, warmstart=False, **kwargs):
        """ Perform runs of the model.
        

        Parameters
        ----------
        names : list
            List of names of the model runs. Needs to be equal length as
            the regions list
        entities : list
            List (nested) of the entities included in each model run. Each
            sublist for a single run includes lists of strings where each
            subsublist contains the names of the entities part of a single
            optimization and (optional) subsubsublist (sorry) contain entities
            that are to be aggregated.
        func : list, optional
            List of functions to be applied to input data set for each of the
            runs. Must be same length as other run lists if given. The default
            is None.
        autoinclude : bool, optional
            If the run is to automatically also include parent and child
            entities of explicitly listed spatial entities. The default is
            True.
        weights : str
            Name of the parameter that is used to as weight when
            calculating the fraction of parent regions. The parameter needs
            to be defined over the REGION set. If None is given equal weights
            are assumed. The default is None.
        processes : int, optional
            Number of CPU processes to be used to solve models. If it is set
            higher than the number of model runs, the number of model runs is
            used instead. The default is 1.
        join_results : bool, optional
            If to join results of multiple runs into a single DataFrame or not.
            The default is False.
        overwrite : bool, optional
           If to overwrite previous results. If False, the run will not proceed
           if results are already existent. The default is False
        solver : str, optional
            Name of the solver to be used. The default is 'cbc'.
        duals : list, optional
            List of constraints for which dual values are to be retrieved.
            The default is None.
        **kwargs : optional
            Additional arguments to be passed to solver.

        Returns
        -------
        Dictionary of results.

        """

        # return if results already existent and overwrite is not set to true
        if hasattr(self,"results") and overwrite==False:
            logger.warning("Model already has results, set overwrite=True to "
                           "run the model and overwrite previous results.")
            return self.results
        
        if pyo is None:
            logger.warning("Running the model is not possible as the required"
                           "dependency (pyomo) is not installed.")
            return
        
        # FIXME: next two lines are almost useless,just because pyomo_data is 
        # needed to identify sets in result processing, do this differently    
        # df_regions = self._create_regions_for_run(entities[0][0],autoinclude)
        # self.pyomo_data = self._create_pyomo_data(df_regions)
        
        # create argument list for all runs/optimizations
        if func is not None:
            arg = [[n,r,f,autoinclude,weights,
                    solver,duals, warmstart] for n,reg,f in zip(names,entities,func) for r in reg]
        else:
            arg = [[n,r,func,autoinclude,weights,
                    solver,duals, warmstart] for n,reg in zip(names,entities) for r in reg]
            
        # run single regions using multi-processing if more than one process
        # required
        if processes == 1:
            results=[]
            for o in arg:
                res = self._perform_single_run(*o)
                results.append(res)
        else:     
            pool = mp.Pool(processes=min(len(arg),processes),maxtasksperchild=15)
            results = pool.starmap(self._perform_single_run,arg)
            pool.terminate()
        self.res = results

        if results.count([])>0:
            logger.error(str(results.count([]))
                         +" optimizations did not succeed.")
            raise RuntimeError ("One or more optimizations failed."
                                "See log file for more information.")
            
        # aggregate results of different optimzations within a single run and
        # if agg_results set for different runs
        logger.info("Processing set of results")
        res = list()
        while len(results)>0:
            
            if len(results) == 1:
                res = res + results
                break
            
            result = results[0]
            
            if join_results ==  True:
                for k,v in result.items():
                    if k == "name":
                        result[k] = "aggregation_of_runs" if len(names)>1 else names[0]
                        continue
                    v = pd.concat([v]+[r[k] for r in results[1:]],
                                  axis=0,join="inner")
                    # aggregate depending on ft_param_agg
                    
                    if k not in self.ms_struct["ft_param_agg"].index:
                        if k.isupper():
                            logger.warning(f"Aggregation method for result component '{k}' "
                                           "not defined. Assuming 'merge'")
                            result[k] = v.drop_duplicates()
                        else:    
                             logger.warning(f"Aggregation method for result component '{k}' "
                                            "not defined. Assuming 'sum'.")
                             result[k] = v.groupby(level=[i for i in
                                                   range(v.index.nlevels)]).sum()

                    elif self.ms_struct["ft_param_agg"].loc[k, "VALUE"] == "sum":
                        result[k] = v.groupby(level=[i for i in
                                              range(v.index.nlevels)]).sum()
                    elif self.ms_struct["ft_param_agg"].loc[k, "VALUE"] == "eq":
                        result[k] = v.groupby(level=[i for i in
                                              range(v.index.nlevels)]).mean()
                    else:
                        raise ValueError("The aggregation method for parameter"+
                                             " values specified in ft_param_agg"+
                                             " is not implemented in fratoo.")
                        

                res.append(result)
                break

          
            if result["name"] in [r["name"] for r in results[1:]]:
                for k,v in result.items():
                    if k == "name":
                        continue
                    v = pd.concat([v]+[r[k] for r in results[1:]
                                       if r['name']==result['name']],
                                  axis=0,join="inner")
                    # aggregate depending on ft_param_agg
                    if v.empty:
                        pass
                    elif k not in self.ms_struct["ft_param_agg"].index:
                        if k.isupper():
                            logger.warning(f"Aggregation method for result component '{k}' "
                                           "not defined. Assuming 'merge'")
                            result[k] = v.drop_duplicates()
                        else:    
                             logger.warning(f"Aggregation method for result component '{k}' "
                                            "not defined. Assuming 'sum'.")
                             result[k] = v.groupby(level=[i for i in
                                                   range(v.index.nlevels)]).sum()

                    elif self.ms_struct["ft_param_agg"].loc[k, "VALUE"] == "sum":
                        result[k] = v.groupby(level=[i for i in
                                              range(v.index.nlevels)]).sum()
                    elif self.ms_struct["ft_param_agg"].loc[k, "VALUE"] == "eq":
                        result[k] = v.groupby(level=[i for i in
                                              range(v.index.nlevels)]).mean()
                    else:
                        raise ValueError("The aggregation method for parameter"+
                                             " values specified in ft_param_agg"+
                                             " is not implemented in fratoo.")
                res.append(result)
                results = [r for r in results if r['name']!=result['name']]
                continue
                
            res.append(result)
            results = results [1:]
            
            # result = results[0]
            # remaining = list()
            # for r in results[1:]:
            #     if result["name"] == r["name"] or join_results == True:
            #         for k,v in result.items():
            #             if k == "name":
            #                 continue
            #             v = pd.concat([v,r[k]],axis=0,join="inner")
            #             result[k] = v.groupby(level=[i for i in
            #                               range(v.index.nlevels)]).sum()
            #     else:
            #         remaining.append(r)
            # res.append(result)
            # results = remaining
         
        logger.info("Processed set of results")
        self.results = res
        
        return self.results


    def _perform_single_run(self, name, regions, func=None, autoinclude=True,
                            weights=None,
                            solver="cbc", duals=None, warmstart=False,
                            **kwargs):
        
        # FIXME: Check if any data and model is initialized
        # FIXME: let tee be set, maybe even if false, output if opt. sol. found
        
        df_regions = self._create_regions_for_run(regions,
                                                  autoinclude=autoinclude,
                                                  weights=weights)
        
        
        pyomo_data = self._create_run_data(df_regions, func)       

        logger.info("Creating pyomo model")
        pm = OSeMOSYS.model.create_instance(pyomo_data)
        logger.info("Created pyomo model")
        
        if duals is not None:
            pm.dual = pyo.Suffix(direction=pyo.Suffix.IMPORT)
        # FIXME: delete this
        # logger.info("Create matrix")
        # pm.write("mod.lp")
        # logger.info("Created matrix")
        
        # FIXME: delete this, or include in warmstart functionality properly
        if warmstart:
            if not os.path.exists("./warmstart/"):
                os.makedirs("./warmstart/")
            for v in os.listdir("./warmstart/"):
                data = pd.read_csv("./warmstart/"+v)
                data = data.set_index([c for c in data.columns if c != "VALUE"])
                data.loc[data["VALUE"]<0,"VALUE"] = 0
                
                if data.empty:
                    continue         
                getattr(pm,v[:-4]).set_values(data.to_dict(orient="dict")["VALUE"])
                


            
        logger.info("Solving model")
        # FIXME: delete line below
        logger.info("df_regions:\n{}".format(df_regions))
        if solver.startswith("highs"):   
            #opt = SolverFactory("appsi_highs")
            opt = appsi.solvers.Highs()
            opt.highs_options["solver"] = "ipm"
            #opt.highs_options["solver"] = "simplex"
            #opt.highs_options["presolve"] = "off"
            #opt.highs_options["simplex_scale_strategy"] = "off"
            opt.highs_options["run_crossover"] = "off"
            if "cov" in solver:
                opt.highs_options["run_crossover"] = "on"
            #opt.highs_options["primal_feasibility_tolerance"]=0.001
            #opt.highs_options["dual_feasibility_tolerance"]=100
            #opt.highs_options["ipm_optimality_tolerance"]=0.00001
            if "itlim" in solver:
                il = int(re.search(r'\d+$', solver).group())
                opt.highs_options["simplex_iteration_limit"] = il

            opt.highs_options["log_file"] = "highs"+str(datetime.datetime.now())+".log"
            #opt.config.load_solution=False
            sol = opt.solve(pm)
            
            logger.info("Solved model (termination condition: {})".format(
                            sol.termination_condition.name))
            solved = ((sol.termination_condition.name=="optimal")
                      or (sol.termination_condition.name == "maxIterations"))
            #opt.load_vars()
        elif solver.startswith("gurobi"):   
            opt = appsi.solvers.Gurobi()
            #opt = SolverFactory("gurobi")
            #opt.set_gurobi_param('Threads', 5)
            #opt.set_gurobi_param('Method', 1)
            opt.gurobi_options["Threads"] = 10
            opt.gurobi_options["Method"] = 2
            
            if "ht" in solver:
                opt.gurobi_options["FeasibilityTol"] = 1.e-5
                opt.gurobi_options["BarConvTol"] = 1.e-5
                opt.gurobi_options["OptimalityTol"] = 1.e-5

            if "co" in solver:
                opt.gurobi_options["Crossover"] = 1
            else:
                opt.gurobi_options["Crossover"] = 0
                
            opt.gurobi_options["LogToConsole"] = 1
            opt.gurobi_options["LogFile"] = "gurobi.log"
            #opt.config.load_solution=False
            sol = opt.solve(pm)
            logger.info("Solved model (termination condition: {})".format(
                            sol.termination_condition.name))
            solved = (sol.termination_condition.name=="optimal")
            
        else:
            opt = SolverFactory(solver)
            #opt.options = {'threads': 4}
            #opt.config.load_solution=False
            sol = opt.solve(pm,tee=True)#,warmstart=warmstart)
            logger.info("Solved model (status:"+
                        " {}, termination condition: {})".format(
                            sol.solver.status,sol.solver.termination_condition))
            solved = (sol.solver.termination_condition=="optimal")
        # FIXME: implement proper way to access solver status
        
        if not solved:
            logger.warning(f"Not possible to solve optimization (solver: {solver})"
                        " with df_regions:\n{}".format(df_regions))
            return []

        logger.info("Processing run results")
        # extract values of variables and save in dataframes
        results = dict()
        # FIXME:  maybe implement differently, e.g., name as key of dict with
        # dict of data as value
        results["name"] = name
        # FIXME: delete this (and wsd below), or include in warmstart function.
        #wsd=pm
        wsd = dict()
        #self.pm = pm
        for v in itertools.chain(pm.component_objects(pyo.Var, active=True),
                                 pm.component_objects(pyo.Param, active=True),
                                 pm.component_objects(pyo.Set, active=True),
                                 pm.component_objects(pyo.Constraint,
                                                      active=True)):

            # load data in dataframe
            #wsd[v.name]=v.get_values()
            
            if isinstance(v,pyo.Constraint):
                if duals is None or str(v) not in duals:
                    continue
                else:
                    if solver.startswith("highs") or solver.startswith("gurobi"):
                        df = pd.DataFrame(opt.get_duals([v[i] for i in v]).values(),
                                          index=[i for i in v],
                                          columns=["VALUE"])

                    else:
                        df = pd.DataFrame([pm.dual[v[i]] for i in v],
                                          index=[i for i in v],
                                          columns=["VALUE"])
         
      
            elif isinstance(v, pyo.Set) and (v.is_indexed() or "_index" in str(v)
                                           or "_domain" in str(v)):
                continue
            elif isinstance(v, pyo.Set):

                df = pd.Series(v.ordered_data(),
                               name="VALUE").to_frame()
                df.index.name="Index"
                # if v.isupper():
                #     # continue
                #     df = pd.Series(pyomo_data[None][v][None],
                #                       name="VALUE").to_frame()
                #     df.index.name="Index"
                # else:
                #     df = pd.DataFrame.from_dict(pyomo_data[None][v],
                #                                 orient="index",columns=["VALUE"])
                    
            elif isinstance(v,pyo.Var):

                df = pd.DataFrame.from_dict(v.extract_values(),
                                        orient="index",columns=["VALUE"])
                
            elif isinstance(v,pyo.Param):        
                df = pd.DataFrame.from_dict(v.extract_values_sparse(),
                                                      orient="index",columns=["VALUE"])
                # if default is 0, this is not necessary (and otherwise takes
                # a long time to process for some parameters)
                if  ((v.default()!= pyo.Param.NoValue)
                     and (v.default() > 0)):
                    df = df.reindex([i for i in v],fill_value=v.default())
                    
                df["VALUE"] = pd.to_numeric(df["VALUE"])

            # delete param rows if default  (and default not <=0)
            if (isinstance(v, pyo.Param) and (v.default() is not pyo.Param.NoValue)
                and v.default()<=0):
                df = df[~(df["VALUE"]==v.default())]

            # FIXME: make this nicer, the conditions and search for index names
            # convert to multiindex if necessary and set index names
            if isinstance(v, pyo.Set):
                pass
            elif len([ 1 for s in v.index_set().subsets() for ss in s.domain.subsets()])>1:
                # df.to_csv("df.csv")
                # pd.Series(df.index.tolist()).to_csv("index.csv")
                # pd.Series([ n  for s in df.index[0] for n,d in 
                #                                       pyomo_data[None].items()
                #                                       if n.isupper() and
                #                                       s in d[None]
                #                                       ]).to_csv("names.csv")
                df.index = pd.MultiIndex.from_tuples(df.index,
                                              names = [s.name if len(list(s.domain.subsets()))==1
                                                       else ss.name
                                                       for s
                                                       in v.index_set().subsets()
                                                       for ss in s.domain.subsets()])                
                # df.index = pd.MultiIndex.from_tuples(df.index,
                #                              names = [ n  for s in df.index[0]
                #                                       for n,d in 
                #                                       pyomo_data[None].items()
                #                                       if n.isupper() and
                #                                       s in d[None]
                #                                       ]) 
            else:

                # df.index = df.index.set_names([ n  for n,d in 
                #                                       pyomo_data[None].items()
                #                                       if n.isupper() and
                #                                       df.index[0] in d[None]
                #                                       ])
                
                df.index = df.index.set_names([s.name if len(list(s.domain.subsets()))==1
                                                       else ss.name
                                                       for s
                                                       in v.index_set().subsets()
                                                       for ss in s.domain.subsets()]) 
             
                
            if warmstart and isinstance(v,pyo.Var):
                data = df.copy()
                # if list(data.index.names).count("REGION")==2:
                #     data = data.droplevel(0)
                if "RUN" in data.index.names:
                    data = data.droplevel("RUN",errors="ignore")
                wsd[v.name] = data
                
            # remove all rows of variables for which values are 0 for all years
            if (isinstance(v, pyo.Var) and not df.empty 
                and df.index.names[0] is not None
                and 'YEAR' in df.index.names):

                rm = df.groupby([s for s in df.index.names
                                        if s!='YEAR']).max()
                rm = rm[rm['VALUE']==0]
                df = df.loc[~df.droplevel("YEAR").index.isin(rm.index)]
            
   
            # add run index level
            ind = df.index.to_frame()
            ind.insert(0,"RUN",[name]*len(ind))
            df.index = pd.MultiIndex.from_frame(ind)

            results[str(v)] = df
            
        for k,v in wsd.items():
            v.to_csv("./warmstart/"+str(k)+".csv")
        
        results = self._demap(results)
        logger.info("Processed run results")
        # free up memory
        del pyomo_data, sol, opt, pm
        gc.collect()
        return results
    

    def plot_results(self, var, x, xy=False, pack_name=None,xfilter=None, xscale=None,
                     zfilter=None, zgroupby=None,cgroupby=None,
                     filter_in=None, filter_out=None,ffilter=None,fgroupby=None,
                     an_change=False,
                     cleanup=True,reagg=None,
                     relative=None,zorder=None, naming=None, 
                     xlabel=None, ylabel=None,df_only=False, **kwargs):
        """ Plot results.
        

        Parameters
        ----------
        var : str
            Name of the result variable to be plotted.
        x : str or list
            Name of set(s) to be used as dimension of the x-axis.
        pack_name : str, optional
                Name of the results package name to be plotted. If none is
                given, the first in the results list is used. The default is
                None.
        xfilter : list, optional
            List of labels for which x-axis labels are tested and excluded if 
            not present in list. The default is None.
        xscale : series, optional
            Series that provides scaling factors for the data (values) for
            certain x-labels (index). This is mainly relevant if values
            represent multi-year periods that are to be scaled to an average
            year. The default is None.
        zfilter : dict, optional
            Dict to filter the data, i.e.,
            pick values (dict values) for certain sets (dict keys) and
            discard the remaining data. The default is None.
        zgroupby : str, or list of str, optional
            A set or list of sets indicating the levels to which data is
            grouped/aggregated. The default is None.
        cgroupby : dict, or func, optional
            A dict or function mapping level values to an aggregate value.
            The default is None.
        filter_in : dict of lists, optional
            Dict of list (of strings) for which data labels are tested and if 
            NOT present are excluded. Dict keys are the index level names.
            The default is None.
        filter_out : dict of lists, optional
            Dict of list (of strings) for which data labels are tested and if 
            present are excluded. Dict keys are the index level names.
            The default is None.
        reagg : dict, optional
            Dictionary that is used to rename index values after processing 
            and before performing a groupby - used to aggregate values.
            The default is None.                
        zorder : list, optional
            List of z-axis labels that is used to reorder (only relevant for
            appearance in some graph types). The default is None.
        naming : Series, optional
            Pandas Series that map names from the model (index) to names to 
            be used for the graph. The default is None.
        relative : str, optional
            String which gives the level which is used to calculate the
            relative values. If none is given, absolute values are plotted.
            The default is None.
        rel_filter_str_in : tuple, optional
            Tuple to filter the data used to calculate base for calculating
            the relative data, i.e., pick a string (second value) for certain
            sets (first value) and discard the all values that do not contain
            the string. The default is None.
        xlabel : str, optional
            Label for the x-axis. The default is None.
        ylabel : str, optional
            Label for the y-axis. The default is None.
        **kwargs : dict, optional
            Additional arguments passed to the DataFrame plot function.

        Returns
        -------
        fig : figure
            plotly figure object.
        df : DataFrame
            DataFrame with the results data for the chosen variable.

        """
        
        logger.info("Plot 1")
        # FIXME: implement filters before groupby, and just everything better/
        # more flexible
        
        if not hasattr(self, "results") and not isinstance(var,pd.DataFrame):
            logger.warning("No results to be plotted found.")
            return
        
        # load relevant dataframe
        if isinstance(var, pd.DataFrame):
            df = var
        elif pack_name is None:
            if var not in self.results[0].keys():
                logger.warning("Attribute '{}' not found.".format(var))
                return
            else:
                df = self.results[0][var].copy()
        else:
            run_ind = [r["name"] for r in self.results].index(pack_name)
            if var not in self.results[run_ind].keys():
                logger.warning("Attribute '{}' not found.".format(var))
                return
            else:
                df = self.results[run_ind][var].copy()    
        
        # convert x to list if given as string
        if isinstance(x,str):
            x = [x]
            
        # choose values for specific z dimensions
        if zfilter is not None:
            df = df.xs([v for v in zfilter.values()],
                       level=[k for k in zfilter.keys()],
                       axis=0)
        logger.info("Plot 3")
        # filter
        if (filter_in is not None) or (filter_out is not None):
            lab = dict()
            ind = df.index.to_frame()
            for il in df.index.names:
                
                if ((filter_in is not None) and (filter_out is not None) and
                    (il in filter_in.keys()) and (il in filter_out.keys())):
                    lab[il] = [e for e in ind[il].unique()
                               if any(str(s) in str(e) for s in filter_in[il]) and
                                all(str(s) not in str(e) for s in filter_out[il])]
                elif ((filter_in is not None) and (il in filter_in.keys())):
                    lab[il] = [e for e in ind[il].unique()
                               if any(str(s) in str(e) for s in filter_in[il])]
                elif ((filter_out is not None) and (il in filter_out.keys())):
                    lab[il] = [e for e in ind[il].unique()
                               if all(str(s) not in str(e) for s in filter_out[il])]
                else:
                    lab[il] = slice(None)
            df = df.loc[tuple([s for s in lab.values()]),:]
            
        logger.info("Plot 4")    
        # scale values if required  
        if xscale is not None:
            df.loc[:,"VALUE"] = df["VALUE"].multiply(xscale,axis=0)#.combine_first(df)
            
                
        # FIXME: delete
        # if elfilter is not None:
        #     df = df.iloc[[True if all([any(s in e[list(elfilter.keys()).index(k)] for s in elfilter[k]) for k in elfilter.keys()])
        #                 else False 
        #                 for e in zip(*[df.index.to_frame().to_dict(orient="list")[k] for k in elfilter.keys()])],:]
                                
        
        # groupby given z dimension
        if zgroupby is not None:
            df = df.groupby(level=list(set(zgroupby+x)), axis=0).sum()
        
        logger.info("Plot 5")
        # groupby content of level based on function or dict
        if cgroupby is not None:
            for k,v in cgroupby.items():
                idx = df.index.to_frame()
                if isinstance(v, dict):
                    agg = df.index.get_level_values(k).to_series().replace(v).to_list()
                if callable(v):
                    agg = [v(i) for i in df.index.get_level_values(k)]
                
                idx = idx.rename(columns={k:k+"_"})
                idx.insert(list(idx.columns).index(k+"_"), k, agg)
                df.index = pd.MultiIndex.from_frame(idx)
        
            df = df.groupby([l for l in df.index.names if l[:-1] not in list(cgroupby.keys())], axis=0).sum()
            
        logger.info("Plot 6")
        # calculate relative values if required
        if relative is not None:
            if isinstance(relative,list):
                df = df/df.groupby([i for i in df.index.names if i not in relative],
                                   axis=0).sum()
            if isinstance(relative,dict):
                df = df/df.xs([v for v in relative.values()],
                           level=[k for k in relative.keys()],
                           axis=0)
        logger.info("Plot 7")
        if an_change:
            dfdiv = df.copy()
            dfdiv["YEAR"] = dfdiv.index.get_level_values("YEAR")
            df = df.groupby(level=[i for i in df.index.names
                             if i !="YEAR"]).diff()["VALUE"].div(dfdiv.groupby(level=[i for i in df.index.names
                             if i !="YEAR"]).diff()["YEAR"]).to_frame()
        logger.info("Plot 8")                                                                  
        # groupby given z dimension
        if fgroupby is not None:
            for k in fgroupby.keys():
                # df = getattr(df.groupby(level=list(set(fgroupby[k]+[x])),
                #                         axis=0),k)()  
                idx = df.abs().groupby(level=list(set(fgroupby[k])),
                                        axis=0).idxmax()
               
                df = df.loc[idx[0]]
        logger.info("Plot 9")      
        if ffilter is not None:
            df = df.loc[tuple([ffilter[n] if n in ffilter.keys()
                               else slice(None)
                               for n in df.index.names]),:]
            
        # FIXME: remove this/implement differently - just important if some
        # very small negative values come out of optimization
        # clean up
        logger.info("Plot 10")
        if  cleanup:
            # remove any negative numbers (due to inaccuracies when solving)
            df[df<0] = 0
            # remove any columns if all values are zero (tolerance of 10^-14)
            df = df.loc[:,df.max()>10**-20]

        logger.info("Plot 11")     
        
        
        if reagg is not None:
            df = df.rename(index=reagg)
            df = df.groupby(df.index.names).sum()
            
        # unstack all but x dimensions and drop top level "VALUE" column level
        df = df.unstack([i for i in df.index.names if i not in x])
        df.columns = df.columns.droplevel()
             
        
        # if more than one column level existent, flatten
        logger.info("Plot 12")
        if (len(df.columns.names) > 1 ) and not df_only:
            for i in range(len(df.columns.names)):
                #idx = df.columns
                df.columns = df.columns.set_levels([df.columns.levels[i].astype(str)],level=[i])
  
            df.columns = df.columns.map(':'.join)
        logger.info("Plot 13")
        # groupby content of level based on function or dict
        # if cgroupby is not None:
        #     if isinstance(cgroupby, dict):
        #         by = df.columns.to_series().replace(cgroupby).to_list()
        #     if callable(cgroupby):
        #         by = [cgroupby(i) for i in df.columns]
                
        #     df = df.groupby(by=by, axis=1).sum()      
        # logger.info("Plot 8")    

        # order z dimension entries
        if zorder is not None:
            df = df[[c for c in zorder if c in df.columns]
                    +[c  for c in df.columns if c not in zorder]]
        logger.info("Plot 14")
        if xy:
            kwargs["x"] = df.index[0]
            kwargs["y"] = df.index[1]
            df = df.T
            
        # rename if necessary
        if naming is not None:
            df = df.rename(index=naming)
            df = df.rename(columns=naming)
            # update color map names
            if "color_discrete_map" in kwargs.keys():
                kwargs["color_discrete_map"] = {naming[k] if k in naming.index
                                                else k : v
                                                for k, v in 
                                                kwargs["color_discrete_map"].items()
                                                }
        logger.info("Plot 15")
        
        if df_only:
            return df
        
        # flatten index if necessary
        if len(df.index.names) > 1:
            for i in range(len(df.index.names)):
                df.index = df.index.set_levels([df.index.levels[i].astype(str)],
                                               level=[i])
  
            df.index = df.index.map(':'.join)

        fig = df.plot(**kwargs)

        logger.info("Plot 16")
        if xlabel is not None:
            fig.update_layout(xaxis_title=xlabel)
        if ylabel is not None:    
            fig.update_layout(yaxis_title=ylabel)
        
        logger.info("Plot 17")
        return fig,df 


    
    def plot_capacity(self, **kwargs):
        """ Plot the capacity of technologies over time.
        

        Parameters
        ----------
        **kwargs : dict, optional
            Takes all arguments that plot_results takes (except pre-set
            variable and x-axis) and passes them on.

        Returns
        -------
        fig : figure
            plotly figure object.
        df : DataFrame
            DataFrame with the results data for the chosen var.

        """

        return self.plot_results(var="TotalCapacityAnnual",
                                 x="YEAR",
                                 **kwargs)   
    
    def plot_generation(self, **kwargs):
        """ Plot the generation of technologies over time.
        

        Parameters
        ----------
        **kwargs : dict, optional
            Takes all arguments that plot_results takes (except pre-set
            variable and x-axis) and passes them on.

        Returns
        -------
        fig : figure
            plotly figure object.
        df : DataFrame
            DataFrame with the results data for the chosen var.

        """
        return self.plot_results(var="TotalProductionByTechnologyAnnual",
                                 x="YEAR",
                                 **kwargs)  

    def plot_map(self,var, mapfile, map_column, loc_column, mapping=None,
                 map_type="map", show_missing=True,
                 xy=False, pack_name=None,xfilter=None, xscale=None,
                 zfilter=None, zgroupby=None,cgroupby=None,
                 filter_in=None, filter_out=None,ffilter=None,fgroupby=None,
                 entities = "all",
                 an_change=False,
                 cleanup=True,
                 naming=None,
                 relative=None,
                 zlabel=None, **kwargs):
        """ Plot results as map.
        
        
        Parameters
        ----------
        var : str, DataFrame
            Either the name of the result variable to be plotted or a dataframe
            of similarly structured data.
        pack_name : str, optional
                Name of the results package name to be plotted. If none is
                given, the first in the results list is used. The default is
                None.
        mapfile : str, GeoDataFrame
            GeoDataFrame or path to the shapefile (normal map) or json (hexmap)
            containing the geographic data.
        map_column : str
            Name of the column in the shapefile to be used to match entities
            with the fratoo model entities.
        entities : list of str, str
            Entities to be shown on the map. Either 'all' or a list of strings
            of entity codes. The default is 'all'.
        show_missing :  bool
            If map entities without data are shown or not.
        loc_column : str
            Name of the column in the shapefile with entitity names to be used
            for plotting.
        relative : str, optional
            String which gives the level which is used to calculate the
            relative values. If none is given, absolute values are plotted.
            The default is None.
        rel_filter_str_in : tuple, optional
            Tuple to filter the data used to calculate base for calculating
            the relative data, i.e., supply a list of values (second tuple 
            value) for certain set (first tuple value) and discard all other
            values that are not present in the list. The default is None.
        zfilter : dict, optional
            Set-value pairs (strings) to filter the data based on values in 
            specific sets. The default is None.
        cgroupby : dict, optional
            A dict where the key gives the multiindex level and the value a
            dict or function mapping level values to an aggregate value.
            The default is None.
        mapping : dict, optional
            Dict which defines if names of specific fratoo entities (keys) are
            mapped to other names (values) to conform with shapefile names.
            The default is None.
        naming : Series, optional
            Pandas Series that map names from the model (index) to names to 
            be used for the graph. The default is None. 
        map_type : str, optional
            Type of the map to be plotted. Options are "map" for a normal map,
            and "hex" for hex maps. The default is "map".
        zlabel : str, optional
            Label for the z-axis. The default is None.
        **kwargs : dict, optional
            Additional arguments passed to the geopandas plot function.

        Returns
        -------
        fig : figure
            plotly figure object.
        df : DataFrame
            DataFrame with the results data for the chosen var.

        """

        # FIXME: improve overall (e.g., some mapfile.dissolve approach to be 
        # able to aggregate and plot different scales with same mapdata)
       
        
        
        logger.info("Plot 1")
        # FIXME: implement filters before groupby, and just everything better/
        # more flexible
        
        if gpd is None or Polygon is None:
            logger.warning("Map creation is not possible as the required"
                           " dependencies (geopandas and/or shapely)"
                           " are not available.")
            return
        
        if not hasattr(self, "results") and not isinstance(var,pd.DataFrame):
            logger.warning("No results to be plotted found.")
            return
        
        # load relevant dataframe
        if isinstance(var, pd.DataFrame):
            df = var
        elif pack_name is None:
            if var not in self.results[0].keys():
                logger.warning("Attribute '{}' not found.".format(var))
                return
            else:
                df = self.results[0][var].copy()
        else:
            run_ind = [r["name"] for r in self.results].index(pack_name)
            if var not in self.results[run_ind].keys():
                logger.warning("Attribute '{}' not found.".format(var))
                return
            else:
                df = self.results[run_ind][var].copy()       
        logger.info("Plot 2")
        
        # choose values for specific z dimensions
        if zfilter is not None:
            df = df.xs([v for v in zfilter.values()],
                       level=[k for k in zfilter.keys()],
                       axis=0)
        logger.info("Plot 3")
        # filter
        if (filter_in is not None) or (filter_out is not None):
            lab = dict()
            ind = df.index.to_frame()
            for il in df.index.names:
                
                if ((filter_in is not None) and (filter_out is not None) and
                    (il in filter_in.keys()) and (il in filter_out.keys())):
                    lab[il] = [e for e in ind[il].unique()
                               if any(str(s) in str(e) for s in filter_in[il]) and
                                all(str(s) not in str(e) for s in filter_out[il])]
                elif ((filter_in is not None) and (il in filter_in.keys())):
                    lab[il] = [e for e in ind[il].unique()
                               if any(str(s) in str(e) for s in filter_in[il])]
                elif ((filter_out is not None) and (il in filter_out.keys())):
                    lab[il] = [e for e in ind[il].unique()
                               if all(str(s) not in str(e) for s in filter_out[il])]
                else:
                    lab[il] = slice(None)
            logger.info(str(tuple([s for s in lab.values()])))
            df = df.loc[tuple([s for s in lab.values()]),:]
            
        logger.info("Plot 4")    
        # scale values if required  
        if xscale is not None:
            df["VALUE"] = df["VALUE"].multiply(xscale,axis=0)#.combine_first(df)
            
                
        # FIXME: delete
        # if elfilter is not None:
        #     df = df.iloc[[True if all([any(s in e[list(elfilter.keys()).index(k)] for s in elfilter[k]) for k in elfilter.keys()])
        #                 else False 
        #                 for e in zip(*[df.index.to_frame().to_dict(orient="list")[k] for k in elfilter.keys()])],:]
                                
        
        # groupby given z dimension
        if zgroupby is not None:
            df = df.groupby(level=list(set(zgroupby)), axis=0).sum()
        
        logger.info("Plot 5")
        # groupby content of level based on function or dict
        if cgroupby is not None:
            for k,v in cgroupby.items():
                idx = df.index.to_frame()
                if isinstance(v, dict):
                    agg = df.index.get_level_values(k).to_series().replace(v).to_list()
                if callable(v):
                    agg = [v(i) for i in df.index.get_level_values(k)]
                
                idx = idx.rename(columns={k:k+"_"})
                idx.insert(list(idx.columns).index(k+"_"), k, agg)
                df.index = pd.MultiIndex.from_frame(idx)
            # FIXME: This used to be a .mean() but sum makes more sense!?
            # better introduce option to choose groupby function?
            df = df.groupby([l for l in df.index.names if l[:-1] not in list(cgroupby.keys())], axis=0).sum()
        print(df)
        logger.info("Plot 6")
        # calculate relative values if required
        if relative is not None:
            if isinstance(relative,list):
                df = df/df.groupby([i for i in df.index.names if i not in relative],
                                   axis=0).sum()
            if isinstance(relative,dict):
                df = df/df.xs([v for v in relative.values()],
                           level=[k for k in relative.keys()],
                           axis=0)
        logger.info("Plot 7")
        if an_change:
            dfdiv = df.copy()
            dfdiv["YEAR"] = dfdiv.index.get_level_values("YEAR")
            df = df.groupby(level=[i for i in df.index.names
                             if i !="YEAR"]).diff()["VALUE"].div(dfdiv.groupby(level=[i for i in df.index.names
                             if i !="YEAR"]).diff()["YEAR"]).to_frame()
        logger.info("Plot 8")                                                                  
        # groupby given z dimension
        if fgroupby is not None:
            for k in fgroupby.keys():
                # df = getattr(df.groupby(level=list(set(fgroupby[k]+[x])),
                #                         axis=0),k)()  
                idx = df.abs().groupby(level=list(set(fgroupby[k])),
                                        axis=0).idxmax()
               
                df = df.loc[idx[0]]
        logger.info("Plot 9")
        if ffilter is not None:
            df = df.loc[tuple([ffilter[n] if n in ffilter.keys()
                               else slice(None)
                               for n in df.index.names]),:]          
        # FIXME: remove this/implement differently - just important if some
        # very small negative values come out of optimization
        # clean up
        logger.info("Plot 10")
        if  cleanup:
            # remove any negative numbers (due to inaccuracies when solving)
            df[df<0] = 0
            # remove any columns if all values are zero (tolerance of 10^-14)
            df = df.loc[:,df.max()>10**-20]


        # rename if necessary
        if naming is not None:
            df = df.rename(index=naming)
            df = df.rename(columns=naming)
            # update color map names
            if "color_discrete_map" in kwargs.keys():
                kwargs["color_discrete_map"] = {naming[k] if k in naming.index
                                                else k : v
                                                for k, v in 
                                                kwargs["color_discrete_map"].items()
                                                }
        
        # sum up if not all dimensions chosen
        df = df.groupby(["REGION"]).sum()

        # apply mapping  
        if mapping is not None:
            df.index = [mapping[r] if r in mapping.keys()
                        else r
                        for r in df.index]
            

        if map_type == "map":
            if isinstance(mapfile,str):
                mapdata = gpd.read_file(mapfile)
                
            else:
                mapdata = mapfile
                
            mapdata = mapdata.to_crs(epsg=4326)
        
        elif map_type == "hex":
           
            with open(mapfile, "r", encoding="utf-8") as json_file:
                data = json.load(json_file)
            data = data["hexes"]
            
            if entities != "all":
                data = {lad:v for lad,v in data.items() if lad in entities}
            
            mapdata = pd.DataFrame.from_dict(data,orient="index")
            
            #hexdata = df_data.loc[:,['id','q','r']]
            mapdata.index.name=map_column
            mapdata = mapdata.rename(columns={"n":map_column[:-2]+"NM"})
            mapdata = mapdata.reset_index()

             
        mapdata = mapdata.merge(right=df,left_on=map_column, right_on='REGION',
                                how='left')

        # FIXME: add different approaches, i.e., filling with 0 or not based
        # on parameter
        # handle missing data
        mapdata["VALUE"] = mapdata["VALUE"].fillna(0)
        
        if map_type == 'hex':
       
            r = 0.5 / np.sin(np.pi/3)
            #o = 0.5 * np.tan(np.pi/3)
            y_diff = np.sqrt(1 - 0.5**2)  
  
  
            mapdata = mapdata.set_index("LAD23CD")
            for hi in mapdata.index:
  
                row = mapdata.loc[hi, "r"]
                col = mapdata.loc[hi, "q"]
                
                if row % 2 == 1:
                    col = col + 0.5
                row = row * y_diff
                
                c = [[col + math.sin(math.radians(ang)) * r,
                      row + math.cos(math.radians(ang)) * r] 
                     for ang in range(0, 360, 60)]
  
                mapdata.loc[hi, "geometry"] = Polygon(c)
                
            mapdata = gpd.GeoDataFrame(mapdata, geometry="geometry")
            mapdata = mapdata.reset_index()
            

               
        # FIXME: plot nan in special colour
        # FIXME: improve in general
        #mapdata = mapdata.set_index("LAD20NM")
        
        mapdata[zlabel] = mapdata["VALUE"]
        geojson = mapdata.__geo_interface__
                
        fig = px.choropleth(mapdata,
                        geojson=geojson,
                        locations= loc_column,
                        color=zlabel,
                        featureidkey="properties."+loc_column,
                        projection="mercator",
                        #color_continuous_scale="Emrld",
                        #hover_name=loc_column[:-2]+"NM",
                        #hover_data={loc_column:False},
                        **kwargs)
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})     
        fig.update_geos(fitbounds="locations", visible=False)

        return fig,df


        

    def get_entities(self, scale=None):
        """ Get spatial entities of model.
        

        Parameters
        ----------
        scale : integer, optional
            Scale for which spatial entities are to be returned. All scales if
            None. The default is None.

        Returns
        -------
        entities : list
            List of spatial entities.

        """
        
        if scale == None:
             entities = self.input_data["REGION"]["VALUE"].tolist()
        else:
            entities = self.ms_struct["ft_scale"][
                self.ms_struct["ft_scale"]["VALUE"]==scale].index.tolist()
        
        return entities
    
    
    def _demap(self, datadict, sep=":"):
        
        # FIXME: improve function, make more efficient (!)
        
        data = dict()
        
        for var, df_values in datadict.items():

            #logger.info("demap:"+str(var))
            # process results if indexed over region set, i.e., pull region
            # out of other sets (fuel, technology, etc.) if not explicitely
            # given/different to actual region (only in the case of fuel or 
            # emission)
            # FIXME: checking the type not needed if no name or other fields
            # added
            if isinstance(df_values,pd.DataFrame) and (
                "REGION" in df_values.index.names or
                "REGION" == df_values.index.name
            ) and not df_values.empty:

                idx = df_values.index.to_frame()

                # FIXME: could be also done with list comprehension (?) as done
                # above, maybe make all this consistent
                idx["REGION"] = "run_region"
                
                if "TECHNOLOGY" in idx.columns:
                    idx["REGION"] = idx["TECHNOLOGY"].str.split(sep,
                                                                expand=True)[0]
                    idx["TECHNOLOGY"] = idx["TECHNOLOGY"].str.split(sep,
                                                                expand=True)[1]
                    
                elif "STORAGE" in idx.columns:
                    idx["REGION"] = idx["STORAGE"].str.split(sep,
                                                             expand=True)[0]
                    idx["STORAGE"] = idx["STORAGE"].str.split(sep,
                                                              expand=True)[1]
 
                if "EMISSION" in idx.columns:
                    
                    # idx[idx["REGION"]=="run_region"]["REGION"] = idx[
                    #     "EMISSION"].str.split(":",expand=True)[0]
                    # idx[idx["EMISSION"].str.split(":",expand=True) ==
                    #     idx["REGION"]]["EMISSION"] = idx[
                    #         "EMISSION"].str.split(":",expand=True)[1]
                         
                    idx["REGION"] = idx.apply(
                        lambda r: r["EMISSION"].split(sep)[0]
                        if r["REGION"] == "run_region"
                        else r["REGION"],
                        axis=1
                        )
                    idx["EMISSION"] = idx.apply(
                        lambda r: r["EMISSION"].split(sep)[1]
                        if r["EMISSION"].split(sep)[0] == r["REGION"]
                        else r["EMISSION"],
                        axis=1
                    )

                elif "FUEL" in idx.columns:
                    
                    idx["REGION"] = idx.apply(
                        lambda r: r["FUEL"].split(sep)[0]
                        if r["REGION"] == "run_region"
                        else r["REGION"],
                        axis=1
                        )                    
                    idx["FUEL"] = idx.apply(
                        lambda r: r["FUEL"].split(sep)[1]
                        if r["FUEL"].split(sep)[0] == r["REGION"]
                        else r["FUEL"],
                        axis=1
                    )

                if (idx["REGION"]=="run_region").any():
                    
                    #idx = idx.explode("REGION")
                    #.index = pd.MultiIndex.from_frame(idx)
                    #df_values = df_values.sort_index()
                    df_values = df_values.reset_index()
                    df_values["REGION"] = [list(datadict["TECHNOLOGY"]["VALUE"].str.split(sep,
                                            expand=True)[0].drop_duplicates())] * len(idx)
                    df_values = df_values.explode("REGION")
                    df_values = df_values.set_index(list(idx.columns))
                else:
                    df_values.index = pd.MultiIndex.from_frame(idx)
                    df_values = df_values.sort_index()
                    #df_values = df_values.explode("REGION")
            #logger.info("demap2")    
            if (isinstance(df_values,pd.DataFrame) and var.isupper() 
                and not df_values.empty):
                if var=="TECHNOLOGY":
                    df_values = df_values["VALUE"].str.split(sep,
                                                                expand=True)[1]
                    df_values = df_values.drop_duplicates()
                    df_values.name = "VALUE"
                    df_values = df_values.to_frame()
                if var=="REGION":
                    # if "TECHNOLOGY" in data.keys():
                    #     df_values["VALUE"] = data["TECHNOLOGY"]["VALUE"].str.split(":",
                    #                                                 expand=True)[0].drop_duplicates()

                    df_values = datadict["TECHNOLOGY"]["VALUE"].str.split(sep,
                                                                expand=True)[0]
                    df_values = df_values.drop_duplicates()
                    df_values.name = "VALUE"
                    df_values = df_values.to_frame()


                if var=="FUEL":
                    df_values = df_values["VALUE"].str.split(sep,
                                                                expand=True)[1]
                    df_values = df_values.drop_duplicates()
                    df_values.name = "VALUE"
                    df_values = df_values.to_frame()
                if var=="EMISSION":
                    df_values = df_values["VALUE"].str.split(sep,
                                                                expand=True)[1]
                    df_values = df_values.drop_duplicates()
                    df_values.name = "VALUE"
                    df_values = df_values.to_frame()
                if var=="STORAGE":
                    df_values = df_values["VALUE"].str.split(sep,
                                                            expand=True)[1]
                    df_values = df_values.drop_duplicates()
                    df_values.name = "VALUE"
                    df_values = df_values.to_frame()
            data[var] = df_values
            
        return data
    

    def expand_results(self, params=None):
        # FIXME: Improve, add comments, docstring, etc.
        # currently processing in any case - these are basic results and quick
        # to calculate
        logger.info("Expanding results")
       
        # if params is None:
        #     return
        
        results = self.results.copy()
       
        for i in range(len(results)):
            
            #if ("basic" in params) or ("extendedcost" in params):
            # calculate and add basic result variable to result data
            # fill in missing operational lifes with default 1 year
            results[i]["OperationalLife"] = results[i]["OperationalLife"].reindex(results[i]["NewCapacity"].add(
                                                                                  results[i]["ResidualCapacity"],fill_value=0).unstack().index,
                                                                                  fill_value=1)
            
            # this uses 364 days per year to make sure only the periods 
            # < (not <=)x+lifetime are included (as it is implemented in 
            # OSeMOSYS)
            results[i]["TotalCapacityAnnual"] = results[i]["NewCapacity"].unstack().droplevel(0,axis=1)
            results[i]["TotalCapacityAnnual"].columns = pd.to_datetime(results[i]["TotalCapacityAnnual"].columns,format='%Y')
            results[i]["TotalCapacityAnnual"] = results[i]["TotalCapacityAnnual"].apply(lambda x: x.rolling(str(int(results[i]["OperationalLife"].loc[x.name,"VALUE"])*364)+"D",min_periods=0).sum(),axis=1)
            results[i]["TotalCapacityAnnual"].columns = results[i]["TotalCapacityAnnual"].columns.year
            results[i]["TotalCapacityAnnual"] = pd.concat([results[i]["TotalCapacityAnnual"]], keys=['VALUE'], axis=1).stack()
            results[i]["TotalCapacityAnnual"] = results[i]["TotalCapacityAnnual"].add(results[i]["ResidualCapacity"],fill_value=0)
            
            results[i]["ProductionByTechnologyAnnual"] = ((results[i]["RateOfActivity"]
                                                           *results[i]["OutputActivityRatio"]).dropna()
                                                          *results[i]["YearSplit"]).groupby(["RUN",
                                                                                            "REGION",
                                                                                            "TECHNOLOGY",
                                                                                            "YEAR",
                                                                                            "FUEL"]).sum()
                                                                                                   
            results[i]["TotalProductionByTechnologyAnnual"] = results[i]["ProductionByTechnologyAnnual"].groupby( ["RUN",
                                                                                                                "REGION",
                                                                                                                "TECHNOLOGY",
                                                                                                                "YEAR"]).sum()
            results[i]["UseByTechnologyAnnual"] = ((results[i]["RateOfActivity"]
                                                    *results[i]["InputActivityRatio"]).dropna()
                                                   *results[i]["YearSplit"]).groupby(["RUN",
                                                                                      "REGION",
                                                                                      "TECHNOLOGY",
                                                                                      "YEAR",
                                                                                      "FUEL"]).sum()
                                                                                      
            # FIXME: add exo. emission with necessary check if respective
            # dataframe exists
            # .add(results[0]["AnnualExogenousEmission"],fill_value=0)
            results[i]["AnnualEmissions"] =  (((results[i]["RateOfActivity"]
                                               *results[i]["EmissionActivityRatio"]).dropna()
                                               *results[i]["YearSplit"])
                                               
                                                               ).groupby(["RUN",
                                                                          "REGION",
                                                                          "EMISSION",
                                                                          "YEAR"]).sum()
                                                                          
            idx =    results[i]["AnnualEmissions"].index.to_frame()                                                       
            idx["REGION"] = idx.apply(
                lambda r: r["EMISSION"].split(":")[0]
                if ":" in r["EMISSION"]
                else r["REGION"],
                axis=1
                )
            idx["EMISSION"] = idx.apply(
                lambda r: r["EMISSION"].split(":")[1]
                if ":" in r["EMISSION"]
                else r["EMISSION"],
                axis=1
            )
            
            results[i]["AnnualEmissions"].index = pd.MultiIndex.from_frame(idx)
            results[i]["AnnualEmissions"] = results[i]["AnnualEmissions"].groupby(["RUN",
                                                                                   "REGION",
                                                                                   "EMISSION",
                                                                                   "YEAR"]).sum() 
               
            # FIXME: uncomment emission penalty  
            results[i]["CostCapital"] = (results[i]["NewCapacity"]
                                         *results[i]["CapitalCost"]
                                         *results[i]["CapitalRecoveryFactor"]
                                         *results[i]["PvAnnuity"]
                                         /results[i]["DiscountFactor"]).add(0,
                                         #results[i]["DiscountedTechnologyEmissionsPenalty"],
                                         fill_value=0).subtract(
                                         results[i]["DiscountedSalvageValue"],
                                         fill_value=0).dropna()
            results[i]["CostInv"] = (results[i]["NewCapacity"]
                                         *results[i]["CapitalCost"]
                                         *results[i]["CapitalRecoveryFactor"]
                                         *results[i]["PvAnnuity"]).add(0,
                                         #results[i]["DiscountedTechnologyEmissionsPenalty"],
                                         fill_value=0).dropna()
            results[i]["CostFixed"] = (results[i]["TotalCapacityAnnual"]
                                         *results[i]["FixedCost"]
                                         /results[i]["DiscountFactorMid"]).dropna()
            
            
            results[i]["CostVariable"] = ((results[i]["RateOfActivity"]
                                           *results[i]["YearSplit"]
                                           *results[i]["VariableCost"]
                                           /results[i]["DiscountFactorMid"]
                                          ).dropna()
                                          ).groupby(["RUN",
                                                     "REGION",
                                                     "TECHNOLOGY",
                                                     "YEAR"]).sum()

            results[i]["CostTotal"] =  (results[i]["CostCapital"].add(
                                        results[i]["CostFixed"], fill_value=0
                                        ).add(
                                        results[i]["CostVariable"], fill_value=0
                                        ))
            # FIXME: add comment below to some check
            # sum of above has been compared with objective value and it
            # seems fine                                
                                                     
            # FIXME: include storage cost once implemented in model file, etc.
            # results[i]["CostCapitalStorage"] = (results[i]["NewStorageCapacity"]
            #                              *results[i]["CapitalCostStorage"]
            #                              /results[i]["DiscountFactor"]).subtract(
            #                              results[i]["DiscountedSalvageValueStorage"],
            #                              fill_value=0).dropna()
                                                         


        self.results = results
        
        logger.info("Expanded results")
        
        return
    
    def aggregate_results(self):
        #FIXME: Improve, add comments, docstring, etc.
        logger.info("Aggregating results")
        logger.info("1")
        results = self.results.copy()
        res = list()
       
        logger.info("2")
        if len(results) == 1:
            res = res + results

        else:
            result = results[0]
            logger.info("3")
    
            for k,v in result.items():
                if k == "name":
                    result[k] = "aggregation_of_runs"
                    continue
                logger.info("4")
                v = pd.concat([v]+[r[k] for r in results[1:] if k in r],
                              axis=0,join="inner")
                logger.info("5")
                logger.info(len(v))
                result[k] = v
                # result[k] = v.groupby(level=[i for i in
                #                       range(v.index.nlevels)]).sum()
                logger.info(len(result[k]))
                logger.info("6")
            res.append(result)
            logger.info("7")
            
        self.results = res
        return
        
    def save_results(self, path):
        """ Save results to zip files.
        

        Parameters
        ----------
        path : str
            Path to the directory where the results should be saved.

        Returns
        -------
        None.

        """
        if not hasattr(self, "results"):
            logger.warning("No results to be saved found.")
            return
        if fl is None:
            logger.warning("The required dependency to handle results"
                           "(frictionless) is not available.")
            return
        # add seperator in the end if not present
        path = os.path.join(path, '')
        
        # go through each of separately saved run results and save to zip file
        logger.info("Saving results")
        for i in range(len(self.results)):
            
            if not os.path.exists(path+self.results[i]['name']):
                os.makedirs(path+self.results[i]['name'])
                
            package = fl.Package()
            for k in self.results[i].keys():
                if k=='name':
                    package.name = self.results[i][k]
                    continue
                if self.results[i][k].empty == True:
                    continue               
                package.add_resource(fl.describe(self.results[i][k], name=k.lower(), title=k,
                                                 path=k.lower()+'.csv'))     
                self.results[i][k].to_csv(path+self.results[i]['name']+'/'+k.lower()+'.csv')
            package.to_json(path+self.results[i]['name']+'/'+'datapackage.json')  
            shutil.make_archive(path+self.results[i]['name'], 'zip',
                                path+self.results[i]['name']) 
            shutil.rmtree(path+self.results[i]['name']) 
            # FIXME: below is more concise but very, very slow, thus,
            # alternative above
            #package.to_zip(path+package.name+'.zip')#resolve=['memory']
            
        logger.info("Saved results")
        return
    

    def load_results(self, path, exclude=None):
        """Load results from zip files into the model.
        

        Parameters
        ----------
        path : str
            Path for a result file or to the directory where one or more the 
            results zip files are saved. All zip files in the folder will be 
            loaded.
        data: str
            List of parameter and variable names to be excluded. The 
            default is None.

        Returns
        -------
        None.

        """
        
        if not os.path.exists(path):
            logger.warning('The result directory or file does not exist.')
            return
        if fl is None:
            logger.warning("The required dependency to handle results"
                           "(frictionless) is not available.")
            return
        
        logger.info("Loading results")
        
        if os.path.isfile(path):
            packf = [path]
        
        else:
            # add seperator in the end if not present
            path = os.path.join(path, '')
        
            # create list of all zip files in directory
            packf = sorted([path+f for f in os.listdir(path) if f.endswith('.zip')])
            if not packf:
                logger.warning("There are no results in the directory to load.")
                return          
            
        results = []


        # go through each zip file and add as dictionary to results list
        for f in packf:
            run = dict()
            
            zf = zipfile.ZipFile(f)
            pack = fl.Package(json.load(zf.open('datapackage.json')))
            
            run['name'] = pack.name
            for r in pack.resources:
                if exclude is not None and r.title in exclude:
                    continue
                run[r.title] = pd.read_csv(zf.open(r.path),
                                           index_col=r.schema['primaryKey'],
                                           dtype = {c["name"]:("str" if c["type"]=="string" else
                                           "int" if c["type"]=="integer" else
                                           "float" if c["type"]=="number" else
                                           "str") for c in r.schema["fields"]})
                # FIXME: delete this, works as well, but quite slow
                #r.scheme='file'
                #r.format='csv'
                #run[r.title] = r.to_pandas()
            results.append(run)

        self.results = results
        
        logger.info("Loaded results")
        


    # FIXME: implement this function        
    def save_input_data_to_datapackage(self, path):
        pass
    
    # FIXME: implement this function
    def check_data_consistency(self):
        
        # FIXME: introduce general consistency checks for input data
        # (multi-scale regions present in param/set files, etc.)
        # FIXME: introduce check in consistency of multi-scale data (e.g.,
        # should be checked if parents are always on higher scale)?
        # FIXME: potential for loads of other (unecessary?) checks (all params
        # set for sets, etc.)
        pass

    
    # FIXME: implement this function, saving model input data, results, etc.     
    def save_model(self, path):
        pass
    
    
    def get_model_data(self, ms=False):
        
        if not hasattr(self,"input_data"):
            raise AttributeError("Input data can not be retrieved as they are"+
                                 " not existing.")
        
        
        logger.info("Retrieve input data")
        
        if self.ms_struct is not None and ms:
            data = {k: v for k, v in self.input_data.update(
                    self.ms_struct).items()}
        else:
            data = {k: v for k, v in self.input_data.update(
                    self.ms_struct).items()}
            
        return data
            

    
if __name__ == "__main__":

    pass
