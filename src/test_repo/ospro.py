"""

Module with processing functions for OSeMOSYS input and results data

Copyright (C) 2025 Leonhard Hofbauer, licensed under a MIT license

"""

import os
import yaml
import pathlib
import copy

import subprocess
import logging

import pandas as pd
import numpy as np

import otoole

try:
    import fratoo as ft
except ImportError:
    ft = None 
    
try:
    import highspy
except ImportError:
    highspy = None 


pd.set_option('future.no_silent_downcasting', True)

logger = logging.getLogger(__name__)



def read_spreadsheets(path,
                      scenario_list,
                      dcfg,
                      file_extensions = [".xlsx",".xls",".ods"],
                      read_recursively = False,
                      use_markers = False,
                      table_marker = None,
                      all_marker = "#ALL#",
                      set_defaults = {},
                      rounding = False):
    """ Read scenario data from spreadsheet files.
    
    Parameters
    ----------
    path : str
        Path to directory or spreadsheet file.
    file_extension: list of str, optional
        List of strings of file extensions that will be considered as input
        data files. The default is [".xlsx",".xls",".ods"].
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the same
        format as used for otoole).
    read_recursively: bool, optional
        If to consider files recursively in folders included in the provided
        path. The default is False.
    use_markers: bool, optional
        If to read spreadsheet files using markers for tables.
        The default is False.
    table_marker: str, optional
        String of table marker. This is required if use_markers is True.
        The default is None.
    set_defaults: dict, optional
        Dictionary with set names as keys and defaults as values.
        The default is {}.
    rounding: int, optional
        The number of digits parameter values are to be rounded to. False
        for no rounding. The default is False.
    all_marker: str, optional
        String of marker symbalizing all values within the applicable set.
        The default is '#ALL#'
    list_scenarios: list of dicts
        List of dictionaries describing each scenario (name, model, levers
        timehorizon).


    Returns
    -------
    data : dict of dicts
        Dictionary with a dictionary of DataFrames with parameters and sets for
        each scenario.

    """
    
    logging.info("Parsing data tables from spreadsheet files.")
    
    # load data spreadsheets
    if not os.path.exists(path):
        raise FileNotFoundError("Directory/file does not exist.")
    if os.path.isfile(path):
        if not path.endswith(tuple(file_extensions)):
            raise ValueError("File extension is not recognized.")                     
        files = [os.path.join(os.path.dirname(path),os.path.basename(path))]
    elif read_recursively is False:
        files = os.listdir(path)
        files = [os.path.join(path,f) for f in files if f.endswith(tuple(file_extensions))]
    else:
        files = []
        for root, dirs, fs in os.walk(path):
            files = files + [os.path.join(root, f) for f in fs]
        
    logging.info("Spreadsheet file(s) being parsed: "+", ".join(files))

    # # add interface-specific elements to config
    # cfg["TECHGROUP"] = {"type":"set",
    #                     "dtype":"str"}   
     
    sets = [k for k,v in dcfg.items() if v["type"]=="set"]
        
    # list of data tables
    dts = list()

    # import all spreadsheet files
    for f in files:
        # import file
        sf = pd.read_excel(f,
                           sheet_name=None,
                           header=None,
                           na_values=[""],
                           keep_default_na=False)
        
        # iterate through sheets
        for df in sf.values():
            
            if use_markers is False:
                dt = df
                # remove any additional rows
                if dt.iloc[:,0].isna().any():
                    li = dt.iloc[:,0].isna().idxmax()
                    dt = dt.loc[:li,:].iloc[:-1]
                
                # remove any additional columns
                if dt.iloc[0,:].isna().any():
                    lc = dt.iloc[0,:].isna().idxmax()
                    dt = dt.loc[:,:lc].iloc[:,:-1]   
                
                # set column names and reset index   
                dt.columns = dt.iloc[0]
                dt = dt[1:]
                dt = dt.reset_index(drop=True)
                
                # check if PARAMETER is used instead of SET column and rename
                se = [n for n,v in dcfg.items() if v["type"] == "set"]
                if "PARAMETER" in dt.columns and not dt.empty and dt.loc[0,"PARAMETER"] in se:
                    dt = dt.rename(columns={"PARAMETER":"SET"})
                
                # replace short parameter names with full names
                ns ={v["short_name"]:n for n,v in dcfg.items()
                     if "short_name" in v.keys()}
                dt = dt.replace(ns)
                
                # add to list of data tables
                dts.append(dt)
                
            else: 
                # get location of marker
                loc = np.where(df==table_marker)
                # skip to next sheet if no marker
                if loc[0].size == 0:
                    continue
                # iterate through each marker location
                for r,c in zip(loc[0],loc[1]):
                    dt = df.iloc[r:,c:]
                    
                    # remove any additional rows
                    if dt.iloc[:,0].isna().any():
                        li = dt.iloc[:,0].isna().idxmax()
                        dt = dt.loc[:li,:].iloc[:-1]
                    
                    # remove any additional columns
                    if dt.iloc[1,:].isna().any():
                        lc = dt.iloc[1,:].isna().idxmax()
                        dt = dt.loc[:,:lc].iloc[:,:-1]   
                    
                    # remove marker row
                    dt = dt.iloc[1:,:]
                    
                    # set column names and reset index   
                    dt.columns = dt.iloc[0]
                    dt = dt[1:]
                    dt = dt.reset_index(drop=True)
                    
                    # check if PARAMETER is used instead of SET column and rename
                    se = [n for n,v in dcfg.items() if v["type"] == "set"]
                    if "PARAMETER" in dt.columns and not dt.empty and dt.loc[0,"PARAMETER"] in se:
                        dt = dt.rename(columns={"PARAMETER":"SET"})
                    
                    # replace short parameter names with full names
                    ns ={v["short_name"]:n for n,v in dcfg.items()
                         if "short_name" in v.keys()}
                    dt = dt.replace(ns)
                    
                    # add to list of data tables
                    dts.append(dt)
                
    logging.info(f"{len(dts)} data tables read from the spreadsheet file(s).")
    
    # process data tables
    logging.info("Processing data tables.")

    # create dataframe for set and parameter data

        
    setsval = pd.concat([dt for dt in dts if "SET" in dt.columns])
    params = pd.concat([pd.DataFrame([],columns=sets+["VALUE"])]
                       +[dt for dt in dts if ("PARAMETER" in dt.columns)])

    # set default value for sets if provided and no value given in data files
    for k,v in set_defaults.items():
        if k in sets:
            params[k] = params[k].fillna(v)
 
    # create dict for scenario to write to spreadsheets
    md = dict()
    
    # iterate through scenarios and fill dictionary

    # for m,s,sc in modscen:
    for s in scenario_list:
        
            
        md[s["name"]] = dict()
        
        # iterate through parameters and sets
        for k,v in dcfg.items():
            
            # if set, process accordingly
            if v["type"] == "set":
                # get all relevant values
                md[s["name"]][k] = setsval.loc[((setsval["MODEL"] == s["model"]) |
                                       (setsval["MODEL"] == all_marker)) &
                                          ((setsval["SCENARIO"].str.split(",",expand=True).isin(s["levers"]).any(axis=1)) |
                                       (setsval["SCENARIO"] == all_marker)) &
                                       (setsval["SET"] == k)]
                md[s["name"]][k] =md[s["name"]][k].loc[:,[c for c in ["VALUE",
                                                       "DESCRIPTION",
                                                       "UNIT",
                                                       "TECHGROUP",
                                                       "UNITOFCAPACITY",
                                                       "UNITOFACTIVITY"]
                                           if c in md[s["name"]][k].columns]]
                
                if not md[s["name"]][k].empty:
                    md[s["name"]][k] = md[s["name"]][k].dropna(axis=1,how="all")
                    
                md[s["name"]][k] = md[s["name"]][k].sort_values("VALUE").reset_index(drop=True)
                
                # set dtype
                md[s["name"]][k].loc[:,"VALUE"] = md[s["name"]][k].loc[:,
                                                    "VALUE"].astype(v["dtype"])
                # if YEAR set, remove years not used
                if k == "YEAR":
                    md[s["name"]][k] = md[s["name"]][k].loc[md[
                                s["name"]][k]["VALUE"].isin(range(*s["timehorizon"]))]
                    
                # check if unique
                if not md[s["name"]][k]["VALUE"].is_unique:
                    md[s["name"]][k] = md[s["name"]][k].drop_duplicates(["VALUE"])
                    logging.warning(f"The values of set '{k}' are not unique "
                                    f"for scenario '{s['name']}' with model '{s['model']}'."
                                    " Deleted duplicates.")
                # check if empty
                if md[s["name"]][k].empty:
                    logging.debug(f"The set '{k}' is empty for scenario '{s['name']}'"
                                  f" with model '{s['model']}'.")
                    #md[s["name"]][k].loc["",:] = ""
            
                # rename value column for TECHGROUP
                if k == "TECHGROUP":
                    md[s["name"]][k] = md[s["name"]][k].rename(columns={"VALUE":"TECHGROUP"})
                    
            # if parameter, process accordingly
            if v["type"] == "param":
                # get all relevant values
                md[s["name"]][k] = params.loc[((params["MODEL"] == s["model"]) |
                                       (params["MODEL"] == all_marker)) &
                                         ((params["SCENARIO"].str.split(",",expand=True).isin(s["levers"]).any(axis=1)) |
                                       (params["SCENARIO"] == all_marker)) &
                                       (params["PARAMETER"] == k)]
                # drop irrelevant columns
                md[s["name"]][k] = md[
                        s["name"]][k].drop(["MODEL","PARAMETER"]
                                         + [c for c in md[s["name"]][k].columns
                                            if ("YEAR" in v["indices"])
                                            and c not in [s for s in v["indices"]
                                                          if s !="YEAR"]
                                            +list(range(*s["timehorizon"]))+["SCENARIO"]]
                                         + [c for c in md[s["name"]][k].columns
                                            if ("YEAR" not in v["indices"])
                                            and c not in (v["indices"]+["VALUE"]+["SCENARIO"])]
                                         ,axis=1)
                # set and sort index
                md[s["name"]][k] = md[
                        s["name"]][k].set_index(["SCENARIO"]+[s for s in v["indices"]
                                               if s != "YEAR"])
                md[s["name"]][k] = md[s["name"]][k].sort_index()
            
                
                # set dtypes
                md[s["name"]][k] = md[s["name"]][k].astype(v["dtype"])
                md[s["name"]][k] = md[s["name"]][k].replace("nan",pd.NA)
                if "YEAR" in v["indices"]:
                    md[s["name"]][k].columns = md[s["name"]][k].columns.astype(dcfg["YEAR"]
                                                               ["dtype"])
                
                # remove years not to be used
                if "YEAR" in v["indices"]:
                    md[s["name"]][k] = md[s["name"]][k].loc[:,md[
                                s["name"]][k].columns.isin(range(*s["timehorizon"]))]
                    
                    
                # if float round if required
                if (v["dtype"] == "float") and (rounding != False):
                    md[s["name"]][k] = md[s["name"]][k].round(rounding)
                    
                # rearrange params indexed over YEAR
                if "YEAR" in v["indices"]:
                    md[s["name"]][k].columns.name = "YEAR"
                    md[s["name"]][k] = md[s["name"]][k].stack()
                    md[s["name"]][k].name = "VALUE"
                    md[s["name"]][k] = md[s["name"]][k].to_frame()      
                
                # check for duplicates
                if (md[s["name"]][k].xs(all_marker,level="SCENARIO").index.has_duplicates
                    if all_marker in md[s["name"]][k].index.get_level_values("SCENARIO") else False  or
                    md[s["name"]][k].loc[ md[s["name"]][k].index.get_level_values("SCENARIO")!=all_marker].index.has_duplicates):

                    logging.error(f"The values of parameter '{k}' are not unique"
                                    f" for scenario '{s['name']}' with model '{s['model']}'.")
                    logging.debug("The duplicates are:")
                    logging.debug(md[s["name"]][k][md[s["name"]][k].index.duplicated(keep=False)])
                    raise ValueError(f"The values of parameter '{k}' are not "
                                    f"unique for scenario '{s['name']}' with model '{s['model']}'.")
                # sort so that rows defined for all scenarios are appearing first
                sorter = [all_marker] + [se for se in md[s["name"]][k].index.get_level_values("SCENARIO").unique() if se != all_marker]
                md[s["name"]][k] =  md[s["name"]][k].sort_values(by="SCENARIO",
                                                       key=lambda column: column.map(lambda e: sorter.index(e)))
                 
                # drop scenario level
                md[s["name"]][k] = md[s["name"]][k].droplevel("SCENARIO")
                
                # if duplicates, only use second/last values (specifically defined for
                # the scenario)
                md[s["name"]][k] = md[s["name"]][k].loc[~md[s["name"]][k].index.duplicated(keep="last")]

    
                if md[s["name"]][k].empty:
                    logging.debug(f"The values for parameter '{k}' are empty "
                                    f"for scenario '{s['name']}' with model '{s['model']}'.")
                    # md[s["name"]][k]= md[s["name"]][k].astype(str)
                    # md[s["name"]][k].loc["",:] = ""
                    
    logging.info("Processed data tables and arranged scenario data.")
    
    return md

def create_multiscale_model(data,
                            dcfg):
    """ Create multi-scale model for each scenario using the fratoo package.
    
    Parameters
    ----------
    data : dict
        Data dictionary with one or more scenarios.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    mod : dict of fratoo.Model
        Dictionary of instances of fratoo models, one for each scenario
        (names as keys).

    """
    if ft is None:
            logger.warning("Multi-scale functionality is not available as"
                           " the required dependency (fratoo)"
                           " is not available.")
            return False

    logging.info("Creating fratoo model for each scenario.")
    
    mod = dict()
    
    for s in data.keys():
        mod[s] = ft.Model()
        mod[s].init_from_dictionary(data[s], config=dcfg, process=False)
        mod[s].process_input_data(sep="9")
        
    logging.info("Created fratoo model(s).")
    
    return mod


def get_multiscale_run_data(mod,
                            regions,
                            region_sep,
                            dcfg):
    """ Create multi-scale model for each scenario using the fratoo package.
    
    Parameters
    ----------
    mod : dict of fratoo.Model
        Dictionary of instances of fratoo models, one for each scenario
        (names as keys).
    regions : List
        List of names of regions to be explicitely included in the run.
        Sublists can be used to aggregate regions.
    region_sep : str
        Region separator to applied when REGION set values are integrated into
        other sets' values, e.g., TECHNOLOGY.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    data : dict
        Data dictionary with one or more scenarios.
    dcfg : dict
        Updated configuration with fratoo parameters removed.
    

    """ 
    if ft is None:
            logger.warning("Multi-scale functionality is not available as"
                           " the required dependency (fratoo)"
                           " is not available.")
            return False

    logging.info("Creating run data for multiscale model.")
    
    data = dict()
    for s in mod.keys():
        rr = mod[s]._create_regions_for_run(regions,
                                            autoinclude=True,
                                            weights="SpecifiedAnnualDemand",
                                            syn=["","p"])
        data[s] = mod[s]._create_run_data(df_regions=rr,
                                          sep=region_sep,
                                          syn=["","p"],
                                          redset=False
                                          ,pyomo=False)
    
    logging.info("Created run data for multiscale model.")
    logging.info("Updating data configuration dictionary.")
    for k in list(dcfg.keys()):
        if k.startswith("ft_"):
            dcfg.pop(k)
            
    logging.info("Updated data configuration dictionary.")
    
    return data,dcfg


def rename_set(mapping,
               data,
               dcfg):
    """ Rename a set name.
    
    Parameters
    ----------
    mapping : dict
        Dictionary mapping old to new set names.
    data : dict
        Data dictionary with one or more scenarios.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).
    
    Returns
    -------
    data : dict of dicts
        Dictionary with a dictionary of DataFrames with parameters and set for
        each scenario.
    dcfg : dict
        Updated config dictionary.
    
    """   
    logging.info("Rename set names.")
    
    for s in data.keys():
        # iterate through parameters and sets
        for k,v in dcfg.items():
            # if param, process accordingly
            if v["type"] == "param":
                for se in mapping.keys():
                    if se in v["indices"]:
                        data[s][k].index = data[s][k].index.rename([s if s!=se
                                                                else mapping[se]
                                                                for s
                                                                in data[s][k].index.names])
        # adjust set name
        for se in mapping.keys():
            data[s][mapping[se]] = data[s].pop(se)
            

    # adjust config file
    for k,v in dcfg.items():
        if dcfg[k]["type"]=="param":
            dcfg[k]["indices"] =  [i if i not in mapping.keys() else mapping[i]
                                  for i in dcfg[k]["indices"]]
    for se in mapping.keys():
        dcfg[mapping[se]] = dcfg.pop(se)
    
    logging.info("Renamed set names")
    
    return data, dcfg
        
def check_data(data, dcfg):
    """ Check scenario data for issues.
    
    Parameters
    ----------
    data : dict
        Data dictionary with one or more scenarios.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    e : bool
        True if no issue was identified during checks, otherwise False.

    """
    # FIXME: implement more checks

    logging.info("Performing checks on data.")
    
    # check if all set values used to define parameters are in respective sets
    # iterate through models and scenarios
    
    e = True
    for s in data.keys():
        # iterate through parameters and sets
        for k,v in dcfg.items():
            # skip multi-scale parameters
            if k.startswith("ft"):
                continue
            # if param, check accordingly
            if v["type"] == "param":
                for ii in v["indices"]:
                    if ii == "YEAR":
                        continue
                    if not data[s][k].index.get_level_values(ii).isin(
                            data[s][ii]["VALUE"].tolist()+[""]).all():
                        # FIXME: raise exception (?)
                        undef = data[s][k][~data[s][k].index.get_level_values(ii).isin(
                            data[s][ii]["VALUE"].tolist())]
                        e=False
                        logging.error(f"The parameter '{k}' for scenario '{s}'"
                                    f" is defined for {ii} values"
                                    f" that are not part of the '{ii}' set. This"
                                    " can cause errors when running the"
                                    " model. The relevant entries are:"
                                    f"{undef}")

        lowerthan = {"ResidualCapacity":"TotalAnnualMaxCapacity",
                     "TotalAnnualMinCapacity":"TotalAnnualMaxCapacity",
                     "TotalTechnologyAnnualActivityLowerLimit":"TotalTechnologyAnnualActivityUpperLimit",
                     "TechnologyActivityByModeLowerLimit":"TechnologyActivityByModeUpperLimit",
                     "TotalAnnualMinCapacityInvestment":"TotalAnnualMaxCapacityInvestment"}
        for l,h in lowerthan.items():
        # check if TotalAnnualMinCapacity is lower than TotalAnnualMaxCapacity
            logging.debug(f"Checking contradictions between {l} and {h}.")
            if (data[s][l].reindex_like(
                data[s][h])>data[s][h]).any().any():
                vio = data[s][h][(data[s][l].reindex_like(
                    data[s][h])>data[s][h])].dropna(how="all").dropna(axis=1)
                e=False
                logging.error(f"{l} is higher than {h}"
                              f" in scenario {s},"
                              " violating the constraint: \n"
                              f"{vio}")
            
    logging.info("Performed checks on data.")
    
    return e

def write_datafile(data,
                   path,
                   dcfg,
                   fuel_rename=False
                   ):
    """ Write data to GNU MathProg datafile(s).
    
    Parameters
    ----------
    data : dict
        Data dictionary with one or more scenarios.
    path : str
        Path to directory for writing the datafiles.
    fuel_rename: bool, optional
        If to rename the FUEL set to COMMIDITY. False if not to update.
        The default is False.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    -

    """

    logging.info("Rearrange data for saving.")
    
    # create deepcopy
    data = _create_data_deepcopy(data)

    # if required rename FUEL set
    # rename FUEL set to COMMODITY (UI - at one point -  required FUEL 
    # for import but uses COMMODITY for export, so this can be used to 
    # make datafiles consistent with datafiles exported from UI)
    if fuel_rename:
        for s in data.keys():
            # iterate through parameters and sets
            for k,v in dcfg.items():
                # if param, process accordingly
                if v["type"] == "param":
                    if "FUEL" in v["indices"]:
                        data[s][k].index = data[s][k].index.rename([s if s!="FUEL"
                                                                else "COMMODITY"
                                                                for s
                                                                in data[s][k].index.names])
            # adjust set name
            data[s]["COMMODITY"] = data[s].pop("FUEL")
        
        # adjust config file
        for k,v in dcfg.items():
            if dcfg[k]["type"]=="param":
                dcfg[k]["indices"] =  [i if i!="FUEL" else "COMMODITY" 
                                      for i in dcfg[k]["indices"]]
        dcfg["COMMODITY"] = dcfg.pop("FUEL")

        
    # rearrange data for otoole and save a datafile for each model and scenario
    for s in data.keys():
        for k,v in dcfg.items():
            if k.startswith("ft_"):
                continue
            if v["type"] == "result":
                continue
            if v["type"] == "set":
                data[s][k] = data[s][k].loc[:,"VALUE"].to_frame()
            if k == "TECHGROUP":
                del data[s][k]
                continue
            if data[s][k].empty:
                data[s][k]= data[s][k].astype(str)
                data[s][k].loc["",:] = ""
                
            data[s][k] = data[s][k][data[s][k]["VALUE"]!=""].dropna()
        defaults = {k:v["default"] for k,v in dcfg.items() if v["type"]!="set"}
        
        logging.info(f"Writing data for scenario {s} to data file.")
        
        # FIXME: delete, previous version
        # from otoole.convert import _get_user_config, _get_write_strategy
        # write_strategy = _get_write_strategy(
        #           dcfg, "datafile", #write_defaults=False
        #           )
        # write_strategy.write(data[s], os.path.join(path,"datafile_"+s+".txt"))

        ndcfg = copy.deepcopy(dcfg)
        for k in list(ndcfg.keys()):
            if ndcfg[k]["type"] == "result":
                del ndcfg[k]
        with open('data_config_temp.yaml', 'w') as outfile:
            yaml.dump(ndcfg, outfile, default_flow_style=False)

        otoole.write("./data_config_temp.yaml",
                     "datafile",os.path.join(path,"datafile_"+s+".txt"),
                      data[s],default_values=defaults)
        pathlib.Path.unlink(pathlib.Path("./data_config_temp.yaml"))
        
    return

def write_spreadsheet(data,
                      path,
                      dcfg,
                      fuel_rename=False):
    """ Write data to spreadsheet file(s).
    
    Parameters
    ----------
    data : dict
        Data dictionary with one or more scenarios.
    path : str
        Path to directory for writing the spreadsheet files.
    fuel_rename: bool, optional
        If to rename the FUEL set to COMMIDITY. False if not to update.
        The default is False.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    -

    """
    logging.info("Rearrange data for saving.")
    
    # create deepcopy
    data = _create_data_deepcopy(data)
    
    # if required rename FUEL set
    if fuel_rename:
        for s in data.keys():
            # iterate through parameters and sets
            for k,v in dcfg.items():
                # if param, process accordingly
                if v["type"] == "param":
                    if "FUEL" in v["indices"]:
                        data[s][k].index = data[s][k].index.rename([s if s!="FUEL"
                                                                else "COMMODITY"
                                                                for s
                                                                in data[s][k].index.names])
            # adjust set name
            data[s]["COMMODITY"] = data[s].pop("FUEL")
        
        # adjust config file
        for k,v in dcfg.items():
            if dcfg[k]["type"]=="param":
                dcfg[k]["indices"] =  [i if i!="FUEL" else "COMMODITY" 
                                      for i in dcfg[k]["indices"]]
        dcfg["COMMODITY"] = dcfg.pop("FUEL")
        
        
    # rearrange data and save a spreadsheet file for each model and scenario
    for s in data.keys():
        for k,v in dcfg.items():
            
            # rearrange params indexed over YEAR in line with UI input files
            if ((v["type"] == "param")
                and "YEAR" in v["indices"]
                and (len(v["indices"])>2)):
                data[s][k] = data[s][k].unstack("YEAR").droplevel(level=0,axis=1)
                
   
         
        logging.info(f"Writing data for scenario {s} to spreadsheet file.")
        

        with pd.ExcelWriter(os.path.join(path,"input_data_"+s+".xlsx")) as writer:
            # iterate through parameters and sets
            for k,v in dcfg.items():
                if k not in data[s].keys():
                    logging.warning(f"Data for parameter {k} for scenario {s} "
                                    "are not available and, thus, not saved.")
                    continue
                if "short_name" in v.keys():
                    n = v["short_name"]
                else:
                    n = k   
                if v["type"]=="set":
                    data[s][k].to_excel(writer, sheet_name=n,
                                        merge_cells=False,index=False)
                else:
                    data[s][k].to_excel(writer,
                                        merge_cells=False,
                                        sheet_name=n)
    return


def write_csv(data,
              path,
              dcfg,
              fuel_rename=False):
    """ Write data to csv files.
    
    Parameters
    ----------
    data : dict
        Data dictionary with one or more scenarios.
    path : str
        Path to directory for writing the csv files.
    fuel_rename : bool, optional
        If to rename the FUEL set to COMMIDITY. False if not to update.
        The default is False.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    -

    """
    logging.info("Rearrange data for saving.")
    
    # create deepcopy
    data = _create_data_deepcopy(data)
    
    # if required rename FUEL set
    if fuel_rename:
        for s in data.keys():
            # iterate through parameters and sets
            for k,v in dcfg.items():
                # if param, process accordingly
                if v["type"] == "param":
                    if "FUEL" in v["indices"]:
                        data[s][k].index = data[s][k].index.rename([s if s!="FUEL"
                                                                else "COMMODITY"
                                                                for s
                                                                in data[s][k].index.names])
            # adjust set name
            data[s]["COMMODITY"] = data[s].pop("FUEL")
        
        # adjust config file
        for k,v in dcfg.items():
            if dcfg[k]["type"]=="param":
                dcfg[k]["indices"] =  [i if i!="FUEL" else "COMMODITY" 
                                      for i in dcfg[k]["indices"]]
        dcfg["COMMODITY"] = dcfg.pop("FUEL")
        
        
    # rearrange data and save a spreadsheet file for each model and scenario
    for s in data.keys():
        logging.info(f"Writing data for scenario {s} to csv files.")
        
        if not os.path.exists(os.path.join(path,s)):
                os.makedirs(os.path.join(path,s))

        # iterate through parameters and sets
        for k,v in dcfg.items():
            if k not in data[s].keys():
                logging.warning(f"Data for parameter {k} for scenario {s} "
                                "are not available and, thus, not saved.")
                continue
            if False: #"short_name" in v.keys():
                n = v["short_name"]
            else:
                n = k   
            if v["type"]=="set":
                data[s][k].to_csv(os.path.join(path,s,n+".csv"),index=False)
            else:
                data[s][k].to_csv(os.path.join(path,s,n+".csv"))
    return

def run_model(data,
              results_path,
              scenario_list,
              dcfg,
              model_file_path,
              config_path,
              glpk_dir=None,
              fuel_rename=False,
              solver="highs",
              solver_cwd = "./"
              ):
    """ Run the model.
    
    Parameters
    ----------
    data : dict,str
        Data dictionary with one or more scenarios.
        Path to directory with model data files to run (all .txt files are
        are assumed to be datafiles), or filepath for single datafile (not yet
        implemented).
    results_path : str
        Path to directory to save results in.
    scenario_list: list
        List of scenario names (str).
    model_file_path : str
        File path to GNU Mathprog model file.
    config_path : str
        File path to data config.
    glpk_dir : str, optional
        Path to folder with GLPK executable. If "None" system installation is used.
        The default is "None".
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).
    fuel_rename : bool, optional
        If to rename the FUEL set to COMMIDITY. False if not to update.
        The default is False.
    solver : str, optional
        String with solver name. Currently cbc and highs are implemented.
        The default is 'highs'.
    solver_cwd : str, optional
        Path to working directory for solver. The defaults is './'.

    Returns
    -------
    data : dict of dicts
        Dictionary with a dictionary of DataFrames with parameters and set for
        each scenario.

    """


        
    if not os.path.exists(results_path):
        os.makedirs(results_path)
      
    # FIXME: could add option to run scenarios from existing datafiles
    # if isinstance(data,str):
    # # get data files path
    #     if not os.path.exists(data):
    #         raise FileNotFoundError("Directory/file does not exist.")
    #     if os.path.isfile(data):
    #         if not data.endswith(".txt"):
    #             raise ValueError("File extension is not recognized.")                     
    #         data = [os.path.basename(data)]
    #         path = os.path.dirname(data)
    #     else:
    #         data = os.listdir(data)
    #         data = [f for f in data if f.endswith(".txt")]

    solution = dict()
    
    for s in scenario_list:
        
        logging.info(f"Running model for scenario {s}.")
        
        if s not in data.keys():
            logging.warning(f"Data for scenario {s} are not provided and, "
                            "thus, the scenario cannot be run.")
            continue
        
        
        filep = (results_path +"/"
                 +s)
        
        if not os.path.exists(filep+"/csv/"):
            os.makedirs(filep+"/csv/")
            
        write_datafile({s:data[s]}, filep, dcfg, fuel_rename=fuel_rename)
        
        glpk_exe = "glpsol" if glpk_dir == "None" else glpk_dir+"glpsol"
        # FIXME: this can be extended to allow for the use of other solvers
        if solver == "cbc":
            subprocess.run([glpk_exe,
                            "-m", model_file_path,
                            "-d", filep+"/"+"datafile_"+s+".txt",
                            "--wlp","opt.lp",
                            "--check"],
                            cwd=solver_cwd
                            )
            subprocess.run(["cbc","opt.lp","solve","-solu","solution.sol",
                            #"-cross","off",
                            "-dualB","1.0e5",
                            "-dualT","1.0e-5",
                            "-primalT","1.0e-5"
                            ],
                            cwd=solver_cwd
                            )
            
            
            otoole.convert_results(config_path, "cbc", "csv",
                                   solver_cwd+"solution.sol",
                                   filep+"/"+"csv",
                                   "datafile",
                                   filep+"/"+"datafile_"+s+".txt")
        elif solver == "highs":
            
            if highspy is None:
                logging.warning("The highs solver is not installed. The model"
                             " will not be solved. Install highs or choose a"
                             " different solver.")
                return False
            
            subprocess.run([glpk_exe,
                            "-m", model_file_path,
                            "-d", filep+"/"+"datafile_"+s+".txt",
                            "--wlp","opt.lp",
                            "--check"],
                            cwd=solver_cwd
                            )
            
            h = highspy.Highs()
            filename = solver_cwd+"opt.lp"
            h.readModel(filename)
            h.run()
            # h.writeSolution(pcfg["run"]["solver_cwd"]+"solution.sol",1)
            sol = pd.DataFrame(zip(h.allVariableNames(),h.getSolution().col_value),
                               columns=["VARINDEX","VALUE"])
            del h
            
            # create separate column with variable name
            sol["VARIABLE"] = (
                sol["VARINDEX"]
                .astype(str)
                .str.split("(",expand=True)[0]
            )
            # create index column
            sol["INDEX"] = (
                sol["VARINDEX"]
                .astype(str)
                .str.split("(",expand=True)[1]
                .str.replace(r"\)|'", "",regex=True)
            )
            # arrange values for each variable
            solution[s] = dict()
            for v in sol["VARIABLE"].unique():
                df = sol.loc[sol["VARIABLE"]==v,["INDEX","VALUE"]]
                if df.empty:
                    continue
                df.index = pd.MultiIndex.from_tuples(df["INDEX"].str.split(","))
                df = df.drop(columns=["INDEX"])
                
                # update index level names
                if v in dcfg.keys():
                    df.index.names = dcfg[v]["indices"]

                # recast dtypes
                idx = df.index
                for i in idx.names:
                    if i in dcfg.keys():
                        df = df.reset_index()
                        df.loc[:,i] = df.loc[:,i].astype(dcfg[i]["dtype"])
                        df = df.set_index(idx.names)

                solution[s][v] = df

            # FIXME: currently not working, improved version above, kept
            # to potentially adapt way to extract duals into above implem.
   
            # df = pd.read_csv(
            #     pcfg["run"]["solver_cwd"]+"solution.sol",
            #     header=None,
            #     sep='(',
            #     #sep="(",
            #     names=["valuevar","index"],
            #     skiprows=2,
            # )  # type: pd.DataFrame
            
            # df = df.dropna()
   
            # df["VARIABLE"] = (
            #     df["valuevar"]
            #     .astype(str)
            #     .str.replace(r"^\*\*", "", regex=True)
            #     .str.split(expand=True)[6]
            # )
            # df["VALUE"] = (
            #     df["valuevar"]
            #     .astype(str)
            #     .str.replace(r"^\*\*", "", regex=True)
            #     .str.split(expand=True)[4]
            # )
            # df["DUAL"] = (
            #     df["valuevar"]
            #     .astype(str)
            #     .str.replace(r"^\*\*", "", regex=True)
            #     .str.split(expand=True)[5]
            # )
            # df["INDEX"] = (
            #     df["index"]
            #     .astype(str)
            #     .str.replace(r"\)|'", "",regex=True)
            # )
            # df = df.drop(columns=["valuevar","index"])
            
            
            # var = ["RateOfActivity","NewCapacity","TotalCapacityAnnual",
            #        "Demand.csv","AnnualEmissions"]
            # dual = ["RateOfActivity","NewCapacity","TotalCapacityAnnual",
            #        "Demand.csv","AnnualEmissions",
            #        "EBb4_EnergyBalanceEachYear4_ICR",
            #        "EBa10_EnergyBalanceEachTS4",
            #        "EBa11_EnergyBalanceEachTS5",
            #        "EBa9_EnergyBalanceEachTS3",
            #        "EBb4_EnergyBalanceEachYear4"
            #        ]
            # # with open(CFP_c, "r") as file:
            # #     cfg = yaml.safe_load(file)
                
            # for v in var:
            #     d = df.loc[df["VARIABLE"]==v,["INDEX","VALUE"]]
            #     if d.empty:
            #         continue
            #     d.index = pd.MultiIndex.from_tuples(d["INDEX"].str.split(","))
            #     d = d.drop(columns=["INDEX"])
                
            #     if v in dcfg.keys():
            #         d.index.names = dcfg[v]["indices"]
                    
            #     d.to_csv(filep+"/"+"csv/"+v+".csv")
                
            # for v in dual:
            #     d = df.loc[df["VARIABLE"]==v,["INDEX","DUAL"]]
            #     if d.empty:
            #         continue
            #     d = d.rename(columns={"DUAL":"VALUE"})
            #     d.index = pd.MultiIndex.from_tuples(d["INDEX"].str.split(","))
            #     d = d.drop(columns=["INDEX"])
                
            #     if v in dcfg.keys():
            #         d.index.names = dcfg[v]["indices"]
            #     # FIXME: currently causing issues, this needs fixing    
            #     # d.to_csv(filep+"/"+"csv/"+v+"_dual.csv")
            #     # df.to_csv(filep+"/"+"csv/"+"all_dual.csv")

        logging.info(f"Solved model for scenario {s}.")   

    # add variable values to model data
    for s in list(data.keys()):
        if s not in solution.keys():
            data.pop(s)
        else:
            for v in solution[s].keys():
                data[s][v] = solution[s][v]
                
    logging.info("Completed process.")
    
    return data

def save_results(results,
                 results_path,
                 scenario_list,
                 dcfg,
                 parameter_list = None,
                 file_format = "csv",
                 ):
    """ Save run results.
    
    Parameters
    ----------
    results : dict of dicts
        Dictionary with a dictionary of DataFrames with results for
        each scenario.
    results_path : str
        Path to directory to save results in.
    scenario_list : list
        List of scenario names to be saved.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).
    parameter_list : list, optional
        List of parameter names to be saved. All are saved if None. The default
        is None.
    file_format : str, optional
        Format the results are to be saved in. Either 'csv' or 'xlsx'. The
        default is 'csv'.

    Returns
    -------
    None

    """
    
    logging.info("Saving results.")
    
    
    for s in scenario_list:
        
        if file_format == "csv":
            filep = (results_path +"/"
                     +s+"/csv/")
        else:
            filep = (results_path +"/"
                     +s+"/")
        
        
        if not os.path.exists(filep):
            os.makedirs(filep)
            
        if file_format == "csv":   
            for k,df in results[s].items():
                if df.empty:
                    continue
                if parameter_list is not None and k not in parameter_list:
                    continue
                df.to_csv(filep+k+".csv")
        else:
            with pd.ExcelWriter(filep+"Results.xlsx",
                                engine='openpyxl') as writer:
                for k,df in results[s].items():
                    if df.empty:
                        continue
                    if parameter_list is not None and k not in parameter_list:
                        continue
                    df.to_excel(writer,
                                merge_cells=False,
                                sheet_name=k)
                
    logging.info("Saved results.") 
       
    return
     

def load_results(results_path,
                 scenario_list,
                 dcfg,
                 data=None):
    """ Load run results.
    
    Parameters
    ----------

    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).
    data : dict, optional
        Model input data that will be added to results data if provided.

    Returns
    -------
    data : dict of dicts
        Dictionary with a dictionary of DataFrames with results for
        each scenario.

    """
    
    logging.info("Loading results.")
    
    results = dict()
    
    for s in scenario_list:
        
        results[s] = dict()
        
        filep = (results_path+"/"
                 +s)
        
        files = os.listdir(filep+"/csv/")
        
        for f in files:
            df = pd.read_csv(filep+"/csv/"+f,
                             keep_default_na=False)
            df = df.rename(columns={"r":"REGION",
                                    "t":"TECHNOLOGY",
                                    "y":"YEAR",
                                    "e":"EMISSION",
                                    "l" :"TIMESLICE",
                                    "m":"MODE_OF_OPERATION"})
            #df.columns = [c for c in df.columns if c!="VALUE"]+["VALUE"]
            
            # strip ' in all str columns
            for c in df.columns:
                if df[c].dtype == "object":
                    df[c] = df[c].str.replace(r"\)|'", "",regex=True)
                    
            df = df.set_index([c for c in df.columns if c != "VALUE"])
            results[s][f.split(".")[0]] = df.copy()
            
        if data is not None:
            for k,v in data[s].items():
                results[s][k] = v.copy()
                
    logging.info("Loaded results.") 
       
    return results

def demap_multiscale_results(data,
                             region_sep,
                             dcfg):
    """ Rearrange multiscale results.
    
    Parameters
    ----------
    results : dict,
        Data dictionary with results data of one or more scenarios.
    region_sep : str
        Region separator to applied when REGION set values are integrated into
        other sets' values, e.g., TECHNOLOGY.
    dcfg : dict
        Dictionary that includes configuration of OSeMOSYS data (in the format
        used for otoole).

    Returns
    -------
    results : dict of dicts
        Data dictionary with processed results data of one or more scenarios.

    """
    
    if ft is None:
            logger.warning("Multi-scale functionality is not available as"
                           " the required dependency (fratoo)"
                           " is not available.")
            return False

    
    logging.info("Processing results for each scenario.")
    
    results = dict()
    mod = ft.Model()
    
    for s in data.keys():
        results[s] = mod._demap(data[s],sep=region_sep)
        
    logging.info("Created fratoo model(s).")
    
    return results



    
def check_results(results,
                  backstop = None):
    """ Check results to generate warning if issues detected.
    
    Parameters
    ----------
    results : dict,
        Data dictionary with results data of one or more scenarios.
    backstop : list, optional
        List of backstop technology names (str), which if detected will cause
        a warning. No check is performed if None is provided. The default is
        None.

    Returns
    -------
    e : bool
        If an issue has been identified.

    """

    logging.info("Performing checks on results")
    e = True
    if backstop != None:
        logging.info("Performing check on backstop usage.")
        
        for s, data in results.items():
            prod_annual = data.get("ProductionByTechnologyAnnual")
            if prod_annual is None:
                logging.warning(f"Could not perform check for scenario {s} as"
                                " the relevant parameter is not available.")
                continue
            
            techs = prod_annual.index.get_level_values("TECHNOLOGY")
            for backstop in ["BACKSTOP", "BACKSTOPUCK", "BACKSTOPRCK"]:
                if backstop in techs and prod_annual.loc[techs == backstop,
                                                         "VALUE"].sum() > 0:
                    logging.warning(f"Backstop technology '{backstop}' is"
                                    f" active in scenario '{s}'.")
                    e = False
                    
    logging.info("Checks on results completed.")
    
    return e

    
def expand_results(results):
    """ Adding additional results variables.
    
    Parameters
    ----------
    results : dict,
        Data dictionary with results data of one or more scenarios.

    Returns
    -------
    data : dict of dicts
        Dictionary with a dictionary of DataFrames with parameters and set for
        each scenario.

    """
    logging.info("Expanding results.")
    
    for s in results.keys():
        
        results[s]["ProductionByTechnologyAnnual"] = (
            results[s]["RateOfActivity"]
            *results[s]["OutputActivityRatio"]
            *results[s]["YearSplit"]).dropna().groupby(["REGION","TECHNOLOGY","FUEL","YEAR"]).sum()
        
        results[s]["TotalProductionByTechnologyAnnual"] = (
            results[s]["RateOfActivity"]
            *results[s]["OutputActivityRatio"]
            *results[s]["YearSplit"]).dropna().groupby(["REGION","TECHNOLOGY","YEAR"]).sum()
        
        results[s]["UseByTechnologyAnnual"] = (
            results[s]["RateOfActivity"]
            *results[s]["InputActivityRatio"]
            *results[s]["YearSplit"]).dropna().groupby(["REGION","TECHNOLOGY","FUEL","YEAR"]).sum()
        
        results[s]["AnnualEmissions"] = (
            results[s]["RateOfActivity"]
            *results[s]["EmissionActivityRatio"]
            *results[s]["YearSplit"]).dropna().groupby(["REGION","EMISSION","YEAR"]).sum()
        
        
        results[s]["CostInvestment"] = (results[s]["NewCapacity"]
                                           *results[s]["CapitalCost"]).dropna()
        # FIXME: this is just a simple estimate
        results[s]["CostCapital"] = (results[s]["TotalCapacityAnnual"]
                                           *results[s]["CapitalCost"]
                                           /results[s]["OperationalLife"]).dropna()
        results[s]["DiscountFactor"] = pd.concat([(1 + results[s]["DiscountRate"]["VALUE"]).pow(y - results[s]["YEAR"]["VALUE"].min() + 0.0) #.squeeze()
                                                     for y in results[s]["YEAR"]["VALUE"]],
                                               keys=results[s]["YEAR"]["VALUE"],
                                               names=["YEAR"]
                                                    ).to_frame()
        
        # results[s]["EBb4_EnergyBalanceEachYear4_ICR_dual"].index.names = ["REGION","FUEL","YEAR"]

        # results[s]["MarginalCost"] = (results[s]["EBb4_EnergyBalanceEachYear4_ICR_dual"]
        #                                    *results[s]["DiscountFactor"]).dropna()
        
        # results[s]["MarginalCost"].loc[results[s]["MarginalCost"]["VALUE"]>1000,
        #                                   "VALUE"] = pd.NA
        # results[s]["MarginalCost"] = results[s]["MarginalCost"].interpolate()
        
        # results[s]["CostperFuel"] = results[s]["MarginalCost"].copy()
        
    logging.info("Expanded results.")
    
    return results


         
def _create_data_deepcopy(data):
    
    d = dict()
    for s in data.keys():
        d[s] = dict()
        for k,v in data[s].items():
            d[s][k] = v.copy(deep=True)
            
    return d

