

import os
import logging
import yaml

import pandas as pd

# the ospro and fratoo modules need to currently be copied into the folder
# that also includes this script – these will be packaged in future
import ospro as op

import graphing_library as gl
import graphing_library_cooking as glc

logger = logging.getLogger(__name__)

def run_model(dataconfig_file,
              input_path,
              model_file_path,
              scenario_list,
              spatial_config,
              output_path,
              glpk_dir = None,
              rename_set = None,
              agg_years = None,
              agg_config = None,
              agg_timeslices = None,
              solve = "optimize",
              overwrite = False):

    
    #%% Check if results already exist
    
    if os.path.exists(output_path):
        if not overwrite:
            logger.info("Results folder already exists and will not be"
                         " overwritten. Set 'overwrite' to True to rerun"
                         " the model.")
            return
        
    #%% Load config file
            
    # load data config file
    with open(dataconfig_file) as s:    
        try:
            dcfg = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logger.error(exc)  
        
#%% Load CORE-WESM data and process data
    
    # load model data
    data = op.read_spreadsheets(path = input_path,
                                scenario_list = scenario_list,
                                all_marker = "#ALL",
                                dcfg = dcfg)
    # rename set
    if rename_set is not None:
        data, dcfg = op.rename_set(mapping=rename_set,
                                   data=data,
                                   dcfg=dcfg)


    ### Aggregate years
    if agg_years is not None:
        ysa = pd.read_csv(agg_years,index_col="VALUE")["AGG"]
        tap = pd.read_csv(agg_config,index_col="PARAM")["VALUE"]
        
        for s in data.keys():
            for n,df in data[s].items():
                if "YEAR" in df.index.names:
                    df = df.rename(index=ysa.to_dict())
                    if n not in tap.index:
                        logger.info(f"Aggregation method not defined for {n}, assuming sum.")
                        df = df.groupby(df.index.names).sum()
                    elif tap.loc[n] == "eq":
                        df = df.groupby(df.index.names).mean()
                    else:
                        df = df.groupby(df.index.names).sum()
                elif n=="YEAR":
                    df = pd.DataFrame(ysa.unique(),columns=["VALUE"])
                
                data[s][n] = df
        
    ### Aggregate timeslices
    if agg_timeslices is not None:
        tsa = pd.read_csv(agg_timeslices,
                          index_col="VALUE")["AGG"]
        
        for s in data.keys():
            for n,df in data[s].items():
                if "TIMESLICE" in df.index.names:
                    df = df.rename(index=tsa.to_dict())
                    
                    if n=="CapacityFactor":
                        df = df.groupby(df.index.names).mean()
                    else:
                        df = df.groupby(df.index.names).sum()
                elif n=="TIMESLICE":
                    df = pd.DataFrame(tsa.unique(),columns=["VALUE"])
                
                data[s][n] = df


#%% Process to multi-scale fratoo model and get run data
    mod = op.create_multiscale_model(data,
                                     dcfg)


#%% Get run data and run optimization

    # get list of counties
    cs = [c for c in mod[scenario_list[0]["name"]].ms_struct["ft_scale"].index.to_list() if c!="RE1"]
    if isinstance(spatial_config,list):
        runs = spatial_config
    
    elif spatial_config == "full":
        runs = cs
        
    elif spatial_config == "full-sep":
        runs = [[c] for c in cs]
    
    elif spatial_config == "full-agg":
        runs = [cs]
    

    # if single optimization, get run data and run
    if (isinstance(spatial_config,list)
        or "sep" not in spatial_config):
        data,dcfg = op.get_multiscale_run_data(mod=mod,
                                               regions = runs,
                                               region_sep = "9",
                                               dcfg=dcfg)
        # perform checks on model data
        op.check_data(data=data,
                      dcfg=dcfg)
        
        # write datafile if to be used with OSeMOSYS cloud, etc. 
        
        # op.write_datafile(data, "./", pcfg, dcfg)
        # op.write_spreadsheet(data, "./", pcfg, dcfg)
        if solve == "datafile":
            # FIXME: check, does currently not work
            op.write_datafile(data = data,
                              path = "./",
                              dcfg = dcfg)
    
        elif solve == "spreadsheet":
            op.write_spreadsheet(data = data,
                                 path = "./",
                                 dcfg = dcfg)
            
        elif solve == "csv":
            op.write_csv(data = data,
                         path = "./",
                         dcfg = dcfg)


    #%% test tz-osemosys run
    
    # # clean data
    # # FIXME: needs proper addressing elsewhere?
    # data["Reference"]["SpecifiedDemandProfile"] = data["Reference"]["SpecifiedDemandProfile"].loc[data["Reference"]["SpecifiedDemandProfile"].index.get_level_values("FUEL").isin(data["Reference"]["SpecifiedAnnualDemand"].index.get_level_values("FUEL").unique())]
    # data["Reference"]["ReserveMarginTagTechnology"].loc[:,"VALUE"] = data["Reference"]["ReserveMarginTagTechnology"].loc[:,"VALUE"].astype(int)
    # data["Reference"]["ReserveMarginTagFuel"].loc[:,"VALUE"] = data["Reference"]["ReserveMarginTagFuel"].loc[:,"VALUE"].astype(int)
    
    
    # from tz.osemosys.schemas.time_definition import TimeDefinition
    # TimeDefinition(**basic_time_definition)
    # basic_time_definition = dict(
    #     id="Reference",
    #     years=range(2019, 2051),
    #     timeslices=['S11', 'S12', 'S14', 'S15'],
    #     year_split={'S11': 0.2499, 'S12': 0.3748, 'S14': 0.1667, 'S15': 0.2084},
    #     adj={
    #         "years": dict(zip(range(2019, 2050), range(2020, 2051))),
    #         "timeslices": dict(zip(['S11', 'S12', 'S14'], ['S12', 'S14', 'S15'])),
    #     },
    # )
    
    # TimeDefinition(**basic_time_definition)
    # model.time_definition = TimeDefinition(**basic_time_definition)
    
    
    # # copy in time sets and params - currently manually
    # # remove artificial storage set elements - DONE
    # # yearsplit constant across years - FINE?
    # # reserve margin issue (some techs listed don't exist, potentially?) - FINE?
    # # fuels not produced by any tech (BA9ELC003,NA9ELC003) - FINE?
    # # emissions not caused by any tech (NA9CO2com,NA9CO2ind,NA9CO2tra,...)  – currently manually
    # # no demand and not input (but output of a tech) (NA9COA,NA9ELC001,NA9GEO, ...) - FINE?
    
    # from tz.osemosys import Model
    # op.write_csv(data, "./test_csvs", pcfg, dcfg)
    # path_to_csvs = "./Reference_running"
    # path_to_csvs = "./test_csvs/Reference"
    # model = Model.from_otoole_csv(root_dir=path_to_csvs)
    
    # model.solve(solver="highs")


#%%
        # run model
        elif solve == "optimize":
            # run single optimization
            res = op.run_model(data = data,
                               model_file_path=model_file_path,
                               config_path=dataconfig_file,
                               glpk_dir = glpk_dir,
                               results_path = output_path,
                               scenario_list = [v["name"] for v in scenario_list],
                               dcfg = dcfg)
        
        ### Process and save results
        
        # res = op.load_results(pcfg, dcfg, data)
        
            res = op.expand_results(res)
            
            res = op.demap_multiscale_results(data = res,
                                              region_sep = "9",
                                              dcfg = dcfg)
    
    
    elif "sep" in spatial_config:

    
        res_list = list()
        
        for i,run in enumerate(runs):
            
            data,dcfg = op.get_multiscale_run_data(mod=mod,
                                                   regions = run,
                                                   region_sep = "9",
                                                   dcfg=dcfg)
            # perform checks on data
            op.check_data(data=data,
                          dcfg=dcfg)
            
            # write datafile if to be used with OSeMOSYS cloud, etc. 
            
            # op.write_datafile(data, "./", pcfg, dcfg)
            # op.write_spreadsheet(data, "./", pcfg, dcfg)
            if solve == "datafile":
                # FIXME: check, does currently not work
                op.write_datafile(data = data,
                                  path = "./",
                                  dcfg = dcfg)
        
            elif solve == "spreadsheet":
                op.write_spreadsheet(data = data,
                                     path = "./",
                                     dcfg = dcfg)
                
            elif solve == "csv":
                op.write_csv(data = data,
                             path = "./",
                             dcfg = dcfg)
                
            elif solve == "optimize":    
                res = op.run_model(data = data,
                                   model_file_path=model_file_path,
                                   config_path=dataconfig_file,
                                   glpk_dir = glpk_dir,
                                   results_path = output_path,
                                   scenario_list = [v["name"] for v in scenario_list],
                                   dcfg = dcfg)
                # load and expand results
                res_list.append(res)
    
            if solve == "optimize": 
                # aggregate results
                s = scenario_list[0]
                result = res_list[0][s]
                
                for k,v in result.items():
                    v = pd.concat([r[s][k] for r in res_list],
                                  axis=0,join="inner")
                    
                    if k not in mod[s].ms_struct["ft_param_agg"].index:
                        if k.isupper():
                            logger.warning(f"Aggregation method for result component '{k}' "
                                           "not defined. Assuming 'merge'")
                            result[k] = v.drop_duplicates()
                        else:    
                             logger.warning(f"Aggregation method for result component '{k}' "
                                            "not defined. Assuming 'sum'.")
                             result[k] = v.groupby(level=[i for i in
                                                   range(v.index.nlevels)]).sum()
                
                    elif  mod[s].ms_struct["ft_param_agg"].loc[k, "VALUE"] == "sum":
                        result[k] = v.groupby(level=[i for i in
                                              range(v.index.nlevels)]).sum()
                    elif  mod[s].ms_struct["ft_param_agg"].loc[k, "VALUE"] == "eq":
                        result[k] = v.groupby(level=[i for i in
                                              range(v.index.nlevels)]).mean()
                    else:
                        raise ValueError("The aggregation method for parameter"+
                                             " values specified in ft_param_agg"+
                                             " is not implemented in fratoo.")
                
                res = {s:result}
            
            
                res = op.expand_results(res)
            
                res = op.demap_multiscale_results(data = res,
                                                  region_sep = "9",
                                                  dcfg = dcfg)
    # save results
    op.save_results(results = res, 
                    results_path = output_path,
                    scenario_list = [v["name"] for v in scenario_list],
                    dcfg = dcfg)


def plot_national(input_path,
                  dataconfig_file,
                  years_agg_file,
                  scenario_list,
                  naming=None):
            
    # load data config file
    with open(dataconfig_file) as s:    
        try:
            dcfg = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logger.error(exc) 
    
    # load results
    res = op.load_results(results_path=input_path,
                          scenario_list = scenario_list,
                          dcfg=dcfg)

    if naming is not None:
        gcfg = pd.read_csv(naming)
        naming = gcfg.set_index("Name")["Description"]
        col = gcfg.set_index("Description")["Colour"]
    
    

    glc.plot_national_overview(results=res,
                               parameter = "TotalProductionByTechnologyAnnual",
                               scenarios = scenario_list,
                               dcfg = dcfg,
                               naming=naming,
                               col=col,
                               agg_years = years_agg_file,
                               )
    
def plot_counties(input_path,
                  dataconfig_file,
                  years_agg_file,
                  scenario_list,
                  counties,
                  list_counties,
                  naming=None):
            
    # load data config file
    with open(dataconfig_file) as s:    
        try:
            dcfg = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logger.error(exc) 
    
    # load results
    res = op.load_results(results_path=input_path,
                          scenario_list = scenario_list,
                          dcfg=dcfg)

    if naming is not None:
        gcfg = pd.read_csv(naming)
        naming = gcfg.set_index("Name")["Description"]
        col = gcfg.set_index("Description")["Colour"]
    
    

    glc.plot_counties(results=res,
                               parameter = "TotalProductionByTechnologyAnnual",
                               scenarios = scenario_list,
                               counties=counties,
                               list_counties = list_counties,
                               dcfg = dcfg,
                               naming=naming,
                               col=col,
                               agg_years = years_agg_file,
                               )
     
    glc.plot_county_impacts(results=res,
                               parameter = "UseByTechnologyAnnual",
                               scenarios = scenario_list,
                               counties=counties,
                               list_counties = list_counties,
                               dcfg = dcfg,
                               naming=naming,
                               col=col,
                               agg_years = years_agg_file,
                               )  

def plot_county(input_path,
                county,
                dataconfig_file,
                tech_to_sector_file,
                years_agg_file,
                scenario_list,
                str_filter=None,
                naming=None):
            
    # load data config file
    with open(dataconfig_file) as s:    
        try:
            dcfg = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logger.error(exc) 
    
    # load results
    res = op.load_results(results_path=input_path,
                          scenario_list = [v["name"] for v in scenario_list],
                          dcfg=dcfg)

    if naming is not None:
        naming = pd.read_excel(naming,sheet_name=None)
        naming = naming["TechnologiesList"].set_index("Name")["Description"]
    
    
    for s in [v["name"] for v in scenario_list]:
        gl.plot_tech_sector(results=res,
                            parameter="ProductionByTechnologyAnnual",
                            scenario = s,
                            #sector = "Agriculture",
                            geography=county,
                            naming = naming,
                            str_filter = str_filter,
                            mapping_tech_sector=tech_to_sector_file,
                            agg_years = years_agg_file,
                            xscale = years_agg_file,
                            )
    
    
    
        