import os
import sys
import logging

import pandas as pd
import yaml

import otoole

import COREWESM_county_functions as cf


logger = logging.getLogger(__name__)


#%% Set up logger

def setup_logger(level):
    

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


#%% 

def process_county_model(dataconfig_file,
                         input_path,
                         datasets,
                         output_path,
                         overwrite=False):
    """ Integrate county-resolved datasets
    

    Parameters
    ----------
    dataconfig_file : str
        File path to the data config file as required by the otoole package.
    input_path : str
        File path to the directory with the input model spreadsheet files.
    datasets : list
        List of dataset or other processing steps (strings) to be integrated.
    output_path : str
        File path to the directory where the processed model spreadsheet files
        are to be saved.
    overwrite : bool
        If to overwrite model if output_path is a non-empty, existing
        directory.

    Returns
    -------
    None.

    """
    
    # check if model already exists
    if os.path.exists(output_path) and len(os.listdir(output_path)) != 0:
        if not overwrite:
            logger.info("Model already exists and will not be overwritten."
                           " Set 'overwrite' to True to overwrite existing"
                           " models.")
            return
        
    # load data
    logger.info('Loading data.')
    data = cf.load_model(input_path)
    
    
    # integrate datasets
    options = ["cooking"]
    if "cooking" in datasets:
        data = cf.cookstove_dataset(data)
        logger.info("Integrated the 'cooking' dataset/enhancements.")
        
    rem = [e for e in datasets if e not in options]
    if len(rem) != 0:
        logger.warning("The following dataset/enhancements keys are not"
                       " implemented and thus have not been integrated: "
                       +", ".join(rem)+".")

    

    # save data
    cf.save_model(output_path, data, overwrite=overwrite)
    
    

def convert_datafile(dataconfig_file, data_file, output_file,
                     overwrite=False):
    """ Convert national model data from datafile to spreadsheet format
    
    Parameters
    ----------
    dataconfig_file : str
        File path to the data config file as required by the otoole package.
    data_file : str
        File path to the OSeMOSYS datafile to be converted.
    output_file : str
        File path to the output spreadsheet file.
    overwrite : bool
        If to overwrite model if output_file already exists. The default is
        False.
        
    Raises
    ------
    FileExistsError
        Raised when config or datafile do not exist.

    Returns
    -------
    None.

    """
    # check if model already exists
    if os.path.isfile(output_file):
        if not overwrite:
            logger.info("Model spreadsheet already exists and will not be"
                           " overwritten. Set 'overwrite' to True to"
                           " overwrite existing models.")
            return
        
    # check if files exist
    if not os.path.isfile(data_file):
        raise FileExistsError("Datafile does not exist.")
    if not os.path.isfile(dataconfig_file):
        raise FileExistsError("Data config file does not exist.")
        
    # convert files using otoole
    otoole.convert(dataconfig_file, 'datafile', 'excel',
                   data_file, output_file)
    
    logger.info("Successfully converted the datafile to a spreadsheet file.")

#%%


def downscale(input_file,
              dataconfig_file,
              tech_sector_mapping,
              comm_sector_mapping,
              list_counties,
              pop_file,
              pop_file_ruur,
              gdp_file,
              county_sectors,
              remove_fte_tech_mode,
              output_path,
              overwrite=False):
    """ Downscale the national model dataset to a county dataset

    Parameters
    ----------
    input_file : str
        DESCRIPTION.
    dataconfig_file : str
        DESCRIPTION.
    tech_sector_mapping : str
        DESCRIPTION.
    comm_sector_mapping : str
        DESCRIPTION.
    list_counties : str
        DESCRIPTION.
    pop_file : str
        DESCRIPTION.
    gdp_file : str
        DESCRIPTION.
    county_sectors : list
        DESCRIPTION.
    remove_fte_tech_mode : bool
        DESCRIPTION.
    output_path : str
        DESCRIPTION.
    overwrite : bool
        If to overwrite model if output_path is a non-empty, existing
        directory.
        
    Raises
    ------
    FileExistsError
        DESCRIPTION.
    ValueError
        DESCRIPTION.

    Returns
    -------
    None.

    """
    # check if model already exists
    if os.path.exists(output_path) and len(os.listdir(output_path)) != 0:
        if not overwrite:
            logger.info("Downscaled model already exists and will not be"
                           " overwritten. Set 'overwrite' to True to"
                           " overwrite existing models.")
            return
        
    # check if files exists
    if not os.path.isfile(input_file):
        raise FileExistsError("Input spreadsheet file does not exist.")
    if not os.path.isfile(dataconfig_file):
        raise FileExistsError("Data config file does not exist.")
        
    # create the output directory if it does not exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    
    # load technology to sector mapping
    tech_to_sector_data = pd.read_excel(tech_sector_mapping)
    tech_to_sector_dict = dict(zip(tech_to_sector_data['technology'], tech_to_sector_data['sector']))
    sector_to_tech_dict = dict()
    for sector,tech in zip(tech_to_sector_data["sector"],
                           tech_to_sector_data["technology"]):
        sector_to_tech_dict.setdefault(sector, []).append(tech)
    sectors_list = list(tech_to_sector_data["sector"].unique())
    
    # load demand commodities to sector mapping
    commodity_column = 'COMMODITY'
    com_to_sector_data = pd.read_excel(comm_sector_mapping)
    com_to_sector_dict = dict(zip(com_to_sector_data['commodity'], com_to_sector_data['sector']))
    sector_to_com_dict = dict()
    for sector,com in zip(com_to_sector_data["sector"],
                           com_to_sector_data["commodity"]):
        sector_to_com_dict.setdefault(sector, []).append(com)
    
    # load county list
    counties = pd.read_csv(list_counties,
                           keep_default_na=False)
    list_counties = counties["ID"].tolist()
    
    # load data config
    with open(dataconfig_file) as s:    
        try:
            dcfg = yaml.safe_load(s)
        except yaml.YAMLError as exc:
            logger.error(exc)  
    
    
    # load the spreadsheet file into pandas DataFrames
    model_data = pd.read_excel(input_file,
                               sheet_name=None,
                               engine="openpyxl",
                               #engine="xlrd"
                               )
    
    # get a list of all sheet names
    sheets_list = list(model_data.keys())
    
    
    # FIXME: use narrow table format (and then removing defaults – keep long format
    # (otherwise an issue with ospro at the moment)?
    
    # rearrange and clean sheets (remove defaults, set index)
    sheet_cap = {s:False for s in sheets_list}
    for param in dcfg.keys():
        # skip result variables
        if dcfg[param]["type"] == "result":
            continue
    
        if param in sheets_list:
            sheet_cap[param] = True
        elif "short_name" in dcfg[param].keys() and dcfg[param]["short_name"] in sheets_list:
            model_data[param] = model_data.pop(dcfg[param]["short_name"])
            sheet_cap[dcfg[param]["short_name"]] = True
        else:
            logger.info(f"Parameter {param} is not part of the input dataset.")
            continue
        
        # set index
        if "indices" in dcfg[param].keys():
            model_data[param] = model_data[param].set_index([i for i in dcfg[param]["indices"] if (i != "YEAR" or "YEAR" in model_data[param].columns)])
        
        # convert to long format if years in colums
        if "YEAR" in model_data[param].index.names:
            model_data[param] = model_data[param].unstack("YEAR").droplevel(level=0,
                                                                            axis=1)
            
        # filter based on default value, replace empty cells with default
        if dcfg[param]["type"]=="param":
            # get default value
            default = dcfg[param]["default"]
            # filter out rows with only default value
            model_data[param] = model_data[param].loc[~(model_data[param]==default).all(axis=1)]
            # replace empty cells with default values
            model_data[param] = model_data[param].fillna(default)
            
    if not all(sheet_cap.values()):
        logger.error("The following sheets do not correspond to entries in"
                     "the configuration file: " + ", ".join([k for k,v in sheet_cap.items() if v is False]))
        raise ValueError


    #%% Get sectoral mapping for emissions
    # FIXME: this would not work if an emission type is used across sectors
    em_to_sector_data = model_data["EmissionActivityRatio"].copy()
    em_to_sector_data = em_to_sector_data.reset_index()[["TECHNOLOGY","EMISSION"]]
    em_to_sector_data.loc[:,"TECHNOLOGY"] = em_to_sector_data.loc[:,"TECHNOLOGY"].replace(tech_to_sector_dict)
    em_to_sector_data.columns= ["sector","emission"]
    em_to_sector_data = pd.concat([em_to_sector_data,
                                   pd.DataFrame([["Services",
                                                  "CO2com"]],
                                                columns=["sector",
                                                         "emission"])])
    em_to_sector_data = em_to_sector_data.drop_duplicates()
    
    em_to_sector_dict = dict(zip(em_to_sector_data['emission'], em_to_sector_data['sector']))
    sector_to_em_dict = dict()
    for sector,em in zip(em_to_sector_data["sector"],
                           em_to_sector_data["emission"]):
        sector_to_em_dict.setdefault(sector, []).append(em)
    


    #%% Arrange downscaling factors

    # Read population data; total, urban and rural
    county_pop = pd.read_csv(pop_file,
                             keep_default_na=False,
                             usecols = ["ID"] + [str(y) for y in range(2019,2051)],
                             index_col= "ID",
                             nrows=47)
    county_pop = county_pop.apply(pd.to_numeric)
    county_pop = county_pop/county_pop.sum()
    county_pop.columns = county_pop.columns.astype(int)
    county_pop.index.name = "REGION"
    
    county_pop_ur = pd.read_csv(pop_file_ruur,
                             keep_default_na=False,
                             skiprows=3,
                             usecols = [2,5],
                             index_col= "ID",
                             nrows=47,
                             thousands=",")
    county_pop_ur = county_pop_ur.apply(pd.to_numeric)
    #county_pop_ur = county_pop_ur/county_pop_ur.sum()
    county_pop_ur.index.name = "REGION"
    county_pop_ur.columns = ["VALUE"]
    
    county_pop_ru = pd.read_csv(pop_file_ruur,
                             keep_default_na=False,
                             skiprows=3,
                             usecols = [5,8],
                             index_col= "ID",
                             nrows=47,
                             thousands=",")
    county_pop_ru = county_pop_ru.apply(pd.to_numeric)
    #county_pop_ru = county_pop_ru/county_pop_ru.sum()
    county_pop_ru.index.name = "REGION"
    county_pop_ru.columns = ["VALUE"]
    
    county_pop_urf = county_pop.mul((county_pop_ur/(county_pop_ur+county_pop_ru))
                                    ["VALUE"],axis=0)
    county_pop_urf = county_pop_urf/county_pop_urf.sum()
    county_pop_ruf = county_pop.mul((county_pop_ru/(county_pop_ur+county_pop_ru))
                                    ["VALUE"],axis=0)
    county_pop_ruf = county_pop_ruf/county_pop_ruf.sum()
    
    # Read GDP data
    # county_to_gdp = pd.read_csv(os.path.join(input_data_path, gdp_file), keep_default_na=False).set_index(column_counties_id)
    # if 'Administrative Unit' in county_to_gdp.columns:
    #     county_to_gdp = county_to_gdp.drop('Administrative Unit', axis=1)
    # gdp_share_data = pd.DataFrame(index=county_to_gdp.index)
    # for year in county_to_gdp.columns:
    #     kenya_gdp = county_to_gdp.loc['Kenya', year]
    #     gdp_share_data[year] = county_to_gdp[year] / kenya_gdp if kenya_gdp != 0 else 0
    # years_average_gdp_data = ['2015', '2016', '2017', '2018', '2019']
    # gdp_share_data_factor = gdp_share_data[years_average_gdp_data].mean(axis=1)
    

    # Read GCP data
    # The file GCP_Intro has the key to the columns in each GCP file
    gcp_data = pd.read_csv(gdp_file,
                           keep_default_na=False,
                           usecols= ["ID"] + ["Sec_"+str(i) for i in range (1,20)],
                           index_col = "ID",
                           nrows=47
                           )
    gcp_data = gcp_data.apply(pd.to_numeric)
    
    gcp_mapping = {"Agriculture":['Sec_1'],
                      "Industry":['Sec_2', 'Sec_3', 'Sec_6'],
                      "Services":['Sec_4', 'Sec_5',
                                  'Sec_7', 'Sec_9',
                                  'Sec_10', 'Sec_11',
                                  'Sec_12','Sec_13',
                                  'Sec_14', 'Sec_15',
                                  'Sec_16', 'Sec_17',
                                  'Sec_18', 'Sec_19'],
                      "Transport":['Sec_8']}
    
    
    gcp_factors = dict()
    for sec in gcp_mapping.keys():
        gcp_factors[sec] = gcp_data[gcp_mapping[sec]].sum(axis=1)
        gcp_factors[sec] = gcp_factors[sec]/gcp_factors[sec].sum()
        gcp_factors[sec].index.name = "REGION"
        

    #%% sector processing


    ### parameter processing
    # FIXME: move to a workflow system config (?)
    # define generic, non-sector parameters and set list
    gen = ["DiscountRate","YearSplit","TradeRoute",
           "UDCTag","UDCConstant","UDCMultiplierActivity", # UDC not currently used
           "UDCMultiplierNewCapacity","UDCMultiplierTotalCapacity",
           "CapitalCostStorage", "OperationalLifeStorage" # Storage not currently used
           ] + [n for n in dcfg.keys() if dcfg[n]["type"]=="set"]
    # params to be disaggregated
    # FIXME: get from multi-scale definition in future
    param_disagg = ['ResidualCapacity', 'AccumulatedAnnualDemand',
                    'SpecifiedAnnualDemand','TotalTechnologyAnnualActivityLowerLimit',
                    'TotalAnnualMaxCapacity','TotalAnnualMinCapacity',
                    'TechnologyActivityByModeUpperLimit']
    
    ### county-national links
    fuel_links_down = ["ELC003","DSL","GSL","BGS","BIO","CHC","ETH","KER","LPG",
                       "HFOMOM","HFONAI","JFL"]
    fuel_links_up = ["ELC001"]


    
    
    # create dicts for data
    ddata = dict()
    for g in list_counties + ["National"]:
        ddata[g] = dict()
        
    # FIXME: how does downscaling urban/rural work with this setup? Or not required?
    # downscale data
    for param in dcfg.keys():
        
        # skip result variables
        if dcfg[param]["type"] == "result":
            continue
        
        df_copy = model_data[param].copy()
        
        
        # FIXME: replace both allocation through a csv param_to_sector allocation
        # if set, general or unused param, save in national data
        if param in gen:
            if param in ddata["National"].keys():
                ddata["National"][param] = pd.concat([ddata["National"][param],
                                                      df_copy])
            else:
                ddata["National"][param] = df_copy   
            continue
        # if reserve margin or RE target, save in electricity sector data
        elif "ReserveMargin" in param or param.startswith("RE"):
                if param in ddata["National"].keys():
                    ddata["National"][param] = pd.concat([ddata["National"][param],
                                                          df_copy])
                else:
                    ddata["National"][param] = df_copy   
                continue
                   
        for sector in sectors_list:
            # get sector data
            if "TECHNOLOGY" in df_copy.index.names:
                fil = df_copy.index.get_level_values("TECHNOLOGY").isin(sector_to_tech_dict[sector])
                df_sec = df_copy.loc[fil]
                df_copy = df_copy.loc[~fil]
            elif "COMMODITY" in df_copy.index.names:
                if sector not in sector_to_com_dict.keys():
                    continue
                fil = df_copy.index.get_level_values("COMMODITY").isin(sector_to_com_dict[sector])
                df_sec = df_copy.loc[fil]
                df_copy = df_copy.loc[~fil]
            elif "EMISSION" in df_copy.index.names:
                if sector not in sector_to_em_dict.keys():
                    continue
                fil = df_copy.index.get_level_values("EMISSION").isin(sector_to_em_dict[sector])
                df_sec = df_copy.loc[fil]
                df_copy = df_copy.loc[~fil]
            else:
                logger.error(f"Data for sector {sector} for parameter {param} cannot be allocated.")
                raise ValueError
            
    
            # process data
            if df_sec.empty:
                continue
            else:
                # adjust to connect relevant fuels to national level
                if param == "InputActivityRatio" and sector in county_sectors:
                    for f in fuel_links_down:
                        df_sec = df_sec.rename(index={f:":RE1:"+f},
                                               level="COMMODITY")
                if param == "OutputActivityRatio" and sector in county_sectors:
                    for f in fuel_links_up:
                        df_sec = df_sec.rename(index={f:":RE1:"+f},
                                               level="COMMODITY")
                        
                        
                if sector in county_sectors and param not in param_disagg:
    
                    for c in list_counties:
                        df = df_sec.rename(index={"RE1":c})
                        if param in ddata[c].keys():
                            ddata[c][param] = pd.concat([ddata[c][param],df])
                        else:
                            ddata[c][param] = df
                elif sector in county_sectors and param in param_disagg:
                    for c in list_counties:
                        df = df_sec.rename(index={"RE1":c})
                        if sector == "Residential":
                            fac = county_pop
                        elif sector == "Residential-Urban":
                            fac = county_pop_urf
                        elif sector == "Residential-Rural":
                            fac = county_pop_ruf
                        elif sector in gcp_factors.keys():
                            fac = gcp_factors[sector]
                        else:
                            fac = 1/47
                            logger.warning(f"No downscaling factor defined for"
                                           f"sector {sector}, assume 1/47.")
                        if param in ddata[c].keys():
                            ddata[c][param] = pd.concat([ddata[c][param],df.mul(fac,axis=0)])
                        else:
                            ddata[c][param] = df.mul(fac,axis=0)
                else:
                    if param in ddata["National"].keys():
                        ddata["National"][param] = pd.concat([ddata["National"][param],
                                                              df_sec])
                    else:
                        ddata["National"][param] = df_sec.copy()
            
    
        if not df_copy.empty:
            logger.error(f"Data for parameter {param} could not be completely allocated."
                         "Remaining values include:")
            logger.error(df_copy.index)
            raise ValueError
    #%% simplify model if triggered
    
    # FIXME: note that removing FTE technologies currently also removes
    # sectoral emission accounting, could move these EmissionActivityRatios
    # to sector end-use tech
    if remove_fte_tech_mode:
        
        # get fuel connections
        relink = dict()
        for k in ddata.keys():
           relink.update({ddata[k]["OutputActivityRatio"].xs((fte,1),level=("TECHNOLOGY","MODE_OF_OPERATION")).index.get_level_values("COMMODITY").values[0]:
                          ddata[k]["InputActivityRatio"].xs((fte,1),level=("TECHNOLOGY","MODE_OF_OPERATION")).index.get_level_values("COMMODITY").values[0]
                          for fte in 
                          [t for t in ddata[k]["InputActivityRatio"].index.get_level_values("TECHNOLOGY") if t.startswith("FTE")]}
                         )
               
        for k in ddata.keys():
            for p in ddata[k].keys():
                # remove FTE techs and fuels, remove second mode
                if p == "TECHNOLOGY":
                    ddata[k][p] = ddata[k][p].loc[~ddata[k][p]["VALUE"].str.startswith("FTE")]
                    continue
                if p == "COMMODITY":
                    ddata[k][p] = ddata[k][p].loc[~ddata[k][p]["VALUE"].isin(relink.keys())]
                    continue
                if p== "MODE_OF_OPERATION":
                    ddata[k][p] = ddata[k][p].loc[ddata[k][p]["VALUE"]==1]
                    continue
                if "TECHNOLOGY" in ddata[k][p].index.names:
                    ddata[k][p] = ddata[k][p].loc[~ddata[k][p].index.get_level_values("TECHNOLOGY").str.startswith("FTE")]
                    # relink technologies using end use sector fuels
                    if "COMMODITY" in ddata[k][p].index.names:
                        ddata[k][p] = ddata[k][p].rename(index=relink)
                if "MODE_OF_OPERATION" in ddata[k][p].index.names:
                    ddata[k][p] = ddata[k][p].loc[ddata[k][p].index.get_level_values("MODE_OF_OPERATION")==1]
                
        
#%% save files
      
    # FIXME: remove empty sheets before saving?
    for k in ddata.keys():
        with pd.ExcelWriter(output_path+k+".xlsx", engine='openpyxl') as writer:
            for param in ddata[k].keys():
                if "short_name" in dcfg[param].keys():
                    sheet = dcfg[param]["short_name"]
                else:
                    sheet = param
                # df.insert(0, 'MODEL', '#ALL')
                # df.insert(1, 'SCENARIO', '#ALL')
                # df.insert(2, 'PARAMETER', sheet_name)
                # FIXME: use SET for sets as index level
                df = ddata[k][param]
                for l,v in zip(['PARAMETER','SCENARIO','MODEL'],
                               [param,'#ALL','#ALL']):
                    if dcfg[param]["type"] == "set":
                        df.insert(0, l, v)
                    else:
                        df = pd.concat([df],
                                       keys=[v],
                                       names=[l])
                # df = pd.concat([ddata[k][param]],
                #                keys=['#ALL','#ALL',param],
                #                names=['MODEL','SCENARIO','PARAMETER'])
                ind = True if dcfg[param]["type"]=="param" else False
                df.to_excel(writer,merge_cells=False,
                            sheet_name=sheet, index=ind)     

    logger.info("Successfully downscaled the national model.")
    
# FIXME: delete?
if __name__ == "__main__":
    

    data_file = '../model/national_model/data_smp_mod.txt' 
    output_file = '../model/National_parameter_model/data_smp.xlsx'
    dataconfig_file = './config_files/config_OSeMOSYS_Kenya.yaml'
    
    convert_datafile(dataconfig_file, data_file, output_file)