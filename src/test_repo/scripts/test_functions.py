import logging
import yaml

import pandas as pd

# the ospro and fratoo modules need to currently be copied into the folder
# that also includes this script – these will be packaged in future
import ospro as op

import graphing_library as gl


logger = logging.getLogger(__name__)


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

    
    gl.plot_tech_sector(results=res,
                        parameter="ProductionByTechnologyAnnual",
                        scenario = "Reference",
                        #sector = "Agriculture",
                        geography=county,
                        naming = naming,
                        str_filter = str_filter,
                        mapping_tech_sector=tech_to_sector_file,
                        agg_years = years_agg_file,
                        xscale = years_agg_file,
                        )
    
    
    
        