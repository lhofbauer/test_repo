import os
import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


#%% Define util functions for dataset integration

# FIXME: develop scaling/calibration with nat model function
def scaling(data,etc):
    pass


def load_model(path):
    
    # check if directory exists
    if not os.path.isdir(path):
        raise ValueError("Model directory does not exists or a file and not"
                         " a directory is provided.")
    
    # get list of all files
    files = [f for f in os.listdir(path) if f.endswith(".xlsx")]
    
    data = dict()
    
    for f in files:
        data[f] = pd.read_excel(path+f,sheet_name=None,
                                keep_default_na=False)

    return data

def save_model(path, data, overwrite=False):
    
    # check if directory exists, create if not
    if not os.path.exists(path):
        os.makedirs(path)
    # check if it exists and is non-empty, exit if the case and overwrite=False
    elif len(os.listdir(path)) != 0:
        if not overwrite:
            logger.warning("Model already exists and will not be overwritten."
                           " Set 'overwrite' to True to overwrite existing"
                           " models.")
            return True
        

    for k in data.keys():
        with pd.ExcelWriter(path+k, engine='openpyxl') as writer:
            for s in data[k].keys():
                data[k][s].to_excel(writer,merge_cells=False,
                            sheet_name=s, index=False)     

    logger.info("Saved updated data.")
    
    return True


#%% Define functions for the integration of county-resolved datasets


def cookstove_dataset(data):
    # FIXME: move filepaths to a config file
    
    # for testing
    # input_path  ="../model/county_model/v010/"
    # data = load_model(input_path)
    
    #%% helper functions
    def to_frac(df):
        df = df.divide(df.sum())
        return df
    def to_abs(df,total):
        df = df*total
        return df
   
    #%% load county data
    
    # load list of counties
    counties = pd.read_csv("config_files/list_counties.csv",
                           keep_default_na=False)
    # load data tables
    cdf = pd.read_excel("../data/KNBS/Housing_Survey/Chapter-5-Housing-Characteristics-Amenities-and-Adequacy.xlsx",
                        sheet_name="Table 5.7",
                        skiprows=2,
                        usecols="A:S"
                        )

    cdf.columns = ['Geography',
                    'ELC001',
                    'ELC001',
                    'ELC001',
                    'ELC001',
                    'BGS001',
                    'LPG001',    
                    'ETH001',
                    'NA',
                    'BIO00X',
                    'BIO00X',
                    'CHC00X',
                    'BIO00X',
                    'BIO00X',
                    'BIO00X',
                    'KER001',
                    'NA',
                    'BIO00X',
                    'HH']

    cdf = cdf.set_index("Geography")
    cdf.columns.names = ["Stoves"]
    cdf = cdf.T.groupby("Stoves").sum()


    sdf = pd.read_excel("../data/KNBS/Housing_Survey/Chapter-5-Housing-Characteristics-Amenities-and-Adequacy.xlsx",
                        sheet_name="Table 5.8",
                        skiprows=1,
                        usecols="A:N"
                        )
    
    sdf.columns = ["Geography"] + list(sdf.columns[1:])
    sdf = sdf.set_index("Geography")
    sdf = sdf.T
    cdf.loc["BIO001",:] = cdf.loc["BIO00X",:]* (sdf.loc["Three stone stove/open fire",:]
                                                /(sdf.loc["Three stone stove/open fire",:]
                                                  +sdf.loc["Improved Firewood Jiko",:]))
    cdf.loc["BIO005",:] = cdf.loc["BIO00X",:]* (sdf.loc["Improved Firewood Jiko",:]
                                                /(sdf.loc["Three stone stove/open fire",:]
                                                  +sdf.loc["Improved Firewood Jiko",:]))
    cdf.loc["CHC001",:] = cdf.loc["CHC00X",:]* (sdf.loc["Ordinary Charcoal Jiko",:]
                                                /(sdf.loc["Ordinary Charcoal Jiko",:]
                                                  +sdf.loc["Improved Charcoal Jiko",:]))
    cdf.loc["CHC005",:] = cdf.loc["CHC00X",:]* (sdf.loc["Improved Charcoal Jiko",:]
                                                /(sdf.loc["Ordinary Charcoal Jiko",:]
                                                  +sdf.loc["Improved Charcoal Jiko",:])) 
    
    urdf = pd.read_excel("../data/KNBS/Housing_Survey/Chapter-3-Household-Demographic-and-Economic-Characteristics.xlsx",
                        sheet_name="Table 3.5",
                        skiprows=2,
                        usecols="A:D"
                        )
    urdf.columns = ["Geography","Rural","Urban","Total"]
    urdf = urdf.set_index("Geography")

    cdf = pd.concat([cdf,urdf.T])
    
    for g in ["Urban","Rural"]:
        for s in ['BGS001','ELC001','ETH001','KER001','LPG001',
                  'BIO001','BIO005','CHC001','CHC005']:
        
            if g == "Urban":
                prefix = "RK1"
                oth = "Rural"
            else:
                prefix = "RK2"
                oth = "Urban"

            cdf.loc[prefix+s,:] = (cdf.loc[s,:]#*cdf.loc["Total",:]
                                   /(cdf.loc[g,:]+(cdf.loc[oth,:]
                                     *cdf.loc[s,oth]/
                                     cdf.loc[s,g]))
                                   )
            # Equations:
            #     rf = uf*f
            #     f = rf_t/uf_t
            #     tf*totalHH = uf * uHH + rf * rHH
            #     uf = (tf*totalHH)/(uHH+f*rHH)
            

    # clean up
    cdf = cdf.loc[[i for i in cdf.index if i.startswith("RK")]]
    cdf = cdf.rename(columns={"Nairobi City":"Nairobi",
                              "Homabay":"HomaBay",
                              "Taita-Taveta":"TaitaTaveta"})
    cdf.columns = cdf.columns.str.replace(" ","")       
    cdf = cdf.rename(columns=counties.loc[:,["ID","NAME"]].set_index("NAME")["ID"].to_dict())
    cdf = cdf.iloc[:,4:]
    cdf.index.names = ["TECHNOLOGY"]
    cdf.columns.names = ["REGION"]


    # calibrate
    # FIXME: above calculation do not ensure percentage in rural/urban areas
    # in each county add up to one – to be looked at
    cdf.loc[cdf.index.str.startswith("RK1")] = cdf.loc[cdf.index.str.startswith("RK1")]/cdf.loc[cdf.index.str.startswith("RK1")].sum()
    cdf.loc[cdf.index.str.startswith("RK2")] = cdf.loc[cdf.index.str.startswith("RK2")]/cdf.loc[cdf.index.str.startswith("RK2")].sum()
    
    # rearrange dataframe
    cdf = cdf.stack()
    cdf = cdf.swaplevel()
    
    #%% load national data
    
    scenarios = ["S1","S2","S3","S4","S5"]
    
    for i,s in enumerate(scenarios):
        
        file = ("../data/nat_scens/run"+str((i+1))
                +"/csv"+str((i+1))+"/TotalTechnologyAnnualActivity.csv")
        # load activity data for national scenario
        df = pd.read_csv(file)
        df = df.set_index(list(df.columns[:-1]))
        
        # filter for cooking techs
        df = df.loc[df.index.get_level_values("t").str.contains("RK1|RK2")]
        
        # rearrange dataframe
        
        df = df.unstack(fill_value=0).droplevel(level=0,axis=1)
        df = df.droplevel(level=0)
        df.index.names = ["TECHNOLOGY"]
        
        # split in urban and rural, calculate and save national totals
        nru = df.loc[df.index.get_level_values("TECHNOLOGY").str.contains("RK2")]
        nur = df.loc[df.index.get_level_values("TECHNOLOGY").str.contains("RK1")]
        nrut = nru.sum()
        nurt = nur.sum()
        nru = to_frac(nru)
        nur = to_frac(nur)
        
        # calculate and save county totals (demand)
    
    
        df = None
        for k in data.keys():
            if "SpecifiedAnnualDemand" in data[k].keys():
                if df is None:
                    df = data[k]["SpecifiedAnnualDemand"]
                else:
                    df = pd.concat([df,data[k]["SpecifiedAnnualDemand"]])
                    
        # FIXME: integrate choosing scenarios once used
        df = df.drop(["MODEL","SCENARIO","PARAMETER"],axis=1)
        df = df.loc[df["COMMODITY"].str.startswith("DEMRK")]
        ct = df.set_index(["REGION","COMMODITY"])
                
    
        # extend to county dataframe
        cru = pd.concat([nru]*len(counties["ID"]),
                        keys=list(counties["ID"]),
                        names=["REGION"])
        cur = pd.concat([nur]*len(counties["ID"]),
                        keys=list(counties["ID"]),
                        names=["REGION"])  
        
        crut = cru*ct.xs("DEMRK2",level="COMMODITY")
        curt = cur*ct.xs("DEMRK1",level="COMMODITY")

    
    #%% integrate county baseline data
    
        baseline = True
        if baseline is not None:
            
            
            # apply to dataset
    
            baseyears = [2019,2020,2021,2022,2023,2024]
            for by in baseyears:
                cru.loc[:,by] = cdf
                cur.loc[:,by] = cdf
            cru = cru.fillna(0)
            cur = cur.fillna(0)


    #%% adjust years based on county baseline
    
    # nrutt = to_abs(nru,ct.xs("DEMRK2",level="COMMODITY").sum())
    
    # cru.loc[:,2050] = cru.loc[:,2024] 
    # crut = cru*ct.xs("DEMRK2",level="COMMODITY")
    
    # diff = nrutt- crut.groupby("TECHNOLOGY").sum()
    
    # po_tech = diff.loc[diff[2050]<0].index.to_list()
    
    # crut.loc[crut.index.get_level_values("TECHNOLOGY").isin(po_tech)].groupby("REGION").sum()/crut.groupby("REGION").sum()
    
    # fr1 = crut.loc[crut.index.get_level_values("TECHNOLOGY").isin(po_tech)].groupby("REGION").sum()/crut.loc[crut.index.get_level_values("TECHNOLOGY").isin(po_tech)].sum()
    
    # fr2 = crut.loc[crut.index.get_level_values("TECHNOLOGY").isin(po_tech)]/crut.loc[crut.index.get_level_values("TECHNOLOGY").isin(po_tech)].groupby("TECHNOLOGY").sum()
    
    # diffe = pd.concat([diff]*len(fr1),
    #                   names=["REGION"],
    #                   keys= fr1.index)
    # add = fr1*diffe.loc[~diffe.index.get_level_values("TECHNOLOGY").isin(po_tech)]
    # ded = fr2*diffe.loc[diffe.index.get_level_values("TECHNOLOGY").isin(po_tech)]
    # 
    # crut1 = crut+pd.concat([add,ded])
    # cru1 = to_frac(crut1)
    # crut1 = to_abs(cru1, cru*ct.xs("DEMRK2",level="COMMODITY"))    
    #%% replace selected years through interpolation
    
        iy = list(range(2025,2030))
        
        cru.loc[:,iy] = np.nan
        cru = cru.interpolate(axis=1)
        
        cur.loc[:,iy] = np.nan
        cur = cur.interpolate(axis=1)
        
        iy = list(range(2031,2050))
        
        cru.loc[:,iy] = np.nan
        cru = cru.interpolate(axis=1)
        
        cur.loc[:,iy] = np.nan
        cur = cur.interpolate(axis=1)    
        
        
        # ru = to_frac(ru)
        # ru = to_abs(ru, rut)



    #%% update model parameter and save
    
    # get

        crut = cru*ct.xs("DEMRK2",level="COMMODITY")
        curt = cur*ct.xs("DEMRK1",level="COMMODITY")
        
        # concat rural and urban, rearrange dataframe
        ow = pd.concat([crut,curt],
                       keys=[("#ALL",s,
                        "TotalTechnologyAnnualActivityLowerLimit")]
                        *(len(crut)+len(curt)),
                        names=["MODEL","SCENARIO","PARAMETER"])
    
        # add activity limits
        for k in data.keys():
            
            if "TotalTechnologyAnnualActivityLo" in data[k].keys():
                df = data[k]["TotalTechnologyAnnualActivityLo"]
                ind = ["MODEL",
                       "SCENARIO",
                       "PARAMETER",
                       "REGION",
                       "TECHNOLOGY"]
                df = df.set_index(ind)
                
                # FIXME: delete
                # fil = df.index.get_level_values("TECHNOLOGY").isin(crut.index.get_level_values("TECHNOLOGY").append(curt.index.get_level_values("TECHNOLOGY")))
                # df.loc[fil,:] = pd.concat([crut,curt],
                #           keys=[("#ALL","#ALL",
                #                 "TotalTechnologyAnnualActivityLowerLimit")]
                #           *(len(crut)+len(curt)),
                #           names=["MODEL","SCENARIO","PARAMETER"])
                
                # FIXME: if structure of spreadsheet is changed (e.g., not all
                # county technologies together in one sheet), this might not work
                cow = ow.loc[ow.index.get_level_values("REGION").isin(
                                df.index.get_level_values("REGION"))]            
                
                df = pd.concat([df,cow])
                #df = df.loc[~df.index.duplicated(keep='last'), :]
                
                # overwrite data
                data[k]["TotalTechnologyAnnualActivityLo"] = df.reset_index()
                
                # remove upper limit
                if "TotalTechnologyAnnualActivityUp" in data[k].keys():
                    dfu = data[k]["TotalTechnologyAnnualActivityUp"].set_index(ind)
                    # FIXME: just delete all cooking tech from limit?
                    fil = dfu.index.get_level_values("TECHNOLOGY").isin(crut.index.get_level_values("TECHNOLOGY").append(curt.index.get_level_values("TECHNOLOGY")))
                    dfu = dfu.loc[~fil,:]
                    
                    # overwrite data
                    data[k]["TotalTechnologyAnnualActivityUp"] = dfu.reset_index()
                
    return data

    #%%
    # load list of counties
    # counties = pd.read_csv("config_files/list_counties.csv",
    #                        keep_default_na=False)
    
    # # load data tables
    # cdf = pd.read_excel("../data/KNBS/Housing_Survey/Chapter-5-Housing-Characteristics-Amenities-and-Adequacy.xlsx",
    #                     sheet_name="Table 5.7",
    #                     skiprows=2,
    #                     usecols="A:S"
    #                     )
    
    # cdf.columns = ['Geography',
    #                 'ELC001',
    #                 'ELC001',
    #                 'ELC001',
    #                 'ELC001',
    #                 'BGS001',
    #                 'LPG001',    
    #                 'ETH001',
    #                 'NA',
    #                 'BIO00X',
    #                 'BIO00X',
    #                 'CHC00X',
    #                 'BIO00X',
    #                 'BIO00X',
    #                 'BIO00X',
    #                 'KER001',
    #                 'NA',
    #                 'BIO00X',
    #                 'HH']
    
    # cdf = cdf.set_index("Geography")
    # cdf.columns.names = ["Stoves"]
    # cdf = cdf.T.groupby("Stoves").sum()
    

    # sdf = pd.read_excel("../data/KNBS/Housing_Survey/Chapter-5-Housing-Characteristics-Amenities-and-Adequacy.xlsx",
    #                     sheet_name="Table 5.8",
    #                     skiprows=1,
    #                     usecols="A:N"
    #                     )
    # sdf.columns = ["Geography"] + list(sdf.columns[1:])
    # sdf = sdf.set_index("Geography")
    # sdf = sdf.T
    # cdf.loc["BIO001",:] = cdf.loc["BIO00X",:]* (sdf.loc["Three stone stove/open fire",:]
    #                                             /(sdf.loc["Three stone stove/open fire",:]
    #                                               +sdf.loc["Improved Firewood Jiko",:]))
    # cdf.loc["BIO005",:] = cdf.loc["BIO00X",:]* (sdf.loc["Improved Firewood Jiko",:]
    #                                             /(sdf.loc["Three stone stove/open fire",:]
    #                                               +sdf.loc["Improved Firewood Jiko",:]))
    # cdf.loc["CHC001",:] = cdf.loc["CHC00X",:]* (sdf.loc["Ordinary Charcoal Jiko",:]
    #                                             /(sdf.loc["Ordinary Charcoal Jiko",:]
    #                                               +sdf.loc["Improved Charcoal Jiko",:]))
    # cdf.loc["CHC005",:] = cdf.loc["CHC00X",:]* (sdf.loc["Improved Charcoal Jiko",:]
    #                                             /(sdf.loc["Ordinary Charcoal Jiko",:]
    #                                               +sdf.loc["Improved Charcoal Jiko",:])) 
    
    # urdf = pd.read_excel("../data/KNBS/Housing_Survey/Chapter-3-Household-Demographic-and-Economic-Characteristics.xlsx",
    #                     sheet_name="Table 3.5",
    #                     skiprows=2,
    #                     usecols="A:D"
    #                     )
    # urdf.columns = ["Geography","Rural","Urban","Total"]
    # urdf = urdf.set_index("Geography")
    
    # cdf = pd.concat([cdf,urdf.T])
    
    # for g in ["Urban","Rural"]:
    #     for s in ['BGS001','ELC001','ETH001','KER001','LPG001',
    #               'BIO001','BIO005','CHC001','CHC005']:
        
    #         if g == "Urban":
    #             prefix = "RK1"
    #             oth = "Rural"
    #         else:
    #             prefix = "RK2"
    #             oth = "Urban"

    #         cdf.loc[prefix+s,:] = (cdf.loc[s,:]#*cdf.loc["Total",:]
    #                                /(cdf.loc[g,:]+(cdf.loc[oth,:]
    #                                  *cdf.loc[s,oth]/
    #                                  cdf.loc[s,g]))
    #                                )
    #         # Equations:
    #         #     rf = uf*f
    #         #     f = rf_t/uf_t
    #         #     tf*totalHH = uf * uHH + rf * rHH
    #         #     uf = (tf*totalHH)/(uHH+f*rHH)
                
    
    # # clean up
    # cdf = cdf.loc[[i for i in cdf.index if i.startswith("RK")]]
    # cdf = cdf.rename(columns={"Nairobi City":"Nairobi",
    #                           "Homabay":"HomaBay",
    #                           "Taita-Taveta":"TaitaTaveta"})
    # cdf.columns = cdf.columns.str.replace(" ","")       
    # cdf = cdf.rename(columns=counties.loc[:,["ID","NAME"]].set_index("NAME")["ID"].to_dict())
    # cdf = cdf.iloc[:,4:]
    # cdf.index.names = ["TECHNOLOGY"]
    # cdf.columns.names = ["REGION"]

    
    # # calibrate
    # # FIXME: above calculation do not ensure percentage in rural/urban areas
    # # in each county add up to one – to be looked at
    # cdf.loc[cdf.index.str.startswith("RK1")] = cdf.loc[cdf.index.str.startswith("RK1")]/cdf.loc[cdf.index.str.startswith("RK1")].sum()
    # cdf.loc[cdf.index.str.startswith("RK2")] = cdf.loc[cdf.index.str.startswith("RK2")]/cdf.loc[cdf.index.str.startswith("RK2")].sum()
    
    # # stack
    # cdf = cdf.stack()
    # cdf.name = "fraction"
    
    # # apply dataset to model data
    
    # for k in data.keys():
    #     if "TotalTechnologyAnnualActivityLo" in data[k].keys():
    #         df = data[k]["TotalTechnologyAnnualActivityLo"]
    #         frac = df.copy().merge(right=cdf,on=["REGION","TECHNOLOGY"],how="left")
    #         ind = ["MODEL",
    #                "SCENARIO",
    #                "PARAMETER",
    #                "REGION",
    #                "TECHNOLOGY"]
    #         df = df.set_index(ind)
    #         frac = frac.set_index(ind)
    #         rkf = frac.index.get_level_values("TECHNOLOGY").str.startswith("RK")
    #         rk1f = frac.index.get_level_values("TECHNOLOGY").str.startswith("RK1")
    #         rk2f = frac.index.get_level_values("TECHNOLOGY").str.startswith("RK2")
    #         baseyears = [2019,2020,2021,
    #                      2022,2023,2024]
            
    #         frac.loc[rk1f,[c for c in frac.columns if c !="fraction"]]=frac.loc[rk1f,[c for c in frac.columns if c !="fraction"]]/frac.loc[rk1f,[c for c in frac.columns if c !="fraction"]].sum()
    #         frac.loc[rk2f,[c for c in frac.columns if c !="fraction"]]=frac.loc[rk2f,[c for c in frac.columns if c !="fraction"]]/frac.loc[rk2f,[c for c in frac.columns if c !="fraction"]].sum()
            
    #         frac.loc[rkf,baseyears] = frac.loc[rkf,"fraction"]
    #         frac = frac.drop("fraction",axis=1)
            
    #         frac.loc[rkf,[y for y in frac if y not in baseyears and y != 2050]] = pd.NA
    #         frac = frac.interpolate(axis=1)
            
    #         # replace with sum for rural/urban
    #         df.loc[rk1f]=df.loc[rk1f].sum().to_list()
    #         df.loc[rk2f]=df.loc[rk2f].sum().to_list()
            
    #         # apply fraction
    #         df.loc[rkf] = df.loc[rkf].mul(frac.loc[rkf],axis=0)
            
    #         # adjust upper limit
    #         if "TotalTechnologyAnnualActivityUp" in data[k].keys():
    #             dfu = data[k]["TotalTechnologyAnnualActivityUp"].set_index(ind)
    #             dfu[df.droplevel("PARAMETER").reindex_like(dfu.droplevel("PARAMETER"))
    #                 >dfu.droplevel("PARAMETER")] = df.droplevel("PARAMETER")*1.01
                
    #             data[k]["TotalTechnologyAnnualActivityUp"] = dfu.reset_index()
            
    #         # overwrite data
    #         data[k]["TotalTechnologyAnnualActivityLo"] = df.reset_index()
            
            
    # return data

