import pandas as pd

# Clean EPC column
def extract_epc(value):
    if isinstance(value, str) and '_' in value:
        parts = value.split('_')
        return parts[-1]
    else:
        return value
    
def map_to_numerical(column, mapping):
    return column.map(mapping)

def run_clean():
    # Read the CSV file into a pandas DataFrame
    immo = pd.read_csv('/home/patchwork/Documents/projects_becode/immo_eliza_goats/data/raw/data.csv')
    
    
    #remove unnecessary columns
    #immo = immo.drop(columns=["Unnamed: 0", "public_sales","notary_sales","country","id", "longitude", "latitude", "link"], axis=1)
    immo.drop(columns=["Unnamed: 0"], axis=1, inplace=True)
    
    
    #df = df.drop(df.columns[[0, 1, 30, 31, 32, 33]], axis=1)
    
    # Replace empty values with NaN
    immo.replace('', pd.NA, inplace=True)
    
    immo.head()
    #clean EPC
    immo['epc'] = immo['epc'].apply(extract_epc)
    immo['epc'].value_counts().to_frame()
    
    #Custom mappings
    epc_mapping = {'A++': 9, 'A+': 8, 'A': 7, 'B': 6, 'C': 5, 'D': 4, 'E': 3, 'F': 2, 'G': 1}
    state_mapping = {'JUST_RENOVATED': 6, 'AS_NEW': 5, 'GOOD': 4, 'TO_BE_DONE_UP': 3, 'TO_RENOVATE': 2, 'TO_RESTORE': 1}
    propert_type={'APARTMENT': 1, 'HOUSE': 0}
    # Apply mappings to create new numerical columns
    immo["epc"] = map_to_numerical(immo["epc"], epc_mapping)
    immo["state_building"] = map_to_numerical(immo["state_building"], state_mapping)
    immo["property_type"] = map_to_numerical(immo["property_type"], propert_type)
    
    
    # Create a dictionary with the different regions. 
    belgium_regions = {
        'Antwerp': 'Flanders',
        'Limburg': 'Flanders',
        'East Flanders': 'Flanders',
        'Flemish Brabant': 'Flanders',
        'West Flanders': 'Flanders',
        'Hainaut': 'Wallonia',
        'Walloon Brabant': 'Wallonia',
        'Namur': 'Wallonia',
        'Liège': 'Wallonia',
        'Luxembourg': 'Wallonia',
        'Brussels': 'Brussels-Capital'
    }
    # Create a new data set and map it with belgium_regions
    immo["region"] = immo["province"].map(belgium_regions)
    
    # clean the data -> romove nan in total_area_m2
    cleaned_sqm = immo.dropna(subset="total_area_sqm")
    
    ## calculate the price/sqm
    immo["price_square"] = cleaned_sqm["price"] / cleaned_sqm["total_area_sqm"]
    
    immo.to_csv("/home/patchwork/Documents/projects_becode/immo_eliza_goats/data/clean/data.csv")
    
if __name__ == "__main__":

    run_clean()

