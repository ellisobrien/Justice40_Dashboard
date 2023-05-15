#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 23 17:53:20 2023

@author: ellisobrien

"""
#data processing and manipulatation packages
import pandas as pd
import numpy as np
from urllib.request import urlopen
import json

#visualization packages
import plotly.express as px
import plotly 

#allowing data access 
import redivis
import os

#dashboard packages
import streamlit as st
#setting token 
os.environ["REDIVIS_API_TOKEN"] = DB_TOKEN


#Writing dashboard title 
st.title("Mapping Environmental Injustice: An Exploration of CEJST and NRI Data")

#Adding text describing issue 
st.write('By allocating 40% of all climate and infrastructure dollars to disadvantaged communites, The Biden Administration is making historic investment in combatting environmental injustice.')


st.write('This tool allows users to visualize CEJST data at the census tract level to see which tracts are disadvantaged and other key correlations.')
         
st.write('This tool is intended for federal, state, and local policy makers, who may use it to gain a better understanding of how Justice40 funding will impact their state.')

@st.cache
def load_data1(allow_output_mutation=True):
    # Connect to Redivis
    organization = redivis.organization("EIDC")
    dataset = organization.dataset("socio_economic_physical_housing_eviction_and_risk_dataset_sepher")

    # Load data from Redivis
    df3 = dataset.query("""
    SELECT FIPS,
    STATEABBRV,
    STATEFIPS,
    COUNTYTYPE,
    COUNTYFIPS,
    STCOFIPS,
    TRACT,
    NRI_ID,
    POPULATION,
    BUILDVALUE,
    AGRIVALUE,
    AREA,
    RISK_SCORE,
    RISK_RATNG,
    RISK_NPCTL,
    RISK_SPCTL,
    EAL_SCORE,
    EAL_RATNG,
    EAL_NPCTL,
    EAL_SPCTL,
    EAL_VALT,
    EAL_VALB,
    EAL_VALP,
    EAL_VALPE,
    EAL_VALA,
    SOVI_SCORE,
    SOVI_RATNG,
    SOVI_NPCTL,
    SOVI_SPCTL,
    SOVI_VALUE,
    RESL_SCORE,
    RESL_RATNG,
    RESL_NPCTL,
    RESL_SPCTL,
    RESL_VALUE,
    AVLN_EALT,
    CFLD_EALT,
    CWAV_EALT,
    DRGT_EALT,
    ERQK_EALT,
    HAIL_EALT,
    HWAV_EALT,
    HRCN_EALT,
    LTNG_EALT,
    RFLD_EALT,
    SWND_EALT,
    TRND_EALT,
    WFIR_EALT,
    WNTW_EALT
    FROM `sepher`
    """).to_dataframe()
    
    df3.replace('NA', np.nan, inplace=True)

    # Remove rows with NaN values

    return df3
 
#Call the load_data function to load the data and cache the results
df3 = load_data1()





#importing CEJST Data
@st.cache(allow_output_mutation=True)
def load_data():
    # Connect to Redivis
    organization = redivis.organization("EIDC")
    dataset = organization.dataset("cejst_datasets")

    # Load data from Redivis
    df = dataset.query("""
    SELECT * FROM `eidc.cejst_datasets:jf53:next.communities:t46h`
    """).to_dataframe()

    return df
 
#Call the load_data function to load the data and cache the results
df = load_data()

df.rename(columns={'Census_tract_2010_ID': 'FIPS',
                  },    inplace=True)

df['FIPS'] = df['FIPS'].astype(str)
df['FIPS']=df['FIPS'].str.zfill(11)

df = pd.merge(df, df3, on="FIPS", how='left')

df['% Non-White'] = 1 - df['Percent_White']


#importing json
@st.cache(allow_output_mutation=True)
def load_data2():
    # Connect to Redivis
    organization = redivis.organization("EIDC")
    dataset2 = organization.dataset("census_tract_json")

    # Load data from Redivis
    df2 = dataset2.query("""
    SELECT * FROM `census_tract_json_json`
  """).to_dataframe()


    return df2

df2 = load_data2()


#cleaning CEJST Data 
df=df.replace({'NA': None})
df = df.astype({'PM2_5_in_the_air':'float', 'Housing_burden__percent_':'float', 'Percent_pre_1960s_housing__lead_paint_indicator_':'float', 'Median_value_____of_owner_occupied_housing_units':'float', 'Proximity_to_NPL__Superfund__sites':'float', 'Wastewater_discharge':'float', 'Diagnosed_diabetes_among_adults_aged_greater_than_or_equal_t':'float', 'Current_asthma_among_adults_aged_greater_than_or_equal_to_18':'float', 'Life_expectancy__years_':'float', 'Unemployment__percent_':'float', 'Percent_of_individuals___100__Federal_Poverty_Line':'float', 'Percent_individuals_age_25_or_over_with_less_than_high_schoo_2':'float', 'RISK_SCORE': 'float',
                'SOVI_SCORE': 'float',
                'EAL_VALT': 'float',
                'CFLD_EALT': 'float',
                'CWAV_EALT': 'float',
                'DRGT_EALT': 'float',
                'ERQK_EALT': 'float',
                'HAIL_EALT': 'float',
                'HWAV_EALT': 'float',
                'HRCN_EALT': 'float',
                'LTNG_EALT': 'float',
                'RFLD_EALT': 'float',
                'SWND_EALT': 'float',
                'TRND_EALT': 'float',
                'WFIR_EALT': 'float',
                'WNTW_EALT': 'float',
                'Percent_Black_or_African_American_alone': 'float',
                'Percent_American_Indian___Alaska_Native': 'float',
                'Percent_Asian': 'float',
                'Percent_Native_Hawaiian_or_Pacific': 'float',
                'Percent_two_or_more_races': 'float',
                'Percent_White': 'float',
                'Percent_Hispanic_or_Latino': 'float',
                'Percent_other_races': 'float'})

#cleaning Json Data  
df2['FIPS'] = df2[['FIPS']].astype(str)
df2['FIPS']=df2['FIPS'].str.zfill(11)
df2['STCNTY'] = df2[['STCNTY']].astype(str)
df2['STCNTY']=df2['STCNTY'].str.zfill(5)



#creating state_code variable 
state_codes = {'Alabama': 'AL',
               'Alaska': 'AK',
               'Arizona': 'AZ',
               'Arkansas': 'AR',
               'California': 'CA',
               'Colorado': 'CO',
               'Connecticut': 'CT',
               'Delaware': 'DE',
               'Florida': 'FL',
               'Georgia': 'GA',
               'Hawaii': 'HI',
               'Idaho': 'ID',
               'Illinois': 'IL',
               'Indiana': 'IN',
               'Iowa': 'IA',
               'Kansas': 'KS',
               'Kentucky': 'KY',
               'Louisiana': 'LA',
               'Maine': 'ME',
               'Maryland': 'MD',
               'Massachusetts': 'MA',
               'Michigan': 'MI',
               'Minnesota': 'MN',
               'Mississippi': 'MS',
               'Missouri': 'MO',
               'Montana': 'MT',
               'Nebraska': 'NE',
               'Nevada': 'NV',
               'New Hampshire': 'NH',
               'New Jersey': 'NJ',
               'New Mexico': 'NM',
               'New York': 'NY',
               'North Carolina': 'NC',
               'North Dakota': 'ND',
               'Ohio': 'OH',
               'Oklahoma': 'OK',
               'Oregon': 'OR',
               'Pennsylvania': 'PA',
               'Rhode Island': 'RI',
               'South Carolina': 'SC',
               'South Dakota': 'SD',
               'Tennessee': 'TN',
               'Texas': 'TX',
               'Utah': 'UT',
               'Vermont': 'VT',
               'Virginia': 'VA',
               'Washington': 'WA',
               'West Virginia': 'WV',
               'Wisconsin': 'WI',
               'Wyoming': 'WY'}



df['state_code'] = df['State_Territory'].map(state_codes)

#JSON FIltering function 
def json_filter(state):
    global shp_st
    shp_st=df2[df2.ST_ABBR == state]
    shp_st.to_file("myshpfile.geojson", driver = "GeoJSON")
    global tracts
    with open("myshpfile.geojson") as response:
        tracts = json.load(response)


######################################################################
#section header
st.header('Section 1: Mapping Justice 40 Status and Relevant Risk Factors')

st. write('According to the Climate and Economic Justice Screening Tool (CEJST), A community is highlighted as disadvantaged on the CEJST map if it is in a census tract that is (1) at or above the threshold for one or more environmental, climate, or other burdens, and (2) at or above the threshold for an associated socioeconomic burden.')
st.write ('The first map shows Justice 40 status, the highlights displays environmnetal risk factors, and third displays relevant socio-economic and demographic data')

st.subheader('Justice40 Status: See which census tracts are disadvantaged in your state of interest')

#section desrciption
#creating input for drop down 
state_list = df['state_code'].unique()
state_list = np.delete(state_list, 0)
state_list = sorted(state_list)




#drop down one 
Variable_Name1=st.selectbox(label="Select State to View",
options=(state_list))


state_centroids = {
    "AL": (32.806671, -86.791130),
    "AK": (61.370716, -152.404419),
    "AZ": (33.729759, -111.431221),
    "AR": (34.969704, -92.373123),
    "CA": (36.116203, -119.681564),
    "CO": (39.059811, -105.311104),
    "CT": (41.597782, -72.755371),
    "DE": (39.318523, -75.507141),
    "FL": (27.766279, -81.686783),
    "GA": (33.040619, -83.643074),
    "HI": (21.094318, -157.498337),
    "ID": (44.240459, -114.478828),
    "IL": (40.349457, -88.986137),
    "IN": (39.849426, -86.258278),
    "IA": (42.011539, -93.210526),
    "KS": (38.526600, -96.726486),
    "KY": (37.668140, -84.670067),
    "LA": (31.169546, -91.867805),
    "ME": (44.693947, -69.381927),
    "MD": (39.063946, -76.802101),
    "MA": (42.230171, -71.530106),
    "MI": (43.326618, -84.536095),
    "MN": (45.694454, -93.900192),
    "MS": (32.741646, -89.678696),
    "MO": (38.456085, -92.288368),
    "MT": (46.921925, -110.454353),
    "NE": (41.125370, -98.268082),
    "NV": (38.313515, -117.055374),
    "NH": (43.452492, -71.563896),
    "NJ": (40.298904, -74.521011),
    "NM": (34.840515, -106.248482),
    "NY": (42.165726, -74.948051),
    "NC": (35.630066, -79.806419),
    "ND": (47.528912, -99.784012),
    "OH": (40.388783, -82.764915),
    "OK": (35.565342, -96.928917),
    "OR": (44.572021, -122.070938),
    "PA": (40.590752, -77.209755),
    "RI": (41.680893, -71.511780),
    "SC": (33.856892, -80.945007),
    "SD": (44.299782, -99.438828),
    "TN": (35.747845, -86.692345),
    "TX": (31.968599, -99.901810),
    "UT": (39.320980, -111.093731),
    "VT": (44.558803, -72.577841),
    "VA": (37.431573, -78.656894),
    "WA": (47.382619, -120.447167),
    "WV": (38.640202, -80.622495),
    "WI": (44.624308, -89.994998),
    "WY": (42.995726, -107.551312)
}

state_val = state_centroids[Variable_Name1]

latitude = state_val[0]
longitude = state_val[1]



#formatting title text 
title_text = "**" + Variable_Name1 + "**"


#viz 1 function 
Description='Is Disadvantaged?'
variable_to_map='Identified_as_disadvantaged'
map_dat = df[['FIPS', 'County_Name', "RISK_SCORE", "% Non-White", variable_to_map]]


#defining function to map input variable  
def J40_map(input_var):
    fig1 = px.choropleth_mapbox(map_dat, geojson=tracts, locations='FIPS', featureidkey="properties.FIPS", color=input_var,
                             color_discrete_map={0: "DarkBlue", 1: "DarkRed"},
                               mapbox_style="carto-positron",
                               zoom=5, 
                               opacity=0.5, 
                               center = {"lat": latitude, "lon": longitude},
                               hover_data=["RISK_SCORE", "% Non-White"],
                               labels={input_var:Description}
                              )
    fig1.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig1.update_traces(marker_line_width=0, marker_opacity=0.7)
    fig1.update_geos(fitbounds="locations")
    st.plotly_chart(fig1)

#writing map title 
st.write('**Figure 1: Census Tract Status for**', title_text)
st.caption('Historically underserved communities are more likely to be identified as disadvantaged')

json_filter(Variable_Name1)
J40_map(variable_to_map)


######################################################################
#section header
st.subheader('Mapping Environmental Risk Factors by State')

#section desrciption
st.write("This section uses CEJST data and National Risk Index data (NRI) to Map Environmental and Natural Disaster Risk")

df4=df 
df4.rename(columns={'PM2_5_in_the_air': 'PM 2.5 in Air',
                   'Housing_burden__percent_': 'Housing Burden %',
                   'Percent_pre_1960s_housing__lead_paint_indicator_': '% Lead Paint',
                   'Median_value_____of_owner_occupied_housing_units': 'Median House Value',
                   'Proximity_to_NPL__Superfund__sites': 'Proximity to Super Fund',
                   'Wastewater_discharge': 'Waste Water Discharge',
                   'Diagnosed_diabetes_among_adults_aged_greater_than_or_equal_t': 'Diabetes Rate',
                   'Current_asthma_among_adults_aged_greater_than_or_equal_to_18': 'Asthma Rate',
                   'Life_expectancy__years_': 'Life Expectancy',
                   'Unemployment__percent_': 'Unemployment Percent',
                   'Percent_of_individuals___100__Federal_Poverty_Line': '% Below Poverty Line',
                   'Percent_individuals_age_25_or_over_with_less_than_high_schoo_2': '% Highschool Dropout', 
                   'RISK_SCORE':'Risk Score',
                   'SOVI_SCORE': 'Social Vulnerability Score',
                   'EAL_VALT':'Expected Annual Loss ($)',
                   'CFLD_EALT': 'Coastal Flooding Loss($)',
                   'CWAV_EALT': 'Cold Wave Loss($)',
                   'DRGT_EALT': 'Drought Loss($)',
                   'ERQK_EALT': 'Earthquake Loss($)',
                   'HAIL_EALT': 'Hail Loss($)',
                   'HWAV_EALT': 'Heat Wave Loss($)',
                   'HRCN_EALT': 'Hurricane Loss($)',
                   'LTNG_EALT': 'Lightning Loss($)',
                   'RFLD_EALT': 'Riverine Flooding Loss($)',
                   'SWND_EALT': 'Strong Wind Loss($)',
                   'TRND_EALT': 'Tornado Loss($)',
                   'WFIR_EALT': 'Wildfire Loss($)',
                   'WNTW_EALT': 'Winter Weather Loss($)'
                  },    inplace=True) 




#drop down one 
Variable_Name2=st.selectbox(label="Select Environmental Risk to View",
options=('PM 2.5 in Air',
'Expected agricultural loss rate (Natural Hazards Risk Index)',
'Expected building loss rate (Natural Hazards Risk Index)',
'Expected population loss rate (Natural Hazards Risk Index)',
'Share of properties at risk of flood in 30 years',
'Share of properties at risk of fire in 30 years',
        '% Lead Paint',
        'Proximity to Super Fund',
        'Waste Water Discharge',
        'Risk Score',
        'Social Vulnerability Score',
        'Expected Annual Loss ($)',
        'Coastal Flooding Loss($)',
        'Cold Wave Loss($)',
        'Drought Loss($)',
        'Earthquake Loss($)',
        'Hail Loss($)',
        'Heat Wave Loss($)',
        'Hurricane Loss($)',
        'Lightning Loss($)',
        'Riverine Flooding Loss($)',
        'Strong Wind Loss($)',
        'Tornado Loss($)',
        'Wildfire Loss($)',
         'Winter Weather Loss($)'))

#formatting title text 
df4=df4[df4.state_code == Variable_Name1]


title_text2 = "**" + Variable_Name2 + "**"

#defining function to map input variable  

def tract_map(input_var2):
    fig2 = px.choropleth_mapbox(df4, geojson=tracts, locations='FIPS', featureidkey="properties.FIPS", color=input_var2,
                               color_continuous_scale="balance",
                               mapbox_style="carto-positron",
                               zoom=5, 
                               opacity=0.5, 
                               center = {"lat": latitude, "lon": longitude},
                              )
    fig2.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig2.update_traces(marker_line_width=0, marker_opacity=0.7)
    fig2.update_geos(fitbounds="locations")
    st.plotly_chart(fig2)

    
#writing map title 
st.write('**Figure 2:', title_text2,  'for**', title_text)
st.caption('Environmental Risk Factors are a Key Element of Determining Which Tracts are Disadvantaged')

json_filter(Variable_Name1)
tract_map(Variable_Name2)

st.caption('Losses are heavily influenced by two factors: peril frequency/intensity and population. Population is highly correlated with loss since there tends to be more infrastructure and exposure in highly populated areas. For example, while Miami-Dade County and Los Angeles County are not inherently higher risk than their neighboring counties, their losses are much higher due to population.')



######################################################################
#section header
st.subheader('Mapping Socioeconomic and Demographic Risk Factors by State')

#section desrciption
st.write("This section uses CEJST data to map key demographic, health, economic information about census tracts.")


#drop down two 
var3=st.selectbox(label="Select Variable to View",
options=('% Below Poverty Line',
         'Housing Burden %',
         'Energy burden',
         'Median House Value',
         'Unemployment Percent',
         'Diabetes Rate',
         'Asthma Rate',
         'Life Expectancy',
         '% Highschool Dropout',
         '% Non-White',
         'Percent_Black_or_African_American_alone',
'Percent_American_Indian___Alaska_Native',
'Percent_Asian',
'Percent_Native Hawaiian_or_Pacific',
'Percent_two_or_more_races',
'Percent_White',
'Percent_Hispanic_or_Latino',
'Percent_other_races'
))
         

#st.dataframe(df) 
#st.dataframe(df3) 

title_text3 = "**" + var3 + "**"

#defining function to map input variable  

def tract_map2(input_var3):
    fig3 = px.choropleth_mapbox(df4, geojson=tracts, locations='FIPS', featureidkey="properties.FIPS", color=input_var3,
                               color_continuous_scale="balance",
                               mapbox_style="carto-positron",
                               zoom=5, 
                               opacity=0.5, 
                               center = {"lat": latitude, "lon": longitude},
                              )
    fig3.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig3.update_traces(marker_line_width=0, marker_opacity=0.7)
    fig3.update_geos(fitbounds="locations")
    st.plotly_chart(fig3)
    
#writing map title 
st.write('**Figure 3:', var3,  'for**', title_text)
st.caption('Socioeconomic Factors are a Key Element of Determining Which Tracts are Disadvantaged')

json_filter(Variable_Name1)
tract_map2(var3)

st.caption('Research shows that poorer communites and communities of color are most impacted by extreme weather and natural disasters.')
###########################################################################################################################
st.header('Section 2: Correlations between Demographics and Environmental Risk Factors')
st.write('This section shows correlations between environmental and demographic information.')


X_Name=st.selectbox(label="Select X-Axis Variable,",
options=('% Below Poverty Line',
         'Housing Burden %',
         'Energy burden',
         'Median House Value',
         'Unemployment Percent',
         'Diabetes Rate',
         'Asthma Rate',
         'Life Expectancy',
         '% Highschool Dropout',
         '% Non-White',
         'Percent_Black_or_African_American_alone',
'Percent_American_Indian___Alaska_Native',
'Percent_Asian',
'Percent_Native Hawaiian_or_Pacific',
'Percent_two_or_more_races',
'Percent_White',
'Percent_Hispanic_or_Latino',
'Percent_other_races'))


Y_Name=st.selectbox(label="Select Y-Axis Variable",
     options=('PM 2.5 in Air',
    'Expected agricultural loss rate (Natural Hazards Risk Index)',
    'Expected building loss rate (Natural Hazards Risk Index)',
    'Expected population loss rate (Natural Hazards Risk Index)',
    'Share of properties at risk of flood in 30 years',
    'Share of properties at risk of fire in 30 years',
            '% Lead Paint',
            'Proximity to Super Fund',
            'Waste Water Discharge',
            'Risk Score',
            'Social Vulnerability Score',
            'Expected Annual Loss ($)',
            'Coastal Flooding Loss($)',
            'Cold Wave Loss($)',
            'Drought Loss($)',
            'Earthquake Loss($)',
            'Hail Loss($)',
            'Heat Wave Loss($)',
            'Hurricane Loss($)',
            'Lightning Loss($)',
            'Riverine Flooding Loss($)',
            'Strong Wind Loss($)',
            'Tornado Loss($)',
            'Wildfire Loss($)',
             'Winter Weather Loss($)'))



def scatter_plot(x_value, y_value):
    fig4 = px.scatter(df4, x=x_value, y=y_value,
                     color='Identified_as_disadvantaged',
                     size_max=15,
                     labels={
                     'Identified_as_disadvantaged':'Disadvantaged'
                 },  template="simple_white", trendline="ols"
)

  #  fig.update_layout(transition_duration=500,  xaxis_range=[15, 45], yaxis_range=[-2,70])
    st.plotly_chart(fig4)
    

st.write('**Figure 4: Correlations between Environmental and Economic/Demographic Factors for**', title_text)
st.caption('Race and Wealth Play a Key Role in Disaster Exposure')

    
scatter_plot(X_Name, Y_Name)


###########################################################################################################################
st.header('Section 3: Peril level loss by state')
st.write('This section shows which perils present the most acute threat to your state of interest. This data comes from the National Risk Index.')


NRI_Map3=df4[['Coastal Flooding Loss($)',
'Cold Wave Loss($)',
'Drought Loss($)',
'Earthquake Loss($)',
'Hail Loss($)',
'Heat Wave Loss($)',
'Hurricane Loss($)',
'Lightning Loss($)',
'Riverine Flooding Loss($)',
'Strong Wind Loss($)',
'Tornado Loss($)',
'Wildfire Loss($)',
 'Winter Weather Loss($)']]

NRI_Map3.rename(columns={'Coastal Flooding Loss($)': 'Coastal Flooding',
                   'Cold Wave Loss($)': 'Cold Wave',
                   'Drought Loss($)': 'Drought',
                   'Earthquake Loss($)': 'Earthquake',
                   'Hail Loss($)': 'Hail',
                   'Heat Wave Loss($)': 'Heat Wave',
                   'Hurricane Loss($)': 'Hurricane',
                   'Lightning Loss($)': 'Lightning',
                   'Riverine Flooding Loss($)': 'Riverine Flooding',
                   'Strong Wind Loss($)': 'Strong Wind',
                   'Tornado Loss($)': 'Tornado',
                   'Wildfire Loss($)': 'Wildfire',
                   'Winter Weather Loss($)': 'Winter Weather',
                    
                    
                  },    inplace=True) 


#summing values
NRI_Map3=NRI_Map3.sum()
#converting to data frame
NRI_Map3=NRI_Map3.to_frame() 
#renaming columns 
NRI_Map3.reset_index(inplace=True)
NRI_Map3.rename(columns={0: 'Expected Annual Loss'},
          inplace=True)


#SORTING for bar graph
NRI_Map3=NRI_Map3.sort_values(by=['Expected Annual Loss'], ascending=False)


#making bargraph
fig5 = px.bar(NRI_Map3, x='index', y='Expected Annual Loss',
                 labels={"index": "<b> Peril </b>", 'Expected Annual Loss': '<b>Expected Annual Loss ($)</b>' },
                template="simple_white"
            )
fig5.update_layout(title_text = '<b>Figure 4: Loss by Peril For Selected State </b> <br><sup> Risk Profiles are Very Different Across States </sup>')
fig5.update_traces(marker_color='DarkRed')

#displaying bargraph
st.plotly_chart(fig5)
#displaying caption
st.caption('Earthquake is the leading cause of loss in California, inland flooding is the leading cause of loss in New York, hurricane is the leading cause of loss in Texas and Florida. Different regions, climates, and geographies contrinbute to very different dominant perils across states.')
