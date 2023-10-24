import pandas as pd
import numpy as np
from pandas._libs.tslibs.timedeltas import Timedelta
from datetime import datetime
import sys
import warnings
import os





columns_to_remove = ['action_status', 'contact','contact_type','sys_updated_on']


rename_columns={'number':'Number',
                'sys_created_on':'Created(EST)',
                'first_response_time':'First Response Time(EST)',
                'short_description':'Short description',
                'account':'Account',
                'state':'State',
                'priority':'Priority',
                'assigned_to':'Assigned to',
                'opened_by':'Opened_by'
}

Domain={
    'Azure:':'Azure-Related Alerts',
    'Azure':'Azure-Related Alerts',
    '5XX':'Data Issues from Application, Redemption, Missing Data, GMS',
    'Bus':'Azure-Related Alerts, Data Issues from Application, Redemption, Missing Data, GMS',
    'Database Failed Connections System Errors':'IT, SQL Server (On-Premises and Azure MI)',
    'Log IO':'IT, SQL Server (On-Premises and Azure MI)',
    'DTU':'IT, SQL Server (On-Premises and Azure MI)',
    'Data IO':'IT, SQL Server (On-Premises and Azure MI)',
    'Database':'IT, SQL Server (On-Premises and Azure MI)',
    '[Fivetran':'Snowflake, Fivetran, DBT',
    'Fivetran':'Snowflake, Fivetran, DBT',
    'fivetran':'Snowflake, Fivetran, DBT',
    'SQL':'IT, SQL Server (On-Premises and Azure MI)',
    'dbt':'Snowflake, Fivetran, DBT'
}

Frequency={
    'Severity: 0':'10 minutes',
    'Severity: 1':'10 minutes',
    'Severity: 2':'30 minutes',
    'Severity: 3':'30 minutes',
    'dbt':'60 minutes',
    'Fivetran':None,
    'fivetran':None,
    '[Fivetran':None
}


Threshold={
    '5XX':5,
    'Bus Server':10,
    'SQL Server Database':1,
    'Bus Dead':50,
    'IO':'90 percent',
    'DTU':'90 percent',
    'Data IO':'90 percent',
    'Event Hub Server':1,
    '[Fivetran':None,
    'Fivetran':None,
    'fivetran':None,
    'dbt':None
}
newcolumns={
    'Remarks':11,
    'Resolution Summary':12
}
rename={'Number_x':'Number'
}

col=['Number', 'Created(EST)', 'First Response Time(EST)', 'Short description', 'Account', 'Frequency', 'Number of Alerts in Total', 'Threshold', 'State', 'Severity', 'Priority', 'Assigned to','Opened_by', 'Remarks','Resolution Summary', 'Domain']

# col1=['Number', 'Created(EST)', 'First Response Time(EST)', 'Short description', 'Account', 'State', 'Severity', 'Priority', 'Assigned to', 'Remarks','Opened_by']

 #Tests
def total_count_check(df,total_count):
    total=0
    for i in range(0,len(df)):
        total+=df['Number of Alerts in Total'].iloc[i]
    if total == total_count:
        return True
    
    else:
        return False
def split_check(df,df_P1,df_P2,df_P3):
    if len(df) == len(df_P1)+len(df_P2)+len(df_P3):
        return True
    
    else:
        return False

def source_final_distribution(raw,df):
    raw_P1=raw[raw['priority'] == '1 - Critical']
    raw_P2=raw[raw['priority'] == '2 - High']
    raw_P3=raw[(raw['priority'] != '2 - High') & (raw['priority'] != '1 - Critical')]

    final_P1=int(df.where(df['Priority'] == '1 - Critical')['Number of Alerts in Total'].sum())
    final_P2=int(df.where(df['Priority'] == '2 - High')['Number of Alerts in Total'].sum())
    final_P3=int(df.where((df['Priority'] != '2 - High') & (df['Priority'] != '1 - Critical'))['Number of Alerts in Total'].sum())
    
    if len(raw_P1)+len(raw_P2)+len(raw_P3) == final_P1+final_P2+final_P3:
        return True
    else:
        return False
    

def change_to_minutes(x):
    time_delta = Timedelta(x)

    # Convert Timedelta to minutes
    minutes = time_delta.total_seconds() / 60
    return minutes

def changes_to_minandsec(x):
    total_seconds = x.seconds
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    
    return str(minutes) + ' minutes and '+ str(seconds) + ' seconds'


warnings.filterwarnings("ignore")
def create_Excel(file,tofile):
    raw =  pd.read_csv(file, encoding='latin-1')
    
    #Removing Columns
    temp=raw.drop(columns=columns_to_remove)
    
    #Renaming Columns
    temp.rename(columns=rename_columns, inplace=True)
    
    #Casting string to date
    temp['Created(EST)']=pd.to_datetime(temp['Created(EST)'], format='%m/%d/%Y %I:%M:%S %p')
    temp['First Response Time(EST)']=pd.to_datetime(temp['First Response Time(EST)'], format='%m/%d/%Y %I:%M:%S %p')
    
    for i in range(0,len(temp)):
        a=temp['Short description'][i].split(' ')
        if a[2]=='Severity:':
            temp.at[i, 'Severity'] = a[2]+a[3]
        else:
            temp.at[i, 'Severity'] = 'Manually Created'
    
    # temp=temp[col1]
    temp1=temp
    
    #Making a new column to remove discrepencies
    temp1['despr'] = temp1['Short description'].str.cat(temp1['Priority'], sep='_')
    #Getting Frequency of each alert
    temp2=temp1.groupby('despr')['Number'].agg('count')
    temp2=pd.DataFrame(temp2)
    freq=temp2
    freq.reset_index(inplace=True)
    
    #Getting unique rows
    unique_df = temp1.drop_duplicates(subset=['despr'])
    
    #Perform Join Operation
    df=unique_df.merge(freq,on='despr')
    df['Number of Alerts in Total']=df['Number_y']
    
    
    #Inserting Threshold values
    for key,value in Threshold.items():
        for i in range(0,len(df)):
            if key in df['Short description'].iloc[i]:
                if isinstance(value, str):
                    df.at[i, 'Threshold'] = value
                elif value!=None:
                    df.at[i, 'Threshold'] = int(value) 
    
    #For inserting Frequency
    for key,value in Frequency.items():
        for i in range(0,len(df)):
            if key in df['Short description'].iloc[i]:
                df.at[i, 'Frequency'] = value
    
    #Inserting Domain Values
    for key,value in Domain.items():
        for i in range(0,len(df)):
            if key in df['Short description'].iloc[i]:
                df.at[i, 'Domain'] = value
            
    #Inserting remaining columns
    for key, values in newcolumns.items():
        df.insert(values, key, '')
    
    #Final clearing
    df.rename(columns=rename, inplace=True)
    df.drop(columns=['Number_y'],inplace=True)
    
    df=df[col]
    
    df = df.replace(np.nan, None)
    #Filtering according to priorities
    df_P1=df[df['Priority'] == '1 - Critical']
    df_P2=df[df['Priority'] == '2 - High']
    df_P3=df[(df['Priority'] != '2 - High') & (df['Priority'] != '1 - Critical')]
    
    #Resetting indexes
    df_P1.reset_index(inplace=True)
    df_P2.reset_index(inplace=True)
    df_P3.reset_index(inplace=True)
    df_P1.reset_index(inplace=True)
    df_P2.reset_index(inplace=True)
    df_P3.reset_index(inplace=True)
    
    #Dropping previous indexes
    df_P1.drop(columns=['index'],inplace=True)
    df_P2.drop(columns=['index'],inplace=True)
    df_P3.drop(columns=['index'],inplace=True)

    rename_={
         'level_0':'Sl.No'
    }
    df_P1.rename(columns=rename_, inplace=True)
    df_P2.rename(columns=rename_, inplace=True)
    df_P3.rename(columns=rename_, inplace=True)
    
    #Creating SLno column
    df_P1['Sl.No']=df_P1['Sl.No']+1
    df_P2['Sl.No']=df_P2['Sl.No']+1
    df_P3['Sl.No']=df_P3['Sl.No']+1
    
    #Preparing raw sheet
    temp.reset_index(inplace=True)
    rename_={
         'index':'Sl.No'
    }
    temp.rename(columns=rename_, inplace=True)
    temp['Sl.No']=temp['Sl.No']+1
    
    #Determining SLA
    #temp['Response Time(Minutes)'] its for finding SLA and other temp['Created(EST)'] for display purpose
    temp['Response Time(Minutes)']=temp['First Response Time(EST)']-temp['Created(EST)']
    
    temp['Response Time']=temp['Response Time(Minutes)']
    temp['Response Time(Minutes)']=temp['Response Time(Minutes)'].apply(change_to_minutes)
    
    temp['Response Time']=temp['Response Time'].apply(changes_to_minandsec)
    
    temp.insert(10, 'SLA', '')
    for i in range(0,len(temp)):
        if (temp['Priority'][i]=='1 - Critical') and temp['Response Time(Minutes)'][i] >15:
            temp.at[i,'SLA']='SLA Breached'
        
        elif (temp['Priority'][i]=='2 - High') and temp['Response Time(Minutes)'][i]>60:
            temp.at[i,'SLA']='SLA Breached'
        
        elif (temp['Priority'][i]=='3 - Moderate') and temp['Response Time(Minutes)'][i]>120:
            temp.at[i,'SLA']='SLA Breached'
        
        elif (temp['Priority'][i]=='4 - Low') and temp['Response Time(Minutes)'][i] >240:
            temp.at[i,'SLA']='SLA Breached'
        
        else:
            temp.at[i,'SLA']='Under SLA'
    
    SLA_Breched=temp[temp['SLA'] == 'SLA Breached']
    
    
    SLA_Breched.reset_index(inplace=True)
    SLA_Breched.reset_index(inplace=True)
    
    SLA_Breched.rename(columns=rename_, inplace=True)
    
    SLA_Breched['Sl.No']+=1
    
    temp.drop(columns=['Response Time(Minutes)','despr'],inplace=True)
    SLA_Breched.drop(columns=['Response Time(Minutes)','despr'],inplace=True)

    if total_count_check(df,len(raw)) and split_check(df,df_P1,df_P2,df_P3) and source_final_distribution(raw,df) == True:
        excel_file = tofile
        with pd.ExcelWriter(excel_file) as writer:
            # Write each DataFrame to a separate sheet
            temp.to_excel(writer, sheet_name='Raw', index=False)
            df_P1.to_excel(writer, sheet_name='P1_Alerts', index=False)
            df_P2.to_excel(writer, sheet_name='P2_Alerts', index=False)
            df_P3.to_excel(writer, sheet_name='P3&P4_Alerts', index=False)
            SLA_Breched.to_excel(writer, sheet_name='SLA_Breched', index=False)
        print(f'Data saved to {excel_file}')
        print('Loading....')
        os.system(f'start excel "{excel_file}"')
        print('END')
    else:
        print("Tests Failed")

if __name__ == "__main__":
    # Suppress DeprecationWarnings
    file=sys.argv[1]
    tofile=sys.argv[2]
    create_Excel(file,tofile)
    # create_Excel('sn_customerservice_case _Aug_Week3.csv')