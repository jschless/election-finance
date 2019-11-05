import numpy as np
import pandas as pd


def load_dataset():
    data_dir = 'data/P00000001-ALL.csv'
    dateparse = lambda x: pd.datetime.strptime(x, '%d-%b-%y')
    raw_df = pd.read_csv(data_dir, index_col=False, parse_dates=['contb_receipt_dt'], date_parser=dateparse,
                         low_memory=False)

    filtered_df = remove_negative(raw_df)

    df = add_fields(filtered_df)
    df = clean_employers(df)
    df = clean_occupations(df)

    return df


def load_polls():
    # returns df with poll information
    data_dir = 'data/president_primary_polls.csv'
    raw_polls = pd.read_csv(data_dir, parse_dates=['created_at', 'start_date', 'end_date'])

    # there's lots of extra info in the df
    columns_to_keep = ['poll_id', 'start_date', 'end_date', 'party', 'candidate_name', 'pct', 'created_at', 'state', 'pollster', 'sponsors', 'pollster_rating_name', 'fte_grade', 'sample_size']
    
    df = raw_polls[columns_to_keep]

    return df


def clean_employers(df):
    corrections = {'SELF EMPLOYED': 'SELF-EMPLOYED',
                   'SELF': 'SELF-EMPLOYED',
                   'INFORMATION REQUESTED': 'NONE',
                   'INFORMATION REQUESTED PER BEST EFFORTS': 'NONE',
                   'NOT-EMPLOYED': 'NOT EMPLOYED',
                   'UNEMPLOYED': 'NOT EMPLOYED',
                   'INDEPENDENT CONTRACTOR': 'SELF-EMPLOYED',
                   'OWNER': 'SELF-EMPLOYED',
                   'DEPT OF DEFENSE': 'DOD',
                   'GOOGLE INC.': 'GOOGLE'}
    remove = [',',
              '.',
              ' INC',
              ' LLC',
              ' CORP']
    # May want to combine various DOD / Army forces together
    # Could also combined entrepreneurs and similar with self-employed
    # None and not employed could be the same?
    df['contbr_employer'] = df['contbr_employer'].replace(corrections)

    for val in remove:
        df['contbr_employer'] = df.contbr_employer.str.replace(val, '')
    return df


def clean_occupations(df):
    corrections = {'INFORMATION REQUESTED PER BEST EFFORTS': 'INFORMATION REQUESTED',
                   'OWNER': 'SELF-EMPLOYED',
                   'SELF': 'SELF-EMPLOYED',
                   'BUSINESS OWNER': 'SELF-EMPLOYED',
                   'SMALL BUSINESS OWNER': 'SELF-EMPLOYED',
                   'ENTREPRENEUR': 'SELF-EMPLOYED',
                   'CONTRACTOR': 'SELF-EMPLOYED',
                   'RN': 'NURSE',
                   'R.N.': 'NURSE',
                   'REGISTERED NURSE': 'NURSE',
                   'TRUCK DRIVER': 'DRIVER',
                   'REAL ESTATE BROKER': 'REAL ESTATE',
                   'COMMERCIAL REAL ESTATE': 'REAL ESTATE',
                   'REALTOR': 'REAL ESTATE',
                   'M.D.': 'PHYSICIAN',
                   'SURGEON': 'PHYSICIAN',
                   'DOCTOR': 'PHYSICIAN',
                   'MEDICAL DOCTOR': 'PHYSICIAN',
                   'GRADUATE STUDENT': 'STUDENT',
                   'SUBSTITUTE TEACHER': 'TEACHER'}
    # Could group together executives and various other positions
    df['contbr_occupation'] = df['contbr_occupation'].replace(corrections)
    return df


# TODO: Figure out if any implementation is necessary
def remove_negative(df):
    return df


def add_fields(df):
    # Add party affiliation
    df['party'] = np.where(df['cand_nm'] == 'Trump, Donald J.', 'Republican', 'Democratic')

    us_states = ['AL', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'IA',
                 'ID', 'IL', 'IN', 'KS', 'KY', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO',
                 'NC', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'PA',
                 'RI', 'SC', 'TN', 'TX', 'UT', 'VA', 'WA', 'WI', 'WV',
                 'AK', 'AR', 'HI', 'LA', 'MS', 'MT', 'ND', 'OR',
                 'SD', 'VT', 'WY']

    df['in_50_states'] = np.where(df['contbr_st'].isin(us_states), 1, 0)
    return df


def redownload_data():
    # automates downloading the updated data
    # hopefully they don't change the link :)

    import wget, zipfile
    url = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/Presidential_Map/2020/P00000001/P00000001-ALL.zip"
    filename = wget.download(url, out='./data/')
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall("./data/")
