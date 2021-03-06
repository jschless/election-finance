# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import wget, zipfile

def load_dataset():
    data_dir = 'data/P00000001-ALL.csv'
    dateparse = lambda x: pd.datetime.strptime(x, '%d-%b-%y')
    raw_df = pd.read_csv(data_dir, index_col=False, parse_dates=['contb_receipt_dt'], date_parser=dateparse,
                         low_memory=False)

    filtered_df = filter(raw_df)

    df = add_fields(filtered_df)
    df = clean_employers(df)
    df = clean_occupations(df)

    #TODO: Remove donations for 2018 (and possibly for 2020G?)

    return df


def load_polls():
    # returns df with poll information
    data_dir = 'data/president_primary_polls.csv'
    raw_polls = pd.read_csv(data_dir, parse_dates=['created_at', 'start_date', 'end_date'])

    # there's lots of extra info in the df
    columns_to_keep = ['poll_id', 'start_date', 'end_date', 'party', 'candidate_name', 'pct', 'created_at', 'state', 'pollster', 'sponsors', 'pollster_rating_name', 'fte_grade', 'sample_size']

    df = raw_polls[columns_to_keep]

    grade_to_num = {'D-': 0, 'C-': 1,'C': 2, 'C+': 3, 'B-': 4, 'B': 5, 'B+': 6, 'A-': 7, 'A': 8, 'A+': 9}
    df['fte_grade_num'] = df.fte_grade.apply(lambda x: grade_to_num.get(x, -1))
    # assigns a numeric value for the poll grades 

    # make candidate names the same in polls and contribution data
    name_dict = {'Joseph R. Biden Jr.': 'Biden, Joseph R Jr',
                 'Elizabeth Warren': 'Warren, Elizabeth ',
                 'Bernard Sanders': 'Sanders, Bernard',
                 'Pete Buttigieg': 'Buttigieg, Pete',
                 'Kamala D. Harris': 'Harris, Kamala D.',
                 'Cory A. Booker': 'Booker, Cory A.',
                 'Amy Klobuchar': 'Klobuchar, Amy J.',
                 'Andrew Yang': 'Yang, Andrew',
                 'Tim Ryan': 'Ryan, Timothy J.',
                 'Steve Bullock': 'Bullock, Steve',
                 'Julián Castro': 'Castro, Julián',
                 'John K. Delaney': 'Delaney, John K.',
                 'Tulsi Gabbard': 'Gabbard, Tulsi',
                 'Tom Steyer': 'Steyer, Tom',
                 'Joe Sestak': 'Sestak, Joseph A. Jr.',
                 'Marianne Williamson': 'Williamson, Marianne ',
                 'Michael F. Bennet': 'Bennet, Michael F.',
                 'Donald Trump': 'Trump, Donald J.',
                 'Joe Walsh': 'Walsh, Joe',
                 'William F. Weld': 'Weld, William Floyd (Bill)',
                 'Bill de Blasio': 'de Blasio, Bill',
                 'Jay Robert Inslee': 'Inslee, Jay R',
                 'Kirsten E. Gillibrand': 'Gillibrand, Kirsten ',
                 'Mike Gravel': 'Gravel, Maurice Robert',
                 'Eric Swalwell': 'Swalwell, Eric Michael',
                 'John Hickenlooper': 'Hickenlooper, John W.',
                 'Seth Moulton': 'Moulton, Seth',
                 'Richard Neece Ojeda': 'Ojeda, Richard Neece II',
                 'Paul Ryan': 'Ryan, Timothy J.'}

    # might as well change the name of the column to be consistent too
    df['cand_nm'] = df.candidate_name.apply(lambda x: name_dict.get(x, 'Unknown'))
    df = df.drop(columns=['candidate_name'])
    return df


def load_debates():
    # returns list of democratic debate dates
    dem_debates = [pd.datetime(2019,6,26), pd.datetime(2019,6,27), pd.datetime(2019,7,30),
                   pd.datetime(2019,7,31), pd.datetime(2019,9,12), pd.datetime(2019,10,15)]
    return dem_debates


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
                   'SUBSTITUTE TEACHER': 'TEACHER',
                   'PROGRAMMER': 'SOFTWARE DEVELOPER',
                   'SOFTWARE ENGINEER': 'SOFTWARE DEVELOPER'}
    # Could group together executives and various other positions
    df['contbr_occupation'] = df['contbr_occupation'].replace(corrections)
    return df


def filter(df):
    df = df[df['contb_receipt_amt']<= 2800]  # Remove superpacs and carried over amounts
    return df


def add_fields(df):
    # Add party affiliation

    republicans = ['Trump, Donald J.', 'Weld, William Floyd (Bill)']
    df['party'] = np.where(df['cand_nm'].isin(republicans), 'Republican', 'Democratic')

    us_states = ['AL', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'IA',
                 'ID', 'IL', 'IN', 'KS', 'KY', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO',
                 'NC', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'PA',
                 'RI', 'SC', 'TN', 'TX', 'UT', 'VA', 'WA', 'WI', 'WV',
                 'AK', 'AR', 'HI', 'LA', 'MS', 'MT', 'ND', 'OR',
                 'SD', 'VT', 'WY']

    df['in_50_states'] = np.where(df['contbr_st'].isin(us_states), 1, 0)

    # Adding unique contributor ID using the name and zip code
    df['contbr_id'] = df['contbr_nm'].map(str) + '_' + df['contbr_zip'].map(str)
    df['month'] = df['contb_receipt_dt'].values.astype('datetime64[M]')

    #TODO: Should we flag massive donations (>$1M+) and big unitemized donations

    return df


def redownload_data():
    # automates downloading the updated data
    # hopefully they don't change the link :)

    url = "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/Presidential_Map/2020/P00000001/P00000001-ALL.zip"
    filename = wget.download(url, out='./data/')
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall("./data/")
