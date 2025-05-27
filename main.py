import boto3
from prefect import flow, task
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

AWS_ACCESS_KEY_ID = 'key_here'
AWS_SECRET_ACCESS_KEY = 'secret_here'
AWS_REGION = 'region_here'
BUCKET_NAME = 'bucket-here'



POSTGRES_USER = 'username'
POSTGRES_PASSWORD = 'password'
POSTGRES_DB = 'dabtase'
POSTGRES_HOST ='localhost'
POSTGRES_PORT = 5433

DB_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

#@task
def get_files_from_s3():
    s3_client = get_s3_client()
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    files = [item['Key'] for item in response.get('Contents', [])]
    return files
#@task
def get_data_from_file(filename:str, save_local=False)-> pd.DataFrame:
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=filename)
    items_csv = response['Body'].read().decode('utf-8')
    items_df = pd.read_csv(StringIO(items_csv))
    if save_local:
        items_df.to_csv(filename, index=False)
    
    return items_df

#@task
def get_s3_client():
    return boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                       region_name=AWS_REGION)



#@task
def standarize_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.lower() for col in df.columns if isinstance(col, str)]
    df['work email'] = df['work email'].apply(email_data_validation)
    df['other work emails'] = df['other work emails'].apply(email_data_validation)
    df['twitter'] = df['twitter'].apply(twitter_data_validation)
    df['github'] = df['github'].apply(github_data_validation)
    return df

### Enrichment Functions
def get_seniority_from_job_title(job_title: str) -> str:
    """Extracts seniority level from job title string.

    Args:
        job_title (str): The job title string to analyze.

    Returns:
        str: The seniority level extracted from the job title.
        Possible values are 'Junior', 'Mid', 'Senior', 'Lead', 'Principal', or 'Unknown'.
    """
    seniority_keywords = {
        'junior': 'Junior',
        'mid': 'Mid',
        'senior': 'Senior',
        'lead': 'Lead',
        'principal': 'Principal'
    }
    
    for keyword, seniority in seniority_keywords.items():
        if isinstance(job_title, str):
            if keyword in job_title.lower():
                return seniority
    
    return 'Unknown'

#@task
def has_github_profile(github: str) -> bool:
    """Check if the GitHub profile URL is valid."""
    if isinstance(github, str) and github.startswith('https://www.github.com/'):
        return True
    return False

#@task
def has_multiple_emails(work_email:str, other_work_emails: str) -> bool:
    """Check if the person has multiple work emails."""
    if work_email != 'NaN' and other_work_emails != 'NaN':
        return True
    else:
        return False

## Data Enrichment task
#@task
def data_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """Enrich the DataFrame with additional information.

    Args:
        df (pd.DataFrame): The DataFrame to enrich.

    Returns:
        pd.DataFrame: The enriched DataFrame with additional columns, such as 'seniority', 'tech profile', and 'multiple emails'.
    """
    df['seniority'] = df['job title'].apply(get_seniority_from_job_title)
    df['tech profile'] = df['github'].apply(has_github_profile)
    df['multiple emails'] = df.apply(lambda row: has_multiple_emails(row['work email'], row['other work emails']), axis=1)
    #print("Unique job titles:", get_unieuque_job_titles(df))
    return df


# Data Pipeline Flow


#@task
def email_data_validation(email: str) -> str:
    """Check if the email is valid."""
    if isinstance(email, str) and '@' in email and '.' in email:
        return email
    return 'NaN'

#@task
def twitter_data_validation(twitter: str) -> str:
    """Check if the Twitter handle is valid."""
    if isinstance(twitter, str) and twitter.startswith('https://twitter.com/'):
        return twitter
    return 'NaN'

#@task
def github_data_validation(github: str) -> str:
    """Check if the GitHub URL is valid."""
    if isinstance(github, str) and github.startswith('https://www.github.com/'):
        return github
    return 'NaN'

#@task
def linkedin_data_validation(linkedin: str) -> str:
    """Check if the LinkedIn URL is valid."""
    if isinstance(linkedin, str) and linkedin.startswith('https://www.linkedin.com/'):
        return linkedin
    return 'NaN'

#@task
def save_df_to_postgresql(df: pd.DataFrame, table_name: str):
    """Save the DataFrame to PostgreSQL database."""
    engine = create_engine(DB_URL)
    print(table_name)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Saved {len(df)} rows to table {table_name}")

#@task
def sub_process(file):
        df = get_data_from_file(file, save_local=False)
        df = standarize_data(df)
        df = data_enrichment(df)
        save_df_to_postgresql(df, table_name='enriched_data')
        return df

#@flow
def main(specific_file: str = None):
    file_list = get_files_from_s3()
    if specific_file is not None:
        sub_process(specific_file)
    else:
        for file in file_list:
            sub_process(file)



if __name__ == "__main__":
    main()