import pandas as pd
import sqlite3 

def my_m_and_a(content_database_1, content_database_2, content_database_3, sql_file):
    db_file = sql_file[:-4]+'.db'
    
    #reading csv files
    df1 = pd.read_csv(content_database_1)
    df2 = pd.read_csv(content_database_2, sep = ';', header = None, names = ['Age', 'City', 'Gender', 'Name', 'Email'])
    df3 = pd.read_csv(content_database_3, delimiter= '\t', names= ['Gender','Name','Email','Age','City','Country'],
                 header= 0, na_values= ['N/A'])
    
    #general cleaning of df3
    df3 = clean_prefixes(df3)

    # cleaning gender column
    clean_gender(df1)
    clean_gender(df2) 
    clean_gender(df3)
    
    #splitting Name column into 1stname and 2ndname
    split_name(df2)
    split_name(df3)

    # removing unnecessary characters and capitalizing first letter of firstname and lastname columns
    clean_name(df1)
    clean_name(df2)
    clean_name(df3)

    #email column lowercase
    clean_email(df1)
    clean_email(df2)
    clean_email(df3)

    # filling null columns
    fill_nulls(df1, 'FirstName')
    fill_nulls(df1, 'UserName')
    fill_nulls(df3, 'Email')
    fill_username(df3)
    
    # city column
    clean_city(df1)
    clean_city(df2)
    clean_city(df3)

    # country column
    clean_country(df1)
    clean_country(df2)
    clean_country(df3)
 
    # concatinating all 3 dataframes
    df = pd.concat([df1, df2, df3], ignore_index=True)

    # write dataframe to a database(db) file
    connection = connect_to_database(db_file)
    df_to_db(df, connection)

    # converting db file to sql file
    db_to_sql(db_file,sql_file)


def clean_gender(df):
    gender = {'0':'Male', '1':'Female','M': 'Male','F': 'Female'}
    df['Gender'] = df['Gender'].replace(gender)

def split_name(df):
    df[['FirstName','LastName']] = df.Name.str.split(expand=True)
    df.drop('Name', inplace=True, axis=1)

def clean_name(df):

    df['FirstName'] = df['FirstName'].str.replace('\\','')
    df['LastName'] = df['LastName'].str.replace('\\','')
    df['FirstName'] = df['FirstName'].str.replace('"','')
    df['LastName'] = df['LastName'].str.replace('"','')
    df['FirstName'] = df['FirstName'].str.capitalize() 
    df['LastName'] = df['LastName'].str.capitalize() 

def clean_city(df):
    df['City'] = df['City'].str.replace('_',' ')
    df['City'] = df['City'].str.replace('-',' ')
    df['City'] = df['City'].str.title() 

def clean_email(df):
        df["Email"] = df["Email"].str.lower()

def clean_prefixes(df):
    df = df.replace(to_replace=r'string_|integer_|boolean_|character_', value='', regex=True)
    df['Age'] = df['Age'].replace(to_replace=r'[a-zA-Z]', value='', regex=True)
    return df

def clean_country(df):
    df['Country'] = 'USA'

def fill_nulls(df, col):
    df[col] = df[col].fillna("Nan", inplace = True)

def fill_username(df):
    df['UserName'] = None

def connect_to_database(sql_file):
    try:
        connection = sqlite3.connect(sql_file)
        print("Opened database successfully")
    except Exception as e:
        print("Error during connection:", str(e))
    return connection

def df_to_db(df, connection):

        #create SQL table
        cur = connection.cursor()
        cur.execute('DROP TABLE IF EXISTS customers')
        cur.execute('''
            CREATE TABLE customers(
                "gender" TEXT,
                "firstname" TEXT,
                "lastname" TEXT,
                "email" TEXT,
                "age" TEXT,
                "city" TEXT,
                "country" TEXT,
                "created_at" TEXT,
                "referral" TEXT)
            ''')

        # Insert DataFrame to Table
        for index, row in df.iterrows():
            gender = row['Gender']
            firstname = row['FirstName']
            lastname = row['LastName']
            email = row['Email']
            age = row['Age']
            city = row['City']
            country = row['Country']

            cur.execute('''INSERT INTO customers(gender, firstname, lastname,
                        email, age, city, country) VALUES (?, ?, ?, ?, ?, ?, ?)''', 
                        (gender, firstname, lastname, email, age, city, country))
        connection.commit()
        connection.close()
        
def db_to_sql(db_file,sql_file):
    con = sqlite3.connect(db_file)
    with open(sql_file, 'w') as f:
        for line in con.iterdump():
            f.write('%s\n' % line)
    con.close()

def print_db(conn, sql_file):
    conn = sqlite3.connect(sql_file)
    cur = conn.cursor()
    for row in cur.execute("SELECT * FROM customers"):
        print(row)
    connection.close()

my_m_and_a('only_wood_customer_us_1.csv', 'only_wood_customer_us_2.csv', 'only_wood_customer_us_3.csv', 'plastic_free_boutique.sql')


