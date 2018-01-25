dwh_id = 'pngi'  # Must be lower case

TMP_DATA_PATH = 'tmp_data/'
LOG_PATH = 'logs/'

CONFIG = {

    # Configuration
    'DWH_ID': dwh_id,

    'TMP_DATA_PATH': TMP_DATA_PATH,
    'LOG_PATH': LOG_PATH,

    # Connection details
    'CTL_DB_CONN': {'HOST':     'localhost',
                    'DBNAME':   dwh_id + '_ctl',
                    'USER':     'b_spurling',
                    'PASSWORD': ''},

    'ETL_DB_CONN': {'HOST':     'localhost',
                    'DBNAME':   dwh_id + '_etl',
                    'USER':     'b_spurling',
                    'PASSWORD': ''},

    'TRG_DB_CONN': {'HOST':     'localhost',
                    'DBNAME':   dwh_id + '_trg',
                    'USER':     'b_spurling',
                    'PASSWORD': ''},

    'SOURCE_SYSTEM_CONNS': {
        'IPA': {'TYPE':     'POSTGRES',
                'HOST':     'localhost',
                'DBNAME':   'IPA_scraper_output',
                'USERNAME': 'b_spurling',
                'PASSWORD': ''},
        'WP':  {'TYPE': 	'FILESYSTEM',
                'FILES':  [{'filename': 'documents',
                            'delimiter': ',',
                            'quotechar': '"'}]},
        'MSD': {'TYPE':     'SPREADSHEET'}
    },

    'GOOGLE_SHEETS_API_URL': 'https://spreadsheets.google.com/feeds',
    'GOOGLE_SHEETS_API_KEY_FILE': 'bSETL-83cba3f29177.json',

    # Schema definition files (one per database)
    'ETL_DB_SCHEMA_FILE_NAME': 'PNGi - ETL DB Schema',
    'TRG_DB_SCHEMA_FILE_NAME': 'PNGi - TRG DB Schema',

    # Manual source data spreadsheet
    'MSD_FILE_NAME': 'PNGi - Manual Source Data'
}
